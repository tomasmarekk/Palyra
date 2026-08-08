//! Signed dynamic-tool artifacts with bounded implementation and eval evidence.
//!
//! Proposals remain inert until the host verifies trust, capability review,
//! conformance evidence, approval generation, and a fresh catalog epoch.

use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use palyra_plugins_runtime::{CapabilityGrantSet, WasmRuntime};
use palyra_plugins_sdk::{DEFAULT_RUNTIME_ENTRYPOINT, HOST_CAPABILITIES_IMPORT_MODULE};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use thiserror::Error;
use wasmtime::{Engine, Module};

const DYNAMIC_TOOL_SCHEMA_VERSION: u32 = 1;
const DYNAMIC_TOOL_SIGNATURE_ALGORITHM: &str = "ed25519";
const DYNAMIC_TOOL_SIGNATURE_DOMAIN: &[u8] = b"palyra.dynamic-tool.payload.v1\0";
const DYNAMIC_TOOL_ARTIFACT_DOMAIN: &[u8] = b"palyra.dynamic-tool.artifact.v1\0";
const MAX_TOOL_NAME_BYTES: usize = 128;
const MAX_DESCRIPTION_BYTES: usize = 2_048;
const MAX_CAPABILITIES: usize = 64;
const MAX_IMPLEMENTATION_BYTES: usize = 4 * 1024 * 1024;
const MAX_DECLARATIVE_STEPS: usize = 64;
const MAX_SCHEMA_NODES: usize = 512;
const MAX_SCHEMA_DEPTH: usize = 12;
const MAX_EXECUTION_MS: u64 = 60_000;

/// Restricted implementation families accepted by the dynamic-tool builder.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DynamicToolImplementationType {
    DeclarativeComposition,
    WasmComponent,
}

/// Replay and approval semantics carried into the standard tool catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DynamicToolSemanticsV1 {
    pub mutating: bool,
    pub idempotent: bool,
    pub requires_approval: bool,
    pub max_execution_ms: u64,
}

/// Model-authored proposal. It contains no activation authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DynamicToolProposalV1 {
    pub v: u32,
    pub tool_name: String,
    pub description: String,
    pub input_schema: Value,
    pub output_schema: Value,
    pub capability_needs: Vec<String>,
    pub deterministic_constraints: Vec<String>,
    pub implementation_type: DynamicToolImplementationType,
    pub semantics: DynamicToolSemanticsV1,
    pub previous_artifact_sha256: Option<String>,
}

/// One child-tool step in a declarative composition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeclarativeToolStepV1 {
    pub step_id: String,
    pub tool_name: String,
    pub input_template: Value,
    pub timeout_ms: u64,
}

/// Restricted declarative implementation encoded as canonical JSON bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeclarativeToolPlanV1 {
    pub v: u32,
    pub steps: Vec<DeclarativeToolStepV1>,
}

/// Mandatory conformance scenarios for every generated artifact version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DynamicToolEvalKind {
    HappyPath,
    MalformedInput,
    Timeout,
    SecretHandling,
    Rollback,
    Authority,
}

impl DynamicToolEvalKind {
    const REQUIRED: [Self; 6] = [
        Self::HappyPath,
        Self::MalformedInput,
        Self::Timeout,
        Self::SecretHandling,
        Self::Rollback,
        Self::Authority,
    ];
}

/// Hash-only result for one mandatory evaluation scenario.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DynamicToolEvalCaseV1 {
    pub kind: DynamicToolEvalKind,
    pub passed: bool,
    pub evidence_sha256: String,
    pub duration_ms: u64,
    pub reason_code: String,
}

/// Complete, content-addressed conformance evidence for an artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DynamicToolEvalPackV1 {
    pub v: u32,
    pub cases: Vec<DynamicToolEvalCaseV1>,
    pub pack_sha256: String,
}

impl DynamicToolEvalPackV1 {
    /// Builds the exact six-case pack and binds its digest to canonical evidence.
    ///
    /// # Errors
    /// Rejects missing, duplicate, malformed, failed, or over-budget cases.
    pub fn passed(mut cases: Vec<DynamicToolEvalCaseV1>) -> Result<Self, DynamicToolError> {
        validate_eval_cases(cases.as_slice())?;
        cases.sort_by_key(|case| case.kind);
        let pack_sha256 = sha256_domain_json(b"palyra.dynamic-tool.eval-pack.v1\0", &cases)?;
        Ok(Self { v: DYNAMIC_TOOL_SCHEMA_VERSION, cases, pack_sha256 })
    }
}

/// Build provenance that stays attached to the immutable artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DynamicToolProvenanceV1 {
    pub builder_id: String,
    pub proposal_sha256: String,
    pub implementation_sha256: String,
    pub eval_pack_sha256: String,
    pub built_at_unix_ms: i64,
}

/// Detached signature over the domain-separated artifact payload digest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DynamicToolSignatureV1 {
    pub algorithm: String,
    pub publisher: String,
    pub key_id: String,
    pub public_key_base64: String,
    pub payload_sha256: String,
    pub signature_base64: String,
    pub signed_at_unix_ms: i64,
}

/// Immutable implementation, provenance, eval evidence, and signature.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignedToolArtifact {
    pub v: u32,
    pub proposal: DynamicToolProposalV1,
    pub implementation_bytes: Vec<u8>,
    pub implementation_sha256: String,
    pub provenance: DynamicToolProvenanceV1,
    pub eval_pack: DynamicToolEvalPackV1,
    pub payload_sha256: String,
    pub signature: DynamicToolSignatureV1,
    pub artifact_sha256: String,
}

/// Private-key-bearing build input; intentionally not serializable or debuggable.
#[derive(Clone)]
pub struct DynamicToolBuildRequest {
    pub proposal: DynamicToolProposalV1,
    pub implementation_bytes: Vec<u8>,
    pub eval_pack: DynamicToolEvalPackV1,
    pub allowed_capabilities: Vec<String>,
    pub builder_id: String,
    pub publisher: String,
    pub signing_key: [u8; 32],
    pub built_at_unix_ms: i64,
}

/// Host-owned activation inputs. The artifact itself cannot set these fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DynamicToolHostGate {
    pub host_validated: bool,
    pub policy_approved: bool,
    pub capability_review_approved: bool,
    pub eval_approved: bool,
    pub expected_catalog_epoch: u64,
    pub current_catalog_epoch: u64,
    pub approval_generation: u64,
    pub trusted_publisher: String,
    pub trusted_public_key_base64: String,
    pub previous_active_artifact_sha256: Option<String>,
}

/// Catalog metadata projected only after every activation check passes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DynamicToolCatalogDescriptorV1 {
    pub v: u32,
    pub tool_name: String,
    pub description: String,
    pub input_schema: Value,
    pub output_schema: Value,
    pub capabilities: Vec<String>,
    pub semantics: DynamicToolSemanticsV1,
    pub artifact_sha256: String,
    pub implementation_type: DynamicToolImplementationType,
}

/// Durable activation or rollback decision with stable redacted diagnostics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolActivationDecision {
    pub v: u32,
    pub activated: bool,
    pub tool_name: String,
    pub artifact_sha256: String,
    pub catalog_epoch: u64,
    pub approval_generation: u64,
    pub rollback_artifact_sha256: Option<String>,
    pub descriptor: Option<DynamicToolCatalogDescriptorV1>,
    pub reason_code: String,
}

/// Build, scan, verification, and activation failures.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum DynamicToolError {
    #[error("dynamic tool proposal is invalid: {0}")]
    ProposalInvalid(&'static str),
    #[error("dynamic tool capability request exceeds host policy")]
    CapabilityEscalation,
    #[error("dynamic tool implementation is invalid: {0}")]
    ImplementationInvalid(&'static str),
    #[error("dynamic tool evaluation pack is invalid: {0}")]
    EvalPackInvalid(&'static str),
    #[error("dynamic tool artifact signature verification failed")]
    SignatureInvalid,
    #[error("dynamic tool artifact digest verification failed")]
    DigestInvalid,
    #[error("dynamic tool serialization failed")]
    Serialization,
}

#[derive(Serialize)]
struct UnsignedToolPayload<'a> {
    v: u32,
    proposal: &'a DynamicToolProposalV1,
    implementation_bytes: &'a [u8],
    implementation_sha256: &'a str,
    provenance: &'a DynamicToolProvenanceV1,
    eval_pack: &'a DynamicToolEvalPackV1,
}

/// Builds, statically scans, content-addresses, and signs one immutable version.
///
/// # Errors
/// Rejects malformed schemas, authority expansion, unsupported implementation
/// imports, invalid eval evidence, serialization, or artifact budget failures.
pub fn build_signed_dynamic_tool_artifact(
    mut request: DynamicToolBuildRequest,
) -> Result<SignedToolArtifact, DynamicToolError> {
    canonicalize_proposal(&mut request.proposal);
    validate_proposal(&request.proposal)?;
    if request.builder_id.trim().is_empty()
        || request.builder_id.len() > 128
        || request.publisher.trim().is_empty()
        || request.publisher.len() > 128
        || request.built_at_unix_ms < 0
    {
        return Err(DynamicToolError::ProposalInvalid("builder or publisher identity"));
    }
    let allowed = canonical_strings(request.allowed_capabilities);
    if request.proposal.capability_needs.iter().any(|capability| !allowed.contains(capability)) {
        return Err(DynamicToolError::CapabilityEscalation);
    }
    static_scan_implementation(&request.proposal, request.implementation_bytes.as_slice())?;
    validate_eval_pack(&request.eval_pack)?;

    let proposal_sha256 =
        sha256_domain_json(b"palyra.dynamic-tool.proposal.v1\0", &request.proposal)?;
    let implementation_sha256 = sha256_hex(request.implementation_bytes.as_slice());
    let provenance = DynamicToolProvenanceV1 {
        builder_id: request.builder_id,
        proposal_sha256,
        implementation_sha256: implementation_sha256.clone(),
        eval_pack_sha256: request.eval_pack.pack_sha256.clone(),
        built_at_unix_ms: request.built_at_unix_ms,
    };
    let payload = UnsignedToolPayload {
        v: DYNAMIC_TOOL_SCHEMA_VERSION,
        proposal: &request.proposal,
        implementation_bytes: request.implementation_bytes.as_slice(),
        implementation_sha256: implementation_sha256.as_str(),
        provenance: &provenance,
        eval_pack: &request.eval_pack,
    };
    let payload_sha256 = sha256_domain_json(DYNAMIC_TOOL_SIGNATURE_DOMAIN, &payload)?;
    let signing_key = SigningKey::from_bytes(&request.signing_key);
    let verifying_key = signing_key.verifying_key();
    let signature_bytes = signing_key.sign(payload_sha256.as_bytes());
    let signature = DynamicToolSignatureV1 {
        algorithm: DYNAMIC_TOOL_SIGNATURE_ALGORITHM.to_owned(),
        publisher: request.publisher,
        key_id: key_id_for(&verifying_key),
        public_key_base64: BASE64_STANDARD.encode(verifying_key.as_bytes()),
        payload_sha256: payload_sha256.clone(),
        signature_base64: BASE64_STANDARD.encode(signature_bytes.to_bytes()),
        signed_at_unix_ms: request.built_at_unix_ms,
    };
    let artifact_sha256 = artifact_digest(payload_sha256.as_str(), &signature)?;
    Ok(SignedToolArtifact {
        v: DYNAMIC_TOOL_SCHEMA_VERSION,
        proposal: request.proposal,
        implementation_bytes: request.implementation_bytes,
        implementation_sha256,
        provenance,
        eval_pack: request.eval_pack,
        payload_sha256,
        signature,
        artifact_sha256,
    })
}

/// Recomputes every digest, signature, schema, scan, and eval invariant.
///
/// # Errors
/// Rejects any mutation, unsigned artifact, invalid implementation, or failed eval.
pub fn verify_signed_dynamic_tool_artifact(
    artifact: &SignedToolArtifact,
) -> Result<(), DynamicToolError> {
    if artifact.v != DYNAMIC_TOOL_SCHEMA_VERSION {
        return Err(DynamicToolError::DigestInvalid);
    }
    validate_proposal(&artifact.proposal)?;
    static_scan_implementation(&artifact.proposal, artifact.implementation_bytes.as_slice())?;
    validate_eval_pack(&artifact.eval_pack)?;
    if sha256_hex(artifact.implementation_bytes.as_slice()) != artifact.implementation_sha256
        || artifact.provenance.implementation_sha256 != artifact.implementation_sha256
        || artifact.provenance.eval_pack_sha256 != artifact.eval_pack.pack_sha256
        || artifact.provenance.proposal_sha256
            != sha256_domain_json(b"palyra.dynamic-tool.proposal.v1\0", &artifact.proposal)?
    {
        return Err(DynamicToolError::DigestInvalid);
    }
    let payload = UnsignedToolPayload {
        v: DYNAMIC_TOOL_SCHEMA_VERSION,
        proposal: &artifact.proposal,
        implementation_bytes: artifact.implementation_bytes.as_slice(),
        implementation_sha256: artifact.implementation_sha256.as_str(),
        provenance: &artifact.provenance,
        eval_pack: &artifact.eval_pack,
    };
    let payload_sha256 = sha256_domain_json(DYNAMIC_TOOL_SIGNATURE_DOMAIN, &payload)?;
    if payload_sha256 != artifact.payload_sha256
        || artifact.signature.payload_sha256 != artifact.payload_sha256
        || artifact_digest(artifact.payload_sha256.as_str(), &artifact.signature)?
            != artifact.artifact_sha256
    {
        return Err(DynamicToolError::DigestInvalid);
    }
    verify_signature(&artifact.signature)
}

/// Applies the host validation gate and advances the catalog by exactly one epoch.
#[must_use]
pub fn decide_dynamic_tool_activation(
    artifact: &SignedToolArtifact,
    gate: &DynamicToolHostGate,
) -> ToolActivationDecision {
    let denial = activation_denial(artifact, gate);
    if let Some(reason_code) = denial {
        return decision(
            artifact,
            gate.current_catalog_epoch,
            gate.approval_generation,
            false,
            reason_code,
        );
    }
    let Some(next_epoch) = gate.current_catalog_epoch.checked_add(1) else {
        return decision(
            artifact,
            gate.current_catalog_epoch,
            gate.approval_generation,
            false,
            "dynamic_tool.catalog_epoch_exhausted",
        );
    };
    let descriptor = DynamicToolCatalogDescriptorV1 {
        v: DYNAMIC_TOOL_SCHEMA_VERSION,
        tool_name: artifact.proposal.tool_name.clone(),
        description: artifact.proposal.description.clone(),
        input_schema: artifact.proposal.input_schema.clone(),
        output_schema: artifact.proposal.output_schema.clone(),
        capabilities: artifact.proposal.capability_needs.clone(),
        semantics: artifact.proposal.semantics.clone(),
        artifact_sha256: artifact.artifact_sha256.clone(),
        implementation_type: artifact.proposal.implementation_type,
    };
    ToolActivationDecision {
        v: DYNAMIC_TOOL_SCHEMA_VERSION,
        activated: true,
        tool_name: artifact.proposal.tool_name.clone(),
        artifact_sha256: artifact.artifact_sha256.clone(),
        catalog_epoch: next_epoch,
        approval_generation: gate.approval_generation,
        rollback_artifact_sha256: artifact.proposal.previous_artifact_sha256.clone(),
        descriptor: Some(descriptor),
        reason_code: "dynamic_tool.activated".to_owned(),
    }
}

/// Validates and reactivates the signed rollback target at a fresh catalog epoch.
#[must_use]
pub fn decide_dynamic_tool_rollback(
    current: &ToolActivationDecision,
    rollback_target: &SignedToolArtifact,
    gate: &DynamicToolHostGate,
) -> ToolActivationDecision {
    if !current.activated
        || current.rollback_artifact_sha256.as_deref()
            != Some(rollback_target.artifact_sha256.as_str())
        || gate.current_catalog_epoch != current.catalog_epoch
        || gate.approval_generation <= current.approval_generation
    {
        return decision(
            rollback_target,
            gate.current_catalog_epoch,
            gate.approval_generation,
            false,
            "dynamic_tool.rollback_denied",
        );
    }
    let mut rollback_gate = gate.clone();
    rollback_gate.previous_active_artifact_sha256 =
        rollback_target.proposal.previous_artifact_sha256.clone();
    let mut outcome = decide_dynamic_tool_activation(rollback_target, &rollback_gate);
    if outcome.activated {
        outcome.rollback_artifact_sha256 = Some(current.artifact_sha256.clone());
        outcome.reason_code = "dynamic_tool.rollback_activated".to_owned();
    }
    outcome
}

fn activation_denial(
    artifact: &SignedToolArtifact,
    gate: &DynamicToolHostGate,
) -> Option<&'static str> {
    if verify_signed_dynamic_tool_artifact(artifact).is_err() {
        return Some("dynamic_tool.artifact_verification_failed");
    }
    if artifact.signature.publisher != gate.trusted_publisher
        || artifact.signature.public_key_base64 != gate.trusted_public_key_base64
    {
        return Some("dynamic_tool.publisher_trust_denied");
    }
    if !gate.host_validated {
        return Some("dynamic_tool.host_validation_required");
    }
    if !gate.policy_approved {
        return Some("dynamic_tool.policy_approval_required");
    }
    if !gate.capability_review_approved {
        return Some("dynamic_tool.capability_review_required");
    }
    if !gate.eval_approved {
        return Some("dynamic_tool.eval_approval_required");
    }
    if gate.approval_generation == 0 {
        return Some("dynamic_tool.approval_generation_invalid");
    }
    if gate.current_catalog_epoch == 0 || gate.expected_catalog_epoch != gate.current_catalog_epoch
    {
        return Some("dynamic_tool.catalog_epoch_stale");
    }
    if artifact.proposal.previous_artifact_sha256 != gate.previous_active_artifact_sha256 {
        return Some("dynamic_tool.rollback_pointer_mismatch");
    }
    None
}

fn decision(
    artifact: &SignedToolArtifact,
    catalog_epoch: u64,
    approval_generation: u64,
    activated: bool,
    reason_code: &str,
) -> ToolActivationDecision {
    ToolActivationDecision {
        v: DYNAMIC_TOOL_SCHEMA_VERSION,
        activated,
        tool_name: artifact.proposal.tool_name.clone(),
        artifact_sha256: artifact.artifact_sha256.clone(),
        catalog_epoch,
        approval_generation,
        rollback_artifact_sha256: artifact.proposal.previous_artifact_sha256.clone(),
        descriptor: None,
        reason_code: reason_code.to_owned(),
    }
}

fn validate_proposal(proposal: &DynamicToolProposalV1) -> Result<(), DynamicToolError> {
    if proposal.v != DYNAMIC_TOOL_SCHEMA_VERSION
        || !valid_tool_name(proposal.tool_name.as_str())
        || proposal.description.trim().is_empty()
        || proposal.description.len() > MAX_DESCRIPTION_BYTES
        || proposal.capability_needs.len() > MAX_CAPABILITIES
        || proposal.deterministic_constraints.is_empty()
        || proposal.deterministic_constraints.len() > 64
        || proposal.semantics.max_execution_ms == 0
        || proposal.semantics.max_execution_ms > MAX_EXECUTION_MS
        || (proposal.semantics.idempotent && !proposal.semantics.mutating)
    {
        return Err(DynamicToolError::ProposalInvalid("shape or execution semantics"));
    }
    validate_schema(&proposal.input_schema)?;
    validate_schema(&proposal.output_schema)?;
    let canonical_capabilities = canonical_strings(proposal.capability_needs.clone());
    if canonical_capabilities != proposal.capability_needs
        || canonical_capabilities.iter().any(|capability| !valid_capability(capability))
    {
        return Err(DynamicToolError::ProposalInvalid("capability declaration"));
    }
    let constraints = canonical_strings(proposal.deterministic_constraints.clone());
    if constraints != proposal.deterministic_constraints
        || constraints.iter().any(|value| value.len() > 256)
    {
        return Err(DynamicToolError::ProposalInvalid("deterministic constraints"));
    }
    if proposal.previous_artifact_sha256.as_deref().is_some_and(|digest| !valid_sha256(digest)) {
        return Err(DynamicToolError::ProposalInvalid("rollback pointer"));
    }
    Ok(())
}

fn canonicalize_proposal(proposal: &mut DynamicToolProposalV1) {
    proposal.capability_needs = canonical_strings(std::mem::take(&mut proposal.capability_needs));
    proposal.deterministic_constraints =
        canonical_strings(std::mem::take(&mut proposal.deterministic_constraints));
    proposal.input_schema = canonical_value(std::mem::take(&mut proposal.input_schema));
    proposal.output_schema = canonical_value(std::mem::take(&mut proposal.output_schema));
}

fn static_scan_implementation(
    proposal: &DynamicToolProposalV1,
    bytes: &[u8],
) -> Result<(), DynamicToolError> {
    if bytes.is_empty() || bytes.len() > MAX_IMPLEMENTATION_BYTES {
        return Err(DynamicToolError::ImplementationInvalid("implementation size"));
    }
    match proposal.implementation_type {
        DynamicToolImplementationType::DeclarativeComposition => {
            let mut plan: DeclarativeToolPlanV1 = serde_json::from_slice(bytes)
                .map_err(|_| DynamicToolError::ImplementationInvalid("declarative JSON"))?;
            if plan.v != DYNAMIC_TOOL_SCHEMA_VERSION
                || plan.steps.is_empty()
                || plan.steps.len() > MAX_DECLARATIVE_STEPS
            {
                return Err(DynamicToolError::ImplementationInvalid("declarative plan shape"));
            }
            let mut step_ids = BTreeSet::new();
            for step in &mut plan.steps {
                step.input_template = canonical_value(std::mem::take(&mut step.input_template));
                if !valid_identifier(step.step_id.as_str())
                    || !step_ids.insert(step.step_id.clone())
                    || !valid_tool_name(step.tool_name.as_str())
                    || step.tool_name == proposal.tool_name
                    || step.timeout_ms == 0
                    || step.timeout_ms > proposal.semantics.max_execution_ms
                    || !proposal.capability_needs.contains(&format!("tool:{}", step.tool_name))
                {
                    return Err(DynamicToolError::ImplementationInvalid(
                        "declarative child-tool authority",
                    ));
                }
            }
        }
        DynamicToolImplementationType::WasmComponent => {
            let engine = Engine::default();
            let module = Module::new(&engine, bytes)
                .map_err(|_| DynamicToolError::ImplementationInvalid("WASM compile"))?;
            if !module.exports().any(|export| export.name() == DEFAULT_RUNTIME_ENTRYPOINT) {
                return Err(DynamicToolError::ImplementationInvalid("WASM run export"));
            }
            if module.imports().any(|import| import.module() != HOST_CAPABILITIES_IMPORT_MODULE) {
                return Err(DynamicToolError::ImplementationInvalid("WASM ambient authority"));
            }
            let runtime = WasmRuntime::new()
                .map_err(|_| DynamicToolError::ImplementationInvalid("WASM runtime"))?;
            runtime
                .execute_i32_entrypoint_with_timeout(
                    bytes,
                    DEFAULT_RUNTIME_ENTRYPOINT,
                    &capability_grants(proposal.capability_needs.as_slice()),
                    Duration::from_millis(proposal.semantics.max_execution_ms.min(1_000)),
                )
                .map_err(|_| DynamicToolError::ImplementationInvalid("WASM conformance"))?;
        }
    }
    Ok(())
}

fn capability_grants(capabilities: &[String]) -> CapabilityGrantSet {
    let mut grants = CapabilityGrantSet::default();
    for capability in capabilities {
        if let Some(value) = capability.strip_prefix("http_host:") {
            grants.http_hosts.push(value.to_owned());
        } else if let Some(value) = capability.strip_prefix("secret_lease:") {
            grants.secret_keys.push(value.to_owned());
        } else if let Some(value) = capability.strip_prefix("storage_prefix:") {
            grants.storage_prefixes.push(value.to_owned());
        } else if let Some(value) = capability.strip_prefix("channel:") {
            grants.channels.push(value.to_owned());
        }
    }
    grants.canonicalized()
}

fn validate_eval_pack(pack: &DynamicToolEvalPackV1) -> Result<(), DynamicToolError> {
    if pack.v != DYNAMIC_TOOL_SCHEMA_VERSION {
        return Err(DynamicToolError::EvalPackInvalid("schema version"));
    }
    validate_eval_cases(pack.cases.as_slice())?;
    if !pack.cases.windows(2).all(|cases| cases[0].kind < cases[1].kind) {
        return Err(DynamicToolError::EvalPackInvalid("case ordering"));
    }
    let expected = sha256_domain_json(b"palyra.dynamic-tool.eval-pack.v1\0", &pack.cases)?;
    if expected != pack.pack_sha256 {
        return Err(DynamicToolError::EvalPackInvalid("digest mismatch"));
    }
    Ok(())
}

fn validate_eval_cases(cases: &[DynamicToolEvalCaseV1]) -> Result<(), DynamicToolError> {
    if cases.len() != DynamicToolEvalKind::REQUIRED.len() {
        return Err(DynamicToolError::EvalPackInvalid("required case count"));
    }
    let kinds = cases.iter().map(|case| case.kind).collect::<BTreeSet<_>>();
    if kinds != DynamicToolEvalKind::REQUIRED.into_iter().collect()
        || cases.iter().any(|case| {
            !case.passed
                || !valid_sha256(case.evidence_sha256.as_str())
                || case.duration_ms > MAX_EXECUTION_MS
                || !valid_reason_code(case.reason_code.as_str())
        })
    {
        return Err(DynamicToolError::EvalPackInvalid("case evidence"));
    }
    Ok(())
}

fn validate_schema(schema: &Value) -> Result<(), DynamicToolError> {
    let Some(root) = schema.as_object() else {
        return Err(DynamicToolError::ProposalInvalid("JSON schema root"));
    };
    if root.get("type").and_then(Value::as_str) != Some("object") {
        return Err(DynamicToolError::ProposalInvalid("JSON schema object root"));
    }
    let mut nodes = 0;
    validate_schema_node(schema, 0, &mut nodes)
}

fn validate_schema_node(
    value: &Value,
    depth: usize,
    nodes: &mut usize,
) -> Result<(), DynamicToolError> {
    *nodes = nodes.saturating_add(1);
    if depth > MAX_SCHEMA_DEPTH || *nodes > MAX_SCHEMA_NODES {
        return Err(DynamicToolError::ProposalInvalid("JSON schema budget"));
    }
    match value {
        Value::Object(map) => {
            if map.contains_key("$ref")
                || map.contains_key("$dynamicRef")
                || map.contains_key("unevaluatedProperties")
            {
                return Err(DynamicToolError::ProposalInvalid("JSON schema indirection"));
            }
            for nested in map.values() {
                validate_schema_node(nested, depth.saturating_add(1), nodes)?;
            }
        }
        Value::Array(values) => {
            for nested in values {
                validate_schema_node(nested, depth.saturating_add(1), nodes)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn verify_signature(signature: &DynamicToolSignatureV1) -> Result<(), DynamicToolError> {
    if signature.algorithm != DYNAMIC_TOOL_SIGNATURE_ALGORITHM
        || signature.publisher.trim().is_empty()
        || !valid_sha256(signature.payload_sha256.as_str())
    {
        return Err(DynamicToolError::SignatureInvalid);
    }
    let public_key = BASE64_STANDARD
        .decode(signature.public_key_base64.as_bytes())
        .map_err(|_| DynamicToolError::SignatureInvalid)?;
    let public_key: [u8; 32] =
        public_key.as_slice().try_into().map_err(|_| DynamicToolError::SignatureInvalid)?;
    let verifying_key =
        VerifyingKey::from_bytes(&public_key).map_err(|_| DynamicToolError::SignatureInvalid)?;
    if signature.key_id != key_id_for(&verifying_key) {
        return Err(DynamicToolError::SignatureInvalid);
    }
    let signature_bytes = BASE64_STANDARD
        .decode(signature.signature_base64.as_bytes())
        .map_err(|_| DynamicToolError::SignatureInvalid)?;
    let signature_bytes: [u8; 64] =
        signature_bytes.as_slice().try_into().map_err(|_| DynamicToolError::SignatureInvalid)?;
    verifying_key
        .verify(signature.payload_sha256.as_bytes(), &Signature::from_bytes(&signature_bytes))
        .map_err(|_| DynamicToolError::SignatureInvalid)
}

fn artifact_digest(
    payload_sha256: &str,
    signature: &DynamicToolSignatureV1,
) -> Result<String, DynamicToolError> {
    #[derive(Serialize)]
    struct ArtifactDigest<'a> {
        payload_sha256: &'a str,
        signature: &'a DynamicToolSignatureV1,
    }
    sha256_domain_json(DYNAMIC_TOOL_ARTIFACT_DOMAIN, &ArtifactDigest { payload_sha256, signature })
}

fn sha256_domain_json<T: Serialize>(domain: &[u8], value: &T) -> Result<String, DynamicToolError> {
    let bytes = serde_json::to_vec(value).map_err(|_| DynamicToolError::Serialization)?;
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update((bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
    Ok(hex::encode(hasher.finalize()))
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn key_id_for(key: &VerifyingKey) -> String {
    let digest = sha256_hex(key.as_bytes());
    format!("ed25519:{}", &digest[..16])
}

fn canonical_strings(values: Vec<String>) -> Vec<String> {
    values
        .into_iter()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn canonical_value(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let sorted = map
                .into_iter()
                .map(|(key, value)| (key, canonical_value(value)))
                .collect::<BTreeMap<_, _>>();
            Value::Object(sorted.into_iter().collect::<Map<_, _>>())
        }
        Value::Array(values) => Value::Array(values.into_iter().map(canonical_value).collect()),
        other => other,
    }
}

fn valid_tool_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_TOOL_NAME_BYTES
        && value.contains('.')
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

fn valid_capability(value: &str) -> bool {
    ["tool:", "http_host:", "secret_lease:", "storage_prefix:", "channel:"].iter().any(|prefix| {
        value.strip_prefix(prefix).is_some_and(|scope| !scope.is_empty() && scope.len() <= 256)
    })
}

fn valid_reason_code(value: &str) -> bool {
    valid_identifier(value) && value.contains('.')
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const NOW: i64 = 100_000;
    const KEY: [u8; 32] = [7; 32];

    fn schema() -> Value {
        json!({"type":"object","properties":{"value":{"type":"string"}},"additionalProperties":false})
    }

    fn proposal(previous_artifact_sha256: Option<String>) -> DynamicToolProposalV1 {
        DynamicToolProposalV1 {
            v: 1,
            tool_name: "dynamic.echo".to_owned(),
            description: "Echoes a bounded value through an approved child tool.".to_owned(),
            input_schema: schema(),
            output_schema: schema(),
            capability_needs: vec!["tool:palyra.echo".to_owned()],
            deterministic_constraints: vec!["bounded_output".to_owned()],
            implementation_type: DynamicToolImplementationType::DeclarativeComposition,
            semantics: DynamicToolSemanticsV1 {
                mutating: false,
                idempotent: false,
                requires_approval: false,
                max_execution_ms: 1_000,
            },
            previous_artifact_sha256,
        }
    }

    fn eval_pack() -> DynamicToolEvalPackV1 {
        DynamicToolEvalPackV1::passed(
            DynamicToolEvalKind::REQUIRED
                .into_iter()
                .map(|kind| DynamicToolEvalCaseV1 {
                    kind,
                    passed: true,
                    evidence_sha256: sha256_hex(format!("{kind:?}").as_bytes()),
                    duration_ms: 10,
                    reason_code: "dynamic_tool.eval_passed".to_owned(),
                })
                .collect(),
        )
        .expect("eval pack should pass")
    }

    fn implementation() -> Vec<u8> {
        serde_json::to_vec(&DeclarativeToolPlanV1 {
            v: 1,
            steps: vec![DeclarativeToolStepV1 {
                step_id: "echo".to_owned(),
                tool_name: "palyra.echo".to_owned(),
                input_template: json!({"value":"${input.value}"}),
                timeout_ms: 100,
            }],
        })
        .expect("plan should serialize")
    }

    fn build(previous: Option<String>) -> SignedToolArtifact {
        build_signed_dynamic_tool_artifact(DynamicToolBuildRequest {
            proposal: proposal(previous),
            implementation_bytes: implementation(),
            eval_pack: eval_pack(),
            allowed_capabilities: vec!["tool:palyra.echo".to_owned()],
            builder_id: "host-builder".to_owned(),
            publisher: "palyra.local".to_owned(),
            signing_key: KEY,
            built_at_unix_ms: NOW,
        })
        .expect("artifact should build")
    }

    fn gate(
        artifact: &SignedToolArtifact,
        previous: Option<String>,
        epoch: u64,
        generation: u64,
    ) -> DynamicToolHostGate {
        DynamicToolHostGate {
            host_validated: true,
            policy_approved: true,
            capability_review_approved: true,
            eval_approved: true,
            expected_catalog_epoch: epoch,
            current_catalog_epoch: epoch,
            approval_generation: generation,
            trusted_publisher: artifact.signature.publisher.clone(),
            trusted_public_key_base64: artifact.signature.public_key_base64.clone(),
            previous_active_artifact_sha256: previous,
        }
    }

    #[test]
    fn valid_declarative_artifact_requires_host_gate_and_advances_epoch() {
        let artifact = build(None);
        verify_signed_dynamic_tool_artifact(&artifact).expect("artifact should verify");
        let mut denied_gate = gate(&artifact, None, 4, 1);
        denied_gate.host_validated = false;
        let denied = decide_dynamic_tool_activation(&artifact, &denied_gate);
        assert!(!denied.activated);
        assert_eq!(denied.reason_code, "dynamic_tool.host_validation_required");

        let activated = decide_dynamic_tool_activation(&artifact, &gate(&artifact, None, 4, 1));
        assert!(activated.activated);
        assert_eq!(activated.catalog_epoch, 5);
        assert!(activated.descriptor.is_some());
    }

    #[test]
    fn malicious_capability_and_failed_eval_are_rejected() {
        let mut malicious = proposal(None);
        malicious.capability_needs.push("http_host:metadata.internal".to_owned());
        let error = build_signed_dynamic_tool_artifact(DynamicToolBuildRequest {
            proposal: malicious,
            implementation_bytes: implementation(),
            eval_pack: eval_pack(),
            allowed_capabilities: vec!["tool:palyra.echo".to_owned()],
            builder_id: "host-builder".to_owned(),
            publisher: "palyra.local".to_owned(),
            signing_key: KEY,
            built_at_unix_ms: NOW,
        })
        .expect_err("authority expansion must fail");
        assert_eq!(error, DynamicToolError::CapabilityEscalation);

        let mut cases = eval_pack().cases;
        cases[0].passed = false;
        let error = DynamicToolEvalPackV1::passed(cases).expect_err("failed eval must not package");
        assert_eq!(error, DynamicToolError::EvalPackInvalid("case evidence"));
    }

    #[test]
    fn signature_mismatch_and_schema_epoch_staleness_fail_closed() {
        let mut artifact = build(None);
        artifact.signature.signature_base64 = BASE64_STANDARD.encode([0_u8; 64]);
        assert_eq!(
            verify_signed_dynamic_tool_artifact(&artifact),
            Err(DynamicToolError::DigestInvalid)
        );

        let artifact = build(None);
        let mut stale = gate(&artifact, None, 7, 1);
        stale.expected_catalog_epoch = 6;
        let decision = decide_dynamic_tool_activation(&artifact, &stale);
        assert!(!decision.activated);
        assert_eq!(decision.reason_code, "dynamic_tool.catalog_epoch_stale");
    }

    #[test]
    fn rollback_requires_signed_target_and_new_approval_generation() {
        let original = build(None);
        assert!(decide_dynamic_tool_activation(&original, &gate(&original, None, 1, 1)).activated);
        let replacement = build(Some(original.artifact_sha256.clone()));
        let second = decide_dynamic_tool_activation(
            &replacement,
            &gate(&replacement, Some(original.artifact_sha256.clone()), 2, 2),
        );
        assert!(second.activated);
        let rollback = decide_dynamic_tool_rollback(
            &second,
            &original,
            &gate(&original, None, second.catalog_epoch, 3),
        );
        assert!(rollback.activated);
        assert_eq!(rollback.catalog_epoch, 4);
        assert_eq!(rollback.reason_code, "dynamic_tool.rollback_activated");

        let denied = decide_dynamic_tool_rollback(
            &second,
            &original,
            &gate(&original, None, second.catalog_epoch, 2),
        );
        assert!(!denied.activated);
        assert_eq!(denied.reason_code, "dynamic_tool.rollback_denied");
    }

    #[test]
    fn wasm_sandbox_escape_import_is_rejected() {
        let mut proposal = proposal(None);
        proposal.implementation_type = DynamicToolImplementationType::WasmComponent;
        proposal.capability_needs.clear();
        let escape = br#"
            (module
              (import "wasi_snapshot_preview1" "proc_exit" (func $exit (param i32)))
              (func (export "run") (result i32)
                i32.const 0))
        "#;
        let error = build_signed_dynamic_tool_artifact(DynamicToolBuildRequest {
            proposal,
            implementation_bytes: escape.to_vec(),
            eval_pack: eval_pack(),
            allowed_capabilities: Vec::new(),
            builder_id: "host-builder".to_owned(),
            publisher: "palyra.local".to_owned(),
            signing_key: KEY,
            built_at_unix_ms: NOW,
        })
        .expect_err("ambient WASI authority must fail");
        assert_eq!(error, DynamicToolError::ImplementationInvalid("WASM ambient authority"));
    }
}
