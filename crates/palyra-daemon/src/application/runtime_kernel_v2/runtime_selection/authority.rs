//! Consumable host authority and lossless fallback failure contracts.

use std::{fmt, num::NonZeroU64};

use palyra_common::runtime_contracts::{
    GenerationLeaseV1, RuntimeGenerationLane, RuntimeIdentitySetV1, SideEffectFenceState,
};
use serde::{Deserialize, Serialize};

use super::{
    bounded::SafeLabel, digest::SelectionDigest, policies::SelectionEpochsV1,
    projection::RuntimeSelectionV1, service::RuntimeSelectionError,
};
use crate::{
    application::{
        run_admission::{
            AdmissionCaller, AdmissionEnvironmentSnapshot, AdmissionOrigin, AdmissionQueueIntent,
            PersistedV2AdmissionToken,
        },
        runtime_kernel_v2::{
            selection::{
                ResolvedRuntimeAuthorityIntent, RuntimeAuthority, RuntimeAuthorityDecisionV1,
            },
            RuntimeKernelVersion,
        },
        tool_registry::canonical_json_bytes,
    },
    journal::{run_admission::JournalSessionAuthorityIntent, JournalStore},
};

const PERSISTED_ADMISSION_TOKEN_DOMAIN: &[u8] =
    b"palyra.runtime_selection.persisted_admission_token.v1\0";

/// Host-sealed request to change one durable session authority at a safe boundary.
pub(crate) struct HostVerifiedSessionAuthorityMigration {
    session_id: String,
    expected_revision: NonZeroU64,
    target: JournalSessionAuthorityIntent,
    _private: (),
}

impl HostVerifiedSessionAuthorityMigration {
    pub(in crate::application::runtime_kernel_v2) fn configured_profile_change(
        session_id: String,
        expected_revision: NonZeroU64,
        target: JournalSessionAuthorityIntent,
    ) -> Self {
        Self { session_id, expected_revision, target, _private: () }
    }

    #[cfg(test)]
    pub(crate) fn test_only(
        session_id: String,
        expected_revision: NonZeroU64,
        target: JournalSessionAuthorityIntent,
    ) -> Self {
        Self::configured_profile_change(session_id, expected_revision, target)
    }

    pub(crate) fn session_id(&self) -> &str {
        self.session_id.as_str()
    }

    pub(crate) const fn expected_revision(&self) -> u64 {
        self.expected_revision.get()
    }

    pub(crate) const fn target(&self) -> &JournalSessionAuthorityIntent {
        &self.target
    }

    pub(crate) const fn reason_code(&self) -> &'static str {
        "runtime.session_authority.configured_profile_change"
    }
}

/// Sealed host-issued admission input accepted by M018.
///
/// Only the runtime-kernel dispatcher can issue this proof in production.
/// Transport payloads cannot directly select an origin, assert access, replace
/// policy evidence, or provide a runtime decision to the admission controller.
pub(crate) struct HostVerifiedRunAdmission {
    origin: AdmissionOrigin,
    caller: AdmissionCaller,
    environment: AdmissionEnvironmentSnapshot,
    authority_intent: ResolvedRuntimeAuthorityIntent,
    origin_run_id: Option<String>,
    delegated_admission_json: Option<String>,
    queue_intent: Option<AdmissionQueueIntent>,
    _private: (),
}

impl fmt::Debug for HostVerifiedRunAdmission {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HostVerifiedRunAdmission")
            .field("origin", &self.origin)
            .field("authority_profile", &self.authority_intent.profile())
            .field("selected_runtime", &self.authority_intent.selected_runtime())
            .field("has_delegation", &self.delegated_admission_json.is_some())
            .field("has_queue_intent", &self.queue_intent.is_some())
            .finish_non_exhaustive()
    }
}

impl HostVerifiedRunAdmission {
    fn issue(
        origin: AdmissionOrigin,
        caller: AdmissionCaller,
        environment: AdmissionEnvironmentSnapshot,
        authority_intent: ResolvedRuntimeAuthorityIntent,
        origin_run_id: Option<String>,
        delegated_admission_json: Option<String>,
        queue_intent: Option<AdmissionQueueIntent>,
    ) -> Self {
        Self {
            origin,
            caller,
            environment,
            authority_intent,
            origin_run_id,
            delegated_admission_json,
            queue_intent,
            _private: (),
        }
    }

    /// Issues trusted console provenance after transport/session authorization.
    pub(in crate::application::runtime_kernel_v2) fn console(
        caller: AdmissionCaller,
        environment: AdmissionEnvironmentSnapshot,
        authority_intent: ResolvedRuntimeAuthorityIntent,
        queue_intent: Option<AdmissionQueueIntent>,
    ) -> Self {
        Self::issue(
            AdmissionOrigin::Console,
            caller,
            environment,
            authority_intent,
            None,
            None,
            queue_intent,
        )
    }

    /// Issues trusted channel provenance after channel ownership checks.
    pub(in crate::application::runtime_kernel_v2) fn channel(
        caller: AdmissionCaller,
        environment: AdmissionEnvironmentSnapshot,
        authority_intent: ResolvedRuntimeAuthorityIntent,
        queue_intent: Option<AdmissionQueueIntent>,
    ) -> Self {
        Self::issue(
            AdmissionOrigin::Channel,
            caller,
            environment,
            authority_intent,
            None,
            None,
            queue_intent,
        )
    }

    /// Issues trusted scheduler provenance from a claimed cron job.
    pub(in crate::application::runtime_kernel_v2) fn cron(
        caller: AdmissionCaller,
        environment: AdmissionEnvironmentSnapshot,
        authority_intent: ResolvedRuntimeAuthorityIntent,
        origin_run_id: Option<String>,
        queue_intent: Option<AdmissionQueueIntent>,
    ) -> Self {
        Self::issue(
            AdmissionOrigin::Cron,
            caller,
            environment,
            authority_intent,
            origin_run_id,
            None,
            queue_intent,
        )
    }

    /// Issues trusted internal provenance from a claimed host task.
    pub(in crate::application::runtime_kernel_v2) fn internal(
        caller: AdmissionCaller,
        environment: AdmissionEnvironmentSnapshot,
        authority_intent: ResolvedRuntimeAuthorityIntent,
        origin_run_id: Option<String>,
        queue_intent: Option<AdmissionQueueIntent>,
    ) -> Self {
        Self::issue(
            AdmissionOrigin::Internal,
            caller,
            environment,
            authority_intent,
            origin_run_id,
            None,
            queue_intent,
        )
    }

    /// Issues trusted delegation provenance from validated child authority.
    pub(in crate::application::runtime_kernel_v2) fn delegation(
        caller: AdmissionCaller,
        environment: AdmissionEnvironmentSnapshot,
        authority_intent: ResolvedRuntimeAuthorityIntent,
        origin_run_id: String,
        delegated_admission_json: String,
        queue_intent: Option<AdmissionQueueIntent>,
    ) -> Self {
        Self::issue(
            AdmissionOrigin::Delegation,
            caller,
            environment,
            authority_intent,
            Some(origin_run_id),
            Some(delegated_admission_json),
            queue_intent,
        )
    }

    #[cfg(test)]
    pub(crate) fn test_only(
        origin: AdmissionOrigin,
        caller: AdmissionCaller,
        environment: AdmissionEnvironmentSnapshot,
        authority_intent: ResolvedRuntimeAuthorityIntent,
        origin_run_id: Option<String>,
        delegated_admission_json: Option<String>,
        queue_intent: Option<AdmissionQueueIntent>,
    ) -> Self {
        Self::issue(
            origin,
            caller,
            environment,
            authority_intent,
            origin_run_id,
            delegated_admission_json,
            queue_intent,
        )
    }

    pub(crate) fn into_parts(self) -> HostVerifiedRunAdmissionParts {
        HostVerifiedRunAdmissionParts {
            origin: self.origin,
            caller: self.caller,
            environment: self.environment,
            authority_intent: self.authority_intent,
            origin_run_id: self.origin_run_id,
            delegated_admission_json: self.delegated_admission_json,
            queue_intent: self.queue_intent,
        }
    }
}

pub(crate) struct HostVerifiedRunAdmissionParts {
    pub(crate) origin: AdmissionOrigin,
    pub(crate) caller: AdmissionCaller,
    pub(crate) environment: AdmissionEnvironmentSnapshot,
    pub(crate) authority_intent: ResolvedRuntimeAuthorityIntent,
    pub(crate) origin_run_id: Option<String>,
    pub(crate) delegated_admission_json: Option<String>,
    pub(crate) queue_intent: Option<AdmissionQueueIntent>,
}

/// Non-forgeable persisted M018 admission binding consumed by selection.
///
/// M018 must construct this only after the exact runtime-authority decision and admission
/// snapshot have been durably committed. It is neither cloneable nor serde.
pub(crate) struct PersistedAdmissionAuthorityToken {
    decision: RuntimeAuthorityDecisionV1,
    admission_snapshot_digest: SelectionDigest,
    persisted_token_digest: SelectionDigest,
    _private: (),
}

impl fmt::Debug for PersistedAdmissionAuthorityToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PersistedAdmissionAuthorityToken")
            .field("profile", &self.decision.profile())
            .field("generation", &self.decision.generation())
            .field("admission_snapshot_digest", &self.admission_snapshot_digest)
            .field("persisted_token_digest", &"[redacted]")
            .finish()
    }
}

impl PersistedAdmissionAuthorityToken {
    /// Internal constructor reached only through the verified M018 bridge.
    fn from_persisted_decision(
        decision: RuntimeAuthorityDecisionV1,
        admission_snapshot_digest: SelectionDigest,
        persisted_token_digest: SelectionDigest,
    ) -> Self {
        Self { decision, admission_snapshot_digest, persisted_token_digest, _private: () }
    }
}

/// Single-use host proof tied to one active persisted Run lease.
pub(crate) struct HostRuntimeSelectionAuthorityProof {
    identities: RuntimeIdentitySetV1,
    run_lease: GenerationLeaseV1,
    persisted_admission: PersistedAdmissionAuthorityToken,
    epochs: SelectionEpochsV1,
    _private: (),
}

impl fmt::Debug for HostRuntimeSelectionAuthorityProof {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HostRuntimeSelectionAuthorityProof")
            .field("trace_id", &self.identities.trace_id)
            .field("session_id", &self.identities.session_id)
            .field("run_id", &self.identities.run_id)
            .field("generation", &self.identities.generation)
            .field("lease_id", &self.run_lease.lease_id)
            .field("persisted_admission", &"[redacted]")
            .field("epochs", &self.epochs)
            .finish()
    }
}

impl HostRuntimeSelectionAuthorityProof {
    /// Consumes exact committed M018 authority after rechecking its active lease.
    ///
    /// The admission token is non-cloneable and can only be created after the
    /// journal commit. Selection repeats the active-lease check so a stale token
    /// cannot issue executable authority.
    pub(in crate::application::runtime_kernel_v2) fn from_persisted_v2_admission(
        journal: &JournalStore,
        admission: PersistedV2AdmissionToken,
        epochs: SelectionEpochsV1,
    ) -> Result<Self, RuntimeSelectionError> {
        let parts = admission.into_parts();
        parts.identities.validate().map_err(|_| RuntimeSelectionError::AuthorityProofMismatch)?;
        parts.run_lease.validate().map_err(|_| RuntimeSelectionError::AuthorityProofMismatch)?;
        parts
            .authority_decision
            .validate()
            .map_err(|_| RuntimeSelectionError::AuthorityProofMismatch)?;
        let serialized_decision = serde_json::to_value(&parts.authority_decision)
            .map_err(|_| RuntimeSelectionError::Serialization)?;
        if stable_sha256(canonical_json_bytes(&serialized_decision).as_slice())
            != parts.authority_decision_sha256
        {
            return Err(RuntimeSelectionError::AuthorityProofMismatch);
        }
        let active_lease = journal
            .active_runtime_generation_for_run(
                parts.identities.run_id.as_str(),
                RuntimeGenerationLane::Run,
            )
            .map_err(|_| RuntimeSelectionError::AuthorityProofMismatch)?
            .ok_or(RuntimeSelectionError::AuthorityProofMismatch)?;
        if active_lease != parts.run_lease {
            return Err(RuntimeSelectionError::AuthorityProofMismatch);
        }
        let session_pin = journal
            .load_session_runtime_authority(parts.identities.session_id.as_str())
            .map_err(|_| RuntimeSelectionError::AuthorityProofMismatch)?
            .ok_or(RuntimeSelectionError::AuthorityProofMismatch)?;
        if session_pin.revision != parts.session_authority_pin_revision
            || session_pin.pin_sha256 != parts.session_authority_pin_sha256
        {
            return Err(RuntimeSelectionError::AuthorityProofMismatch);
        }
        let admission_snapshot_digest =
            SelectionDigest::parse(parts.admission_snapshot_sha256.clone())
                .map_err(|_| RuntimeSelectionError::AuthorityProofMismatch)?;
        SelectionDigest::parse(parts.kernel_head_sha256.clone())
            .map_err(|_| RuntimeSelectionError::AuthorityProofMismatch)?;
        SelectionDigest::parse(parts.policy_sha256.clone())
            .map_err(|_| RuntimeSelectionError::AuthorityProofMismatch)?;
        if parts.session_authority_pin_revision == 0 {
            return Err(RuntimeSelectionError::AuthorityProofMismatch);
        }
        SelectionDigest::parse(parts.session_authority_pin_sha256.clone())
            .map_err(|_| RuntimeSelectionError::AuthorityProofMismatch)?;
        let persisted_binding = PersistedAdmissionTokenBinding {
            identities: &parts.identities,
            initial_attempt_id: parts.initial_attempt_id.as_str(),
            run_lease: &parts.run_lease,
            authority_decision_sha256: parts.authority_decision_sha256.as_str(),
            session_authority_pin_revision: parts.session_authority_pin_revision,
            session_authority_pin_sha256: parts.session_authority_pin_sha256.as_str(),
            admission_snapshot_sha256: parts.admission_snapshot_sha256.as_str(),
            kernel_head_sha256: parts.kernel_head_sha256.as_str(),
            policy_sha256: parts.policy_sha256.as_str(),
        };
        let binding_json = serde_json::to_vec(&persisted_binding)
            .map_err(|_| RuntimeSelectionError::Serialization)?;
        let persisted_token_digest =
            SelectionDigest::from_domain_bytes(PERSISTED_ADMISSION_TOKEN_DOMAIN, &binding_json);
        let persisted_admission = PersistedAdmissionAuthorityToken::from_persisted_decision(
            parts.authority_decision,
            admission_snapshot_digest,
            persisted_token_digest,
        );
        Self::from_active_run_lease(parts.identities, parts.run_lease, persisted_admission, epochs)
    }

    #[cfg(test)]
    pub(crate) fn from_persisted_v2_admission_for_test(
        journal: &JournalStore,
        admission: PersistedV2AdmissionToken,
        epochs: SelectionEpochsV1,
    ) -> Result<Self, RuntimeSelectionError> {
        Self::from_persisted_v2_admission(journal, admission, epochs)
    }

    /// Binds a persisted M018 admission decision to the exact active Run lease.
    ///
    /// The host must obtain the lease from the journal's active-generation
    /// check. No caller-provided clock participates in grant issuance.
    pub(in crate::application::runtime_kernel_v2) fn from_active_run_lease(
        identities: RuntimeIdentitySetV1,
        run_lease: GenerationLeaseV1,
        persisted_admission: PersistedAdmissionAuthorityToken,
        epochs: SelectionEpochsV1,
    ) -> Result<Self, RuntimeSelectionError> {
        identities.validate().map_err(|_| RuntimeSelectionError::AuthorityProofMismatch)?;
        run_lease.validate().map_err(|_| RuntimeSelectionError::AuthorityProofMismatch)?;
        epochs.validate()?;
        if run_lease.lane != RuntimeGenerationLane::Run
            || run_lease.session_id != identities.session_id
            || run_lease.run_id.as_ref() != Some(&identities.run_id)
            || run_lease.generation != identities.generation
            || persisted_admission.decision.generation() != identities.generation
        {
            return Err(RuntimeSelectionError::AuthorityProofMismatch);
        }
        Ok(Self { identities, run_lease, persisted_admission, epochs, _private: () })
    }

    #[cfg(test)]
    pub(crate) fn test_only(
        identities: RuntimeIdentitySetV1,
        run_lease: GenerationLeaseV1,
        decision: RuntimeAuthorityDecisionV1,
        admission_snapshot_digest: SelectionDigest,
        persisted_token_digest: SelectionDigest,
        epochs: SelectionEpochsV1,
    ) -> Result<Self, RuntimeSelectionError> {
        Self::from_active_run_lease(
            identities,
            run_lease,
            PersistedAdmissionAuthorityToken::from_persisted_decision(
                decision,
                admission_snapshot_digest,
                persisted_token_digest,
            ),
            epochs,
        )
    }

    pub(super) const fn identities(&self) -> &RuntimeIdentitySetV1 {
        &self.identities
    }

    pub(super) const fn decision(&self) -> &RuntimeAuthorityDecisionV1 {
        &self.persisted_admission.decision
    }

    #[cfg(test)]
    pub(crate) const fn decision_for_test(&self) -> &RuntimeAuthorityDecisionV1 {
        self.decision()
    }

    pub(super) const fn admission_snapshot_digest(&self) -> &SelectionDigest {
        &self.persisted_admission.admission_snapshot_digest
    }

    pub(super) const fn persisted_token_digest(&self) -> &SelectionDigest {
        &self.persisted_admission.persisted_token_digest
    }

    pub(super) const fn epochs(&self) -> &SelectionEpochsV1 {
        &self.epochs
    }
}

#[derive(Serialize)]
struct PersistedAdmissionTokenBinding<'a> {
    identities: &'a RuntimeIdentitySetV1,
    initial_attempt_id: &'a str,
    run_lease: &'a GenerationLeaseV1,
    authority_decision_sha256: &'a str,
    session_authority_pin_revision: u64,
    session_authority_pin_sha256: &'a str,
    admission_snapshot_sha256: &'a str,
    kernel_head_sha256: &'a str,
    policy_sha256: &'a str,
}

fn stable_sha256(payload: &[u8]) -> String {
    use sha2::{Digest, Sha256};

    hex::encode(Sha256::digest(payload))
}

/// Non-cloneable executable authority for one selected generation.
pub(crate) struct AuthoritativeRuntimeGrant {
    identities: RuntimeIdentitySetV1,
    run_lease: GenerationLeaseV1,
    selected_profile: RuntimeKernelVersion,
    selected_authority: Option<RuntimeAuthority>,
    admission_snapshot_digest: SelectionDigest,
    persisted_token_digest: SelectionDigest,
    epochs_digest: SelectionDigest,
    selection_digest: SelectionDigest,
}

impl fmt::Debug for AuthoritativeRuntimeGrant {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthoritativeRuntimeGrant")
            .field("trace_id", &self.identities.trace_id)
            .field("session_id", &self.identities.session_id)
            .field("run_id", &self.identities.run_id)
            .field("generation", &self.identities.generation)
            .field("lease_id", &self.run_lease.lease_id)
            .field("selected_profile", &self.selected_profile)
            .field("selection_digest", &self.selection_digest)
            .field("persisted_admission", &"[redacted]")
            .finish()
    }
}

impl AuthoritativeRuntimeGrant {
    pub(super) fn issue(
        proof: HostRuntimeSelectionAuthorityProof,
        selection_digest: SelectionDigest,
    ) -> Self {
        Self {
            selected_profile: proof.persisted_admission.decision.profile(),
            selected_authority: proof.persisted_admission.decision.selected_runtime(),
            admission_snapshot_digest: proof.persisted_admission.admission_snapshot_digest,
            persisted_token_digest: proof.persisted_admission.persisted_token_digest,
            epochs_digest: proof.epochs.digest().clone(),
            identities: proof.identities,
            run_lease: proof.run_lease,
            selection_digest,
        }
    }

    #[must_use]
    pub(crate) const fn selected_profile(&self) -> RuntimeKernelVersion {
        self.selected_profile
    }

    #[must_use]
    pub(crate) const fn selected_authority(&self) -> Option<RuntimeAuthority> {
        self.selected_authority
    }

    #[must_use]
    pub(crate) const fn trace_id(&self) -> &palyra_common::runtime_contracts::RuntimeTraceId {
        &self.identities.trace_id
    }

    #[must_use]
    pub(crate) const fn session_id(&self) -> &palyra_common::runtime_contracts::RuntimeSessionId {
        &self.identities.session_id
    }

    #[must_use]
    pub(crate) const fn run_id(&self) -> &palyra_common::runtime_contracts::RuntimeRunId {
        &self.identities.run_id
    }

    #[must_use]
    pub(crate) const fn run_generation(
        &self,
    ) -> palyra_common::runtime_contracts::RuntimeGeneration {
        self.identities.generation
    }

    #[must_use]
    pub(crate) const fn run_lease_id(&self) -> &palyra_common::runtime_contracts::RuntimeLeaseId {
        &self.run_lease.lease_id
    }

    #[must_use]
    pub(crate) const fn selection_epochs_digest(&self) -> &SelectionDigest {
        &self.epochs_digest
    }

    /// Returns the exact committed admission snapshot bound into this grant.
    #[must_use]
    pub(crate) const fn admission_snapshot_digest(&self) -> &SelectionDigest {
        &self.admission_snapshot_digest
    }

    /// Returns the opaque digest of the committed single-use admission token.
    #[must_use]
    pub(crate) const fn persisted_admission_token_digest(&self) -> &SelectionDigest {
        &self.persisted_token_digest
    }

    #[must_use]
    pub(crate) const fn selection_digest(&self) -> &SelectionDigest {
        &self.selection_digest
    }

    #[cfg(test)]
    pub(super) fn matches_proof(&self, proof: &HostRuntimeSelectionAuthorityProof) -> bool {
        self.identities.trace_id == proof.identities.trace_id
            && self.identities.session_id == proof.identities.session_id
            && self.identities.run_id == proof.identities.run_id
            && self.identities.generation == proof.identities.generation
            && self.run_lease.lease_id == proof.run_lease.lease_id
            && self.run_lease.generation == proof.run_lease.generation
            && self.selected_profile == proof.decision().profile()
            && self.selected_authority == proof.decision().selected_runtime()
            && self.admission_snapshot_digest == *proof.admission_snapshot_digest()
            && self.persisted_token_digest == *proof.persisted_token_digest()
            && self.epochs_digest == *proof.epochs().digest()
    }
}

/// Durable selection plus its one executable grant.
pub(crate) struct ResolvedRuntimeSelection {
    projection: RuntimeSelectionV1,
    grant: AuthoritativeRuntimeGrant,
}

impl fmt::Debug for ResolvedRuntimeSelection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResolvedRuntimeSelection")
            .field("projection", &self.projection)
            .field("grant", &self.grant)
            .finish()
    }
}

impl ResolvedRuntimeSelection {
    pub(super) const fn new(
        projection: RuntimeSelectionV1,
        grant: AuthoritativeRuntimeGrant,
    ) -> Self {
        Self { projection, grant }
    }

    #[must_use]
    pub(crate) const fn projection(&self) -> &RuntimeSelectionV1 {
        &self.projection
    }

    #[must_use]
    #[cfg(test)]
    pub(crate) const fn grant(&self) -> &AuthoritativeRuntimeGrant {
        &self.grant
    }

    pub(crate) fn into_parts(self) -> (RuntimeSelectionV1, AuthoritativeRuntimeGrant) {
        (self.projection, self.grant)
    }
}

/// Typed fallback trigger.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum RuntimeFallbackTriggerV1 {
    HarnessUnavailable { reason_code: SafeLabel },
    ProviderRouteUnavailable { reason_code: SafeLabel },
}

/// Canonical output-progress state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OutputProgressV1 {
    NoOutput,
    PartialOutput,
}

/// Typed progress evidence used by the fallback fence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RuntimeSelectionProgressV1 {
    output: OutputProgressV1,
    side_effect_state: Option<SideEffectFenceState>,
}

impl RuntimeSelectionProgressV1 {
    #[must_use]
    #[cfg(test)]
    pub(crate) const fn new(
        output: OutputProgressV1,
        side_effect_state: Option<SideEffectFenceState>,
    ) -> Self {
        Self { output, side_effect_state }
    }

    #[must_use]
    #[cfg(test)]
    pub(crate) const fn pristine() -> Self {
        Self::new(OutputProgressV1::NoOutput, None)
    }

    #[cfg(test)]
    pub(super) const fn blocks_fallback(self) -> bool {
        matches!(self.output, OutputProgressV1::PartialOutput)
            || matches!(
                self.side_effect_state,
                Some(
                    SideEffectFenceState::EffectStarted
                        | SideEffectFenceState::EffectObserved
                        | SideEffectFenceState::EffectUnknown
                        | SideEffectFenceState::Reconciled
                        | SideEffectFenceState::Abandoned
                )
            )
    }
}

/// Stable failure cause separated from the returned prior authority.
#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum FallbackFailureCause {
    #[error("fallback is blocked after observable progress")]
    ProgressFence,
    #[error("fallback request does not match prior authority")]
    AuthorityMismatch,
    #[error("fallback policy or input drifted")]
    InputDrift,
    #[error("requested fallback is forbidden")]
    Forbidden,
    #[error("no compatible fallback candidate is available")]
    NoCandidate,
    #[error("fallback selection failed: {0}")]
    Selection(RuntimeSelectionError),
}

/// Lossless fallback failure carrying the exact unchanged prior resolution.
#[cfg(test)]
pub(crate) struct FallbackFailure {
    cause: FallbackFailureCause,
    prior: ResolvedRuntimeSelection,
}

#[cfg(test)]
impl fmt::Debug for FallbackFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FallbackFailure")
            .field("cause", &self.cause)
            .field("prior", &self.prior)
            .finish()
    }
}

#[cfg(test)]
impl FallbackFailure {
    pub(super) const fn new(cause: FallbackFailureCause, prior: ResolvedRuntimeSelection) -> Self {
        Self { cause, prior }
    }

    #[must_use]
    pub(crate) fn into_prior(self) -> ResolvedRuntimeSelection {
        self.prior
    }
}
