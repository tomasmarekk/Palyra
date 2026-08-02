//! Host-owned runtime-profile dispatch and observe-only shadow enrollment.
//!
//! The dispatcher is the sole production caller of runtime authority selection.
//! It retains deployment sampling material and returns only generation-pinned,
//! identity-free decisions to ingress orchestration.

use std::num::NonZeroU64;

use palyra_common::runtime_contracts::RuntimeSessionId;
use thiserror::Error;

use crate::application::run_admission::{
    journal_authority_intent, AdmissionCaller, AdmissionEnvironmentSnapshot, AdmissionQueueIntent,
};
use crate::config::{
    FeatureRolloutsConfig, RuntimeKernelConfig, RuntimeKernelProfile,
    RuntimeKernelSamplingKeySource,
};
use crate::journal::{
    run_admission::{JournalInitialSessionAuthorityPinRequest, JournalSessionAuthorityPinOutcome},
    JournalError, JournalStore,
};

use super::{
    profile_resolver::{
        ExistingSessionAuthorityBinding, ResolvedRuntimeProfileV1, RuntimeProfileResolver,
        RuntimeProfileResolverError, SessionAuthorityResolution,
    },
    runtime_selection::{HostVerifiedRunAdmission, HostVerifiedSessionAuthorityMigration},
    selection::{
        ResolvedRuntimeAuthorityIntent, RuntimeAuthority, RuntimeAuthorityDecisionV1,
        RuntimeAuthorityProgressEvidence, V2RuntimeAvailability,
    },
    shadow::{
        ShadowCandidatePlannerV1, ShadowDifferentialObserver, ShadowObservationResult,
        ShadowObserverError, ShadowPlanSnapshotV1, ShadowSamplingPolicyV1,
    },
};

/// Trusted host provenance after transport or scheduler authentication.
///
/// This vocabulary remains private so an arbitrary crate caller cannot map a
/// transport-controlled origin string into stronger host provenance.
#[derive(Debug)]
enum RuntimeIngressProvenance {
    Console,
    Channel,
    Cron { origin_run_id: Option<String> },
    Internal { origin_run_id: Option<String> },
    Delegation { origin_run_id: String, delegated_admission_json: String },
}

/// Closed execution route selected for one admitted runtime generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RuntimeDispatchDecision {
    /// The existing orchestration loop owns provider, tools, and finalization.
    Legacy { authority: RuntimeAuthorityDecisionV1 },
    /// Legacy remains authoritative while V2 receives only sanitized plan data.
    LegacyWithShadow { authority: RuntimeAuthorityDecisionV1 },
    /// RuntimeKernelV2 exclusively owns the generation.
    V2 { authority: RuntimeAuthorityDecisionV1 },
    /// No implementation was granted authority.
    Blocked { authority: RuntimeAuthorityDecisionV1 },
}

/// Runtime ownership retained for the lifetime of one gRPC stream.
#[derive(Debug)]
pub(crate) enum RunStreamRuntimeDispatch {
    /// No first-message admission has completed.
    Uninitialized,
    /// One exact post-admission generation decision owns the stream.
    Active {
        decision: RuntimeDispatchDecision,
        v2_admission: Option<Box<crate::application::run_admission::PersistedV2AdmissionToken>>,
        shadow_observation_completed: bool,
    },
    /// Admission rejected or durably queued the request; no runtime may start.
    AdmissionClosed,
}

impl RuntimeDispatchDecision {
    /// Returns the exact decision that must be persisted and reused for the generation.
    #[must_use]
    pub(crate) const fn authority(&self) -> &RuntimeAuthorityDecisionV1 {
        match self {
            Self::Legacy { authority }
            | Self::LegacyWithShadow { authority }
            | Self::V2 { authority }
            | Self::Blocked { authority } => authority,
        }
    }

    /// Returns shadow authority only for the explicitly shadow-enabled legacy route.
    #[must_use]
    pub(crate) const fn shadow_authority(&self) -> Option<&RuntimeAuthorityDecisionV1> {
        match self {
            Self::LegacyWithShadow { authority } => Some(authority),
            Self::Legacy { .. } | Self::V2 { .. } | Self::Blocked { .. } => None,
        }
    }
}

/// Daemon-wide host authority for runtime selection and shadow enrollment.
pub(crate) struct RuntimeKernelDispatcher {
    resolver: RuntimeProfileResolver,
    shadow_observer: Option<ShadowDifferentialObserver>,
    explicit_shadow_enrollment: bool,
    v2_availability: V2RuntimeAvailability,
}

impl std::fmt::Debug for RuntimeKernelDispatcher {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RuntimeKernelDispatcher")
            .field("diagnostics", self.resolver.diagnostics())
            .field("shadow_observer_configured", &self.shadow_observer.is_some())
            .field("explicit_shadow_enrollment", &self.explicit_shadow_enrollment)
            .field("v2_availability", &self.v2_availability)
            .finish()
    }
}

impl RuntimeKernelDispatcher {
    /// Resolves merged startup configuration and retains rollout key material.
    ///
    /// `resolved_secret_key` is accepted only when config selected a secret
    /// reference. It is copied into domain-separated selectors and is never
    /// exposed through diagnostics or dispatch results.
    ///
    /// # Errors
    /// Returns [`RuntimeDispatcherError`] when the profile, sampling material,
    /// or shadow policy is invalid.
    pub(crate) fn resolve(
        config: &RuntimeKernelConfig,
        feature_rollouts: &FeatureRolloutsConfig,
        resolved_secret_key: Option<&[u8]>,
        explicit_shadow_enrollment: bool,
        v2_availability: V2RuntimeAvailability,
    ) -> Result<Self, RuntimeDispatcherError> {
        let resolver =
            RuntimeProfileResolver::resolve(config, feature_rollouts, resolved_secret_key)?;
        let shadow_observer = if config.profile == RuntimeKernelProfile::V2Shadow {
            let key = resolved_sampling_key(config, resolved_secret_key)?
                .ok_or(RuntimeDispatcherError::SamplingKeyNotResolved)?;
            let sampling = ShadowSamplingPolicyV1::new(config.shadow_sample_basis_points, key)?;
            Some(ShadowDifferentialObserver::new(sampling))
        } else {
            None
        };
        if explicit_shadow_enrollment && shadow_observer.is_none() {
            return Err(RuntimeDispatcherError::InvalidExplicitShadowEnrollment);
        }
        Ok(Self { resolver, shadow_observer, explicit_shadow_enrollment, v2_availability })
    }

    /// Creates the test/default legacy dispatcher with no rollout key.
    ///
    /// # Errors
    /// Returns [`RuntimeDispatcherError`] if the repository's legacy defaults
    /// no longer form a valid closed profile.
    #[cfg(test)]
    pub(crate) fn legacy_default() -> Result<Self, RuntimeDispatcherError> {
        let config = RuntimeKernelConfig {
            profile: RuntimeKernelProfile::Legacy,
            ..RuntimeKernelConfig::default()
        };
        Self::resolve(
            &config,
            &FeatureRolloutsConfig::default(),
            None,
            false,
            V2RuntimeAvailability::Unavailable(super::selection::V2UnavailabilityReason::NotReady),
        )
    }

    /// Returns the identity-free startup profile and override projection.
    #[must_use]
    pub(crate) const fn diagnostics(&self) -> &ResolvedRuntimeProfileV1 {
        self.resolver.diagnostics()
    }

    /// Seals an authenticated console request for the sole admission controller.
    ///
    /// Only the authenticated run-stream ingress may call this entry point.
    #[must_use]
    pub(crate) fn issue_console_admission(
        &self,
        caller: AdmissionCaller,
        environment: AdmissionEnvironmentSnapshot,
        authority: ResolvedRuntimeAuthorityIntent,
        queue_intent: Option<AdmissionQueueIntent>,
    ) -> HostVerifiedRunAdmission {
        self.issue_admission(
            RuntimeIngressProvenance::Console,
            caller,
            environment,
            authority,
            queue_intent,
        )
    }

    /// Seals an authenticated connector request for the sole admission controller.
    ///
    /// Only the channel route after connector and ownership validation may call
    /// this entry point.
    #[must_use]
    pub(crate) fn issue_channel_admission(
        &self,
        caller: AdmissionCaller,
        environment: AdmissionEnvironmentSnapshot,
        authority: ResolvedRuntimeAuthorityIntent,
        queue_intent: Option<AdmissionQueueIntent>,
    ) -> HostVerifiedRunAdmission {
        self.issue_admission(
            RuntimeIngressProvenance::Channel,
            caller,
            environment,
            authority,
            queue_intent,
        )
    }

    /// Seals scheduler-owned provenance for the sole admission controller.
    ///
    /// Only the scheduler path may supply the originating run identifier.
    #[must_use]
    pub(crate) fn issue_cron_admission(
        &self,
        caller: AdmissionCaller,
        environment: AdmissionEnvironmentSnapshot,
        authority: ResolvedRuntimeAuthorityIntent,
        origin_run_id: Option<String>,
        queue_intent: Option<AdmissionQueueIntent>,
    ) -> HostVerifiedRunAdmission {
        self.issue_admission(
            RuntimeIngressProvenance::Cron { origin_run_id },
            caller,
            environment,
            authority,
            queue_intent,
        )
    }

    /// Seals daemon-owned task provenance for the sole admission controller.
    ///
    /// Only the background queue may call this entry point after resolving the
    /// authenticated task principal from host state.
    #[must_use]
    pub(crate) fn issue_internal_admission(
        &self,
        caller: AdmissionCaller,
        environment: AdmissionEnvironmentSnapshot,
        authority: ResolvedRuntimeAuthorityIntent,
        origin_run_id: Option<String>,
        queue_intent: Option<AdmissionQueueIntent>,
    ) -> HostVerifiedRunAdmission {
        self.issue_admission(
            RuntimeIngressProvenance::Internal { origin_run_id },
            caller,
            environment,
            authority,
            queue_intent,
        )
    }

    /// Seals validated child-run provenance for the sole admission controller.
    ///
    /// Only child-session delegation code may call this entry point after the
    /// delegated authority document has been authenticated.
    #[must_use]
    pub(crate) fn issue_delegation_admission(
        &self,
        caller: AdmissionCaller,
        environment: AdmissionEnvironmentSnapshot,
        authority: ResolvedRuntimeAuthorityIntent,
        origin_run_id: String,
        delegated_admission_json: String,
        queue_intent: Option<AdmissionQueueIntent>,
    ) -> HostVerifiedRunAdmission {
        self.issue_admission(
            RuntimeIngressProvenance::Delegation { origin_run_id, delegated_admission_json },
            caller,
            environment,
            authority,
            queue_intent,
        )
    }

    fn issue_admission(
        &self,
        provenance: RuntimeIngressProvenance,
        caller: AdmissionCaller,
        environment: AdmissionEnvironmentSnapshot,
        authority: ResolvedRuntimeAuthorityIntent,
        queue_intent: Option<AdmissionQueueIntent>,
    ) -> HostVerifiedRunAdmission {
        match provenance {
            RuntimeIngressProvenance::Console => {
                HostVerifiedRunAdmission::console(caller, environment, authority, queue_intent)
            }
            RuntimeIngressProvenance::Channel => {
                HostVerifiedRunAdmission::channel(caller, environment, authority, queue_intent)
            }
            RuntimeIngressProvenance::Cron { origin_run_id } => HostVerifiedRunAdmission::cron(
                caller,
                environment,
                authority,
                origin_run_id,
                queue_intent,
            ),
            RuntimeIngressProvenance::Internal { origin_run_id } => {
                HostVerifiedRunAdmission::internal(
                    caller,
                    environment,
                    authority,
                    origin_run_id,
                    queue_intent,
                )
            }
            RuntimeIngressProvenance::Delegation { origin_run_id, delegated_admission_json } => {
                HostVerifiedRunAdmission::delegation(
                    caller,
                    environment,
                    authority,
                    origin_run_id,
                    delegated_admission_json,
                    queue_intent,
                )
            }
        }
    }

    /// Loads and resolves the generation-free authority for one session.
    ///
    /// Keep-pinned policy reuses durable authority. Safe-boundary migration is
    /// sealed here, compare-and-swapped in the journal, then re-read before the
    /// returned intent can be consumed. Admission binds a V2 intent to the Run
    /// generation allocated in the same transaction.
    ///
    /// # Errors
    /// Returns [`RuntimeDispatcherError`] for invalid stored pin evidence,
    /// migration posture, keyed sampling, or authority selection.
    pub(crate) fn resolve_authority_intent(
        &self,
        journal: &JournalStore,
        session_id: &RuntimeSessionId,
        principal: Option<&str>,
        session_was_created: bool,
        at_safe_boundary: bool,
        progress: RuntimeAuthorityProgressEvidence,
    ) -> Result<ResolvedRuntimeAuthorityIntent, RuntimeDispatcherError> {
        let pin = journal.load_session_runtime_authority(session_id.as_str())?;
        let binding = if session_was_created {
            if pin.is_some() {
                return Err(RuntimeDispatcherError::UnexpectedNewSessionPin);
            }
            ExistingSessionAuthorityBinding::New
        } else {
            ExistingSessionAuthorityBinding::Existing { pinned: pin.as_ref(), at_safe_boundary }
        };
        let resolution = self.resolver.resolve_authority_intent(
            session_id,
            principal,
            binding,
            self.v2_availability,
            progress,
        )?;
        match resolution {
            SessionAuthorityResolution::Use(intent) => Ok(intent),
            SessionAuthorityResolution::Migrate { expected_revision, target } => {
                let expected_revision = NonZeroU64::new(expected_revision)
                    .ok_or(RuntimeDispatcherError::InvalidMigrationRevision)?;
                let proof = HostVerifiedSessionAuthorityMigration::configured_profile_change(
                    session_id.to_string(),
                    expected_revision,
                    journal_authority_intent(&target)
                        .map_err(|_| RuntimeDispatcherError::InvalidMigrationTarget)?,
                );
                journal.migrate_session_runtime_authority(&proof)?;
                let migrated = journal
                    .load_session_runtime_authority(session_id.as_str())?
                    .ok_or(RuntimeDispatcherError::MigrationEvidenceMissing)?;
                match self.resolver.resolve_authority_intent(
                    session_id,
                    principal,
                    ExistingSessionAuthorityBinding::Existing {
                        pinned: Some(&migrated),
                        at_safe_boundary: false,
                    },
                    self.v2_availability,
                    progress,
                )? {
                    SessionAuthorityResolution::Use(intent) => Ok(intent),
                    SessionAuthorityResolution::Migrate { .. } => {
                        Err(RuntimeDispatcherError::MigrationDidNotConverge)
                    }
                }
            }
        }
    }

    /// Persists a non-V2 session route before the legacy Run start mutates state.
    ///
    /// Exact concurrent repeats reuse the winner; a different pre-existing pin
    /// fails compare-and-swap inside the journal.
    pub(crate) fn pin_non_v2_session_authority(
        &self,
        journal: &JournalStore,
        session_id: &RuntimeSessionId,
        intent: &ResolvedRuntimeAuthorityIntent,
    ) -> Result<JournalSessionAuthorityPinOutcome, RuntimeDispatcherError> {
        if intent.selected_runtime() != Some(RuntimeAuthority::Legacy) {
            return Err(RuntimeDispatcherError::InvalidLegacyPinIntent);
        }
        Ok(journal.pin_initial_session_runtime_authority(
            &JournalInitialSessionAuthorityPinRequest {
                session_id: session_id.to_string(),
                expected_revision: 0,
                intent: journal_authority_intent(intent)
                    .map_err(|_| RuntimeDispatcherError::InvalidLegacyPinIntent)?,
                migration_reason_code: "runtime.session_authority.initial_pin".to_owned(),
            },
        )?)
    }

    /// Converts the exact post-admission decision into one closed execution route.
    ///
    /// This must be called only with the decision returned by committed
    /// admission evidence after the actual Run generation is known.
    pub(crate) fn dispatch_decision(
        &self,
        authority: RuntimeAuthorityDecisionV1,
    ) -> Result<RuntimeDispatchDecision, RuntimeDispatcherError> {
        authority.validate().map_err(|_| RuntimeDispatcherError::InvalidDispatchDecision)?;
        let decision = match (authority.selected_runtime(), authority.shadow_evaluation_enabled()) {
            (Some(RuntimeAuthority::Legacy), false) => {
                RuntimeDispatchDecision::Legacy { authority }
            }
            (Some(RuntimeAuthority::Legacy), true) => {
                RuntimeDispatchDecision::LegacyWithShadow { authority }
            }
            (Some(RuntimeAuthority::V2), false) => RuntimeDispatchDecision::V2 { authority },
            (None, false) => RuntimeDispatchDecision::Blocked { authority },
            (Some(RuntimeAuthority::V2), true) | (None, true) => {
                return Err(RuntimeDispatcherError::InvalidDispatchDecision);
            }
        };
        Ok(decision)
    }

    /// Applies the sole shadow enrollment decision and compares sanitized plans.
    ///
    /// # Errors
    /// Returns [`RuntimeDispatcherError`] when shadow mode is not configured or
    /// the authority/plan inputs violate the observe-only contract.
    pub(crate) fn observe_shadow(
        &self,
        sampling_identity: &[u8],
        authority: &RuntimeAuthorityDecisionV1,
        authoritative: &ShadowPlanSnapshotV1,
        candidate_planner: ShadowCandidatePlannerV1,
    ) -> Result<ShadowObservationResult, RuntimeDispatcherError> {
        let observer =
            self.shadow_observer.as_ref().ok_or(RuntimeDispatcherError::ShadowUnavailable)?;
        Ok(observer.observe(
            sampling_identity,
            self.explicit_shadow_enrollment,
            authority,
            authoritative,
            candidate_planner,
        )?)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, fs, path::PathBuf};

    #[test]
    fn production_admission_mint_callsites_are_host_owned() {
        let source_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
        let allowed = BTreeMap::from([
            (".issue_console_admission(", &["application/run_stream/admission_ingress.rs"][..]),
            (".issue_channel_admission(", &["application/run_stream/admission_ingress.rs"][..]),
            (".issue_cron_admission(", &["application/run_stream/admission_ingress.rs"][..]),
            (".issue_internal_admission(", &["application/run_stream/admission_ingress.rs"][..]),
            (".issue_delegation_admission(", &["application/run_stream/admission_ingress.rs"][..]),
        ]);
        let dispatcher = source_root.join("application/runtime_kernel_v2/dispatcher.rs");
        let mut pending = vec![source_root.clone()];
        let mut violations = Vec::new();

        while let Some(path) = pending.pop() {
            for entry in fs::read_dir(path).expect("source directory should be readable") {
                let path = entry.expect("source entry should be readable").path();
                if path.is_dir() {
                    pending.push(path);
                    continue;
                }
                if path.extension().and_then(|value| value.to_str()) != Some("rs")
                    || path == dispatcher
                {
                    continue;
                }
                let source = fs::read_to_string(&path).expect("Rust source should be readable");
                let relative = path
                    .strip_prefix(&source_root)
                    .expect("source path should remain under the crate root")
                    .to_string_lossy()
                    .replace('\\', "/");
                for (mint_call, allowed_paths) in &allowed {
                    if source.contains(mint_call) && !allowed_paths.contains(&relative.as_str()) {
                        violations.push(format!("{relative}: {mint_call}"));
                    }
                }
            }
        }

        assert!(
            violations.is_empty(),
            "runtime admission mint calls must remain on authenticated host paths: {violations:?}"
        );
    }

    #[test]
    fn production_shadow_helper_has_one_run_stream_callsite() {
        let source_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
        let orchestration =
            fs::read_to_string(source_root.join("application/run_stream/orchestration.rs"))
                .expect("run-stream orchestration source should be readable");

        assert_eq!(
            orchestration.matches("observe_and_record_runtime_shadow(").count(),
            1,
            "LegacyWithShadow must have exactly one production observation callsite"
        );
    }
}

fn resolved_sampling_key(
    config: &RuntimeKernelConfig,
    resolved_secret_key: Option<&[u8]>,
) -> Result<Option<[u8; 32]>, RuntimeDispatcherError> {
    match config.sampling_key_source.as_ref() {
        None => Ok(None),
        Some(RuntimeKernelSamplingKeySource::Inline(key)) => Ok(Some(*key.expose_bytes())),
        Some(RuntimeKernelSamplingKeySource::SecretRef(_)) => {
            let bytes =
                resolved_secret_key.ok_or(RuntimeDispatcherError::SamplingKeyNotResolved)?;
            Ok(Some(
                <[u8; 32]>::try_from(bytes)
                    .map_err(|_| RuntimeDispatcherError::InvalidSamplingKeyLength)?,
            ))
        }
    }
}

/// Fail-closed dispatcher construction or routing error.
#[derive(Debug, Error)]
pub(crate) enum RuntimeDispatcherError {
    /// Closed-profile resolution failed.
    #[error(transparent)]
    Profile(#[from] RuntimeProfileResolverError),
    /// Observe-only shadow enrollment or comparison failed.
    #[error(transparent)]
    Shadow(#[from] ShadowObserverError),
    /// Secret-backed sampling material was not resolved by the startup host.
    #[error("runtime sampling key secret was not resolved")]
    SamplingKeyNotResolved,
    /// Secret-backed sampling material was not exactly 32 bytes.
    #[error("runtime sampling key must resolve to exactly 32 bytes")]
    InvalidSamplingKeyLength,
    /// A validated authority projection did not map to one closed route.
    #[error("runtime authority decision does not map to a closed dispatcher route")]
    InvalidDispatchDecision,
    /// Shadow comparison was requested without a configured shadow observer.
    #[error("runtime shadow observation is not configured")]
    ShadowUnavailable,
    /// Explicit QA shadow enrollment was requested outside the shadow profile.
    #[error("explicit runtime shadow enrollment requires the v2_shadow profile")]
    InvalidExplicitShadowEnrollment,
    /// A session reported as newly created already had a durable runtime pin.
    #[error("new runtime session unexpectedly has persisted authority")]
    UnexpectedNewSessionPin,
    /// A blocked or V2 intent reached the legacy-only pin boundary.
    #[error("runtime legacy pin requires a selected legacy authority intent")]
    InvalidLegacyPinIntent,
    /// A persisted migration revision could not be represented as a CAS revision.
    #[error("runtime session authority migration revision must be non-zero")]
    InvalidMigrationRevision,
    /// The resolver produced an authority target that cannot be persisted.
    #[error("runtime session authority migration target is invalid")]
    InvalidMigrationTarget,
    /// A successful migration did not leave readable durable authority evidence.
    #[error("runtime session authority migration evidence is missing")]
    MigrationEvidenceMissing,
    /// Re-resolving the committed pin requested another migration.
    #[error("runtime session authority migration did not converge")]
    MigrationDidNotConverge,
    /// Durable authority-pin storage failed.
    #[error(transparent)]
    Journal(#[from] JournalError),
}
