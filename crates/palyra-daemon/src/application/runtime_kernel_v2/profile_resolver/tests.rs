use palyra_common::{
    feature_rollouts::FeatureRolloutSetting,
    runtime_contracts::{
        RuntimeGeneration, RuntimeIdentitySetV1, RuntimeRunId, RuntimeSessionId, RuntimeTraceId,
    },
};

use super::*;
use crate::config::{RuntimeKernelSamplingKey, RuntimeKernelSamplingKeySource};

fn identities(session: &str) -> RuntimeIdentitySetV1 {
    RuntimeIdentitySetV1::for_run(
        RuntimeTraceId::parse("trace_profile").expect("test trace id is valid"),
        RuntimeSessionId::parse(session).expect("test session id is valid"),
        RuntimeRunId::parse("run_profile").expect("test run id is valid"),
        RuntimeGeneration::new(1).expect("test generation is non-zero"),
    )
}

fn set_bundle(rollouts: &mut FeatureRolloutsConfig, enabled: bool) {
    let setting = FeatureRolloutSetting::from_config(enabled);
    rollouts.context_engine = setting;
    rollouts.provider_stream_normalizer = setting;
    rollouts.provider_recovery = setting;
    rollouts.session_queue_policy = setting;
    rollouts.replay_capture = setting;
    rollouts.delivery_arbitration = setting;
}

fn inline_key() -> RuntimeKernelSamplingKeySource {
    RuntimeKernelSamplingKeySource::Inline(
        RuntimeKernelSamplingKey::parse_hex(
            "11".repeat(32).as_str(),
            "test.runtime_kernel.sampling_key_hex",
        )
        .expect("test key is valid"),
    )
}

fn inline_key_with_byte(byte: &str) -> RuntimeKernelSamplingKeySource {
    RuntimeKernelSamplingKeySource::Inline(
        RuntimeKernelSamplingKey::parse_hex(
            byte.repeat(32).as_str(),
            "test.runtime_kernel.sampling_key_hex",
        )
        .expect("test key is valid"),
    )
}

#[test]
fn config_matrix_resolves_only_atomic_profile_bundles() {
    let config = RuntimeKernelConfig::default();
    let mut rollouts = FeatureRolloutsConfig::default();
    set_bundle(&mut rollouts, true);
    let resolver =
        RuntimeProfileResolver::resolve(&config, &rollouts, None).expect("matrix is valid");
    let decision = resolver
        .resolve_authority(
            &identities("session_matrix"),
            None,
            ExistingSessionBinding::New,
            None,
            V2RuntimeAvailability::Ready,
            RuntimeAuthorityProgressEvidence::pristine(),
        )
        .expect("authority should resolve");
    let bundle = resolver.component_bundle(&decision).expect("bundle should resolve");
    assert_eq!(bundle.generation(), RuntimeComponentGeneration::V2);
    let serialized = serde_json::to_value(bundle).expect("bundle should serialize");
    assert!(serialized
        .as_object()
        .is_some_and(|fields| fields.values().all(|value| value.as_str() == Some("v2"))));

    let partial = FeatureRolloutsConfig {
        context_engine: FeatureRolloutSetting::from_config(true),
        ..FeatureRolloutsConfig::default()
    };
    assert!(
        RuntimeProfileResolver::resolve(&RuntimeKernelConfig::default(), &partial, None).is_err()
    );
}

#[test]
fn legacy_profile_rejects_new_sessions_but_preserves_existing_session_reads() {
    let config = RuntimeKernelConfig {
        profile: RuntimeKernelProfile::Legacy,
        ..RuntimeKernelConfig::default()
    };
    let resolver =
        RuntimeProfileResolver::resolve(&config, &FeatureRolloutsConfig::default(), None)
            .expect("legacy compatibility profile should remain readable");
    let error = match resolver.resolve_authority_intent(
        &RuntimeSessionId::parse("session_new_retired").unwrap(),
        None,
        ExistingSessionAuthorityBinding::New,
        V2RuntimeAvailability::Ready,
        RuntimeAuthorityProgressEvidence::pristine(),
    ) {
        Err(error) => error,
        Ok(_) => panic!("legacy profile must not acquire new-session authority"),
    };
    assert!(matches!(error, RuntimeProfileResolverError::LegacyNewSessionRetired));
    assert!(error.to_string().contains("use runtime_kernel.profile=v2"));

    let existing = resolver
        .profile_for_session(ExistingSessionBinding::Existing {
            pinned_profile: None,
            at_safe_boundary: false,
        })
        .expect("existing unpinned compatibility state should remain readable");
    assert_eq!(existing.profile(), RuntimeKernelVersion::Legacy);
}

#[test]
fn principal_canary_is_stable_and_diagnostics_are_identity_and_key_free() {
    let config = RuntimeKernelConfig {
        profile: RuntimeKernelProfile::V2Canary,
        canary_basis_points: 5_000,
        sampling_identity: RuntimeKernelSamplingIdentity::Principal,
        sampling_key_source: Some(inline_key()),
        ..RuntimeKernelConfig::default()
    };
    let resolver =
        RuntimeProfileResolver::resolve(&config, &FeatureRolloutsConfig::default(), None)
            .expect("principal canary should resolve");
    let first = resolver
        .resolve_authority(
            &identities("session_one"),
            Some("principal-secret-identity"),
            ExistingSessionBinding::New,
            None,
            V2RuntimeAvailability::Ready,
            RuntimeAuthorityProgressEvidence::pristine(),
        )
        .expect("first authority should resolve");
    let second = resolver
        .resolve_authority(
            &identities("session_two"),
            Some("principal-secret-identity"),
            ExistingSessionBinding::New,
            None,
            V2RuntimeAvailability::Ready,
            RuntimeAuthorityProgressEvidence::pristine(),
        )
        .expect("second authority should resolve");
    assert_eq!(first.selected_runtime(), second.selected_runtime());

    let serialized =
        serde_json::to_string(resolver.diagnostics()).expect("diagnostics should serialize");
    assert!(!serialized.contains("principal-secret-identity"));
    assert!(!serialized.contains("111111"));
    assert!(!serialized.contains("bucket"));
}

#[test]
fn existing_sessions_stay_legacy_until_explicit_safe_boundary_migration() {
    let config = RuntimeKernelConfig {
        profile: RuntimeKernelProfile::V2,
        existing_session_policy: ExistingSessionMigrationPolicy::MigrateAtSafeBoundary,
        ..RuntimeKernelConfig::default()
    };
    let mut rollouts = FeatureRolloutsConfig::default();
    set_bundle(&mut rollouts, true);
    let resolver =
        RuntimeProfileResolver::resolve(&config, &rollouts, None).expect("V2 should resolve");

    let before = resolver
        .profile_for_session(ExistingSessionBinding::Existing {
            pinned_profile: None,
            at_safe_boundary: false,
        })
        .expect("unsafe-boundary session should remain pinned");
    let after = resolver
        .profile_for_session(ExistingSessionBinding::Existing {
            pinned_profile: None,
            at_safe_boundary: true,
        })
        .expect("safe-boundary session should migrate");
    assert_eq!(before.profile(), RuntimeKernelVersion::Legacy);
    assert_eq!(after.profile(), RuntimeKernelVersion::V2);
}

#[test]
fn persisted_session_pin_survives_restart_config_change_and_key_rotation() {
    let pin = JournalSessionAuthorityPin {
        schema_version: 1,
        revision: 3,
        configured_profile: JournalRuntimeProfile::V2Canary,
        selected_runtime: JournalRuntimeAuthority::Legacy,
        reason: JournalRuntimeAuthorityReason::V2CanarySessionExcluded,
        shadow_evaluation_enabled: false,
        created_after_run_generation: 7,
        created_at_unix_ms: 1,
        migration_reason_code: "runtime.session_authority.test".to_owned(),
        safe_boundary_evidence: Some(serde_json::json!({"active_run_absent": true})),
        pin_sha256: "a".repeat(64),
    };
    let session_id = RuntimeSessionId::parse("session_restart").unwrap();
    let config_before = RuntimeKernelConfig {
        profile: RuntimeKernelProfile::V2Canary,
        canary_basis_points: 5_000,
        sampling_key_source: Some(inline_key_with_byte("11")),
        ..RuntimeKernelConfig::default()
    };
    let config_after = RuntimeKernelConfig {
        profile: RuntimeKernelProfile::V2Canary,
        canary_basis_points: 9_000,
        sampling_key_source: Some(inline_key_with_byte("22")),
        ..RuntimeKernelConfig::default()
    };
    let before =
        RuntimeProfileResolver::resolve(&config_before, &FeatureRolloutsConfig::default(), None)
            .unwrap();
    let after =
        RuntimeProfileResolver::resolve(&config_after, &FeatureRolloutsConfig::default(), None)
            .unwrap();
    let resolve = |resolver: &RuntimeProfileResolver| match resolver
        .resolve_authority_intent(
            &session_id,
            None,
            ExistingSessionAuthorityBinding::Existing {
                pinned: Some(&pin),
                at_safe_boundary: true,
            },
            V2RuntimeAvailability::Ready,
            RuntimeAuthorityProgressEvidence::pristine(),
        )
        .unwrap()
    {
        SessionAuthorityResolution::Use(intent) => intent,
        SessionAuthorityResolution::Migrate { .. } => {
            panic!("keep-pinned restart must not request migration")
        }
    };
    let first = resolve(&before);
    let restarted = resolve(&after);
    assert_eq!(first.profile(), RuntimeKernelVersion::V2Canary);
    assert_eq!(first.selected_runtime(), Some(RuntimeAuthority::Legacy));
    assert_eq!(
        first.reason(),
        crate::application::runtime_kernel_v2::selection::RuntimeAuthorityReason::V2CanarySessionExcluded
    );
    assert_eq!(first.profile(), restarted.profile());
    assert_eq!(first.selected_runtime(), restarted.selected_runtime());
    assert_eq!(first.reason(), restarted.reason());
    assert_eq!(
        first.bind_generation(RuntimeGeneration::new(8).unwrap()).unwrap().selected_runtime(),
        restarted.bind_generation(RuntimeGeneration::new(99).unwrap()).unwrap().selected_runtime()
    );
}

#[test]
fn safe_boundary_reports_legacy_to_v2_and_rejects_runtime_rollback_to_legacy() {
    let session_id = RuntimeSessionId::parse("session_migration").unwrap();
    let legacy_pin = JournalSessionAuthorityPin {
        schema_version: 1,
        revision: 4,
        configured_profile: JournalRuntimeProfile::Legacy,
        selected_runtime: JournalRuntimeAuthority::Legacy,
        reason: JournalRuntimeAuthorityReason::LegacyProfileSelected,
        shadow_evaluation_enabled: false,
        created_after_run_generation: 3,
        created_at_unix_ms: 1,
        migration_reason_code: "runtime.session_authority.initial_pin".to_owned(),
        safe_boundary_evidence: None,
        pin_sha256: "a".repeat(64),
    };
    let v2_pin = JournalSessionAuthorityPin {
        schema_version: 1,
        revision: 7,
        configured_profile: JournalRuntimeProfile::V2,
        selected_runtime: JournalRuntimeAuthority::V2,
        reason: JournalRuntimeAuthorityReason::V2ProfileSelected,
        shadow_evaluation_enabled: false,
        created_after_run_generation: 6,
        created_at_unix_ms: 1,
        migration_reason_code: "runtime.session_authority.initial_pin".to_owned(),
        safe_boundary_evidence: None,
        pin_sha256: "b".repeat(64),
    };
    let v2_config = RuntimeKernelConfig {
        profile: RuntimeKernelProfile::V2,
        existing_session_policy: ExistingSessionMigrationPolicy::MigrateAtSafeBoundary,
        ..RuntimeKernelConfig::default()
    };
    let mut v2_rollouts = FeatureRolloutsConfig::default();
    set_bundle(&mut v2_rollouts, true);
    let to_v2 = RuntimeProfileResolver::resolve(&v2_config, &v2_rollouts, None).unwrap();
    let legacy_config = RuntimeKernelConfig {
        profile: RuntimeKernelProfile::Legacy,
        existing_session_policy: ExistingSessionMigrationPolicy::MigrateAtSafeBoundary,
        ..RuntimeKernelConfig::default()
    };
    let to_legacy =
        RuntimeProfileResolver::resolve(&legacy_config, &FeatureRolloutsConfig::default(), None)
            .unwrap();

    let SessionAuthorityResolution::Migrate { expected_revision, target } = to_v2
        .resolve_authority_intent(
            &session_id,
            None,
            ExistingSessionAuthorityBinding::Existing {
                pinned: Some(&legacy_pin),
                at_safe_boundary: true,
            },
            V2RuntimeAvailability::Ready,
            RuntimeAuthorityProgressEvidence::pristine(),
        )
        .unwrap()
    else {
        panic!("legacy pin should request a V2 migration");
    };
    assert_eq!(expected_revision, 4);
    assert_eq!(target.selected_runtime(), Some(RuntimeAuthority::V2));

    let error = match to_legacy.resolve_authority_intent(
        &session_id,
        None,
        ExistingSessionAuthorityBinding::Existing { pinned: Some(&v2_pin), at_safe_boundary: true },
        V2RuntimeAvailability::Ready,
        RuntimeAuthorityProgressEvidence::pristine(),
    ) {
        Err(error) => error,
        Ok(_) => panic!("runtime rollback to retired legacy authority must fail"),
    };
    assert!(matches!(error, RuntimeProfileResolverError::LegacyNewSessionRetired));

    assert!(matches!(
        to_v2
            .resolve_authority_intent(
                &session_id,
                None,
                ExistingSessionAuthorityBinding::Existing {
                    pinned: Some(&legacy_pin),
                    at_safe_boundary: false,
                },
                V2RuntimeAvailability::Ready,
                RuntimeAuthorityProgressEvidence::pristine(),
            )
            .unwrap(),
        SessionAuthorityResolution::Use(_)
    ));
}
