//! Controller tests spanning sealed origins and durable authority ordering.

use std::{
    fs,
    path::{Path, PathBuf},
};

use palyra_common::runtime_contracts::{
    RuntimeGeneration, RuntimeIdentitySetV1, RuntimeRunId, RuntimeSessionId, RuntimeTraceId,
};
use tempfile::TempDir;

use super::*;
use crate::{
    application::runtime_kernel_v2::{
        profile::{RuntimeKernelCompatibilityOverridesV1, RuntimeKernelProfileConfigV1},
        runtime_selection::{
            HostVerifiedRunAdmission, RuntimeSelectionError, SelectionEpochsV1,
            TestHostRuntimeSelectionAuthorityProof,
        },
        selection::{
            resolve_runtime_authority, resolve_runtime_authority_intent_for_principal,
            CanarySamplingIdentity, RuntimeAuthorityProgressEvidence, SessionCanarySelector,
            V2RuntimeAvailability,
        },
    },
    journal::{JournalConfig, JournalStore},
};

fn store(path: &Path) -> JournalStore {
    JournalStore::open(JournalConfig {
        db_path: path.to_owned(),
        hash_chain_enabled: true,
        max_payload_bytes: 256 * 1024,
        max_events: 10_000,
    })
    .unwrap()
}

fn identities(session: &str, suffix: &str) -> RuntimeIdentitySetV1 {
    RuntimeIdentitySetV1::for_run(
        RuntimeTraceId::parse(format!("trace_{suffix}").as_str()).unwrap(),
        RuntimeSessionId::parse(session).unwrap(),
        RuntimeRunId::parse(format!("run_{suffix}").as_str()).unwrap(),
        RuntimeGeneration::new(1).unwrap(),
    )
}

fn v2_intent(session: &str) -> ResolvedRuntimeAuthorityIntent {
    resolve_runtime_authority_intent_for_principal(
        &RuntimeKernelProfileConfigV1::new(
            RuntimeKernelVersion::V2,
            0,
            RuntimeKernelCompatibilityOverridesV1::none(),
        )
        .unwrap(),
        &RuntimeSessionId::parse(session).unwrap(),
        None,
        V2RuntimeAvailability::Ready,
        RuntimeAuthorityProgressEvidence::pristine(),
        None,
    )
    .unwrap()
}

fn caller(principal: &str, device_id: &str) -> AdmissionCaller {
    AdmissionCaller::authenticated(
        principal.to_owned(),
        device_id.to_owned(),
        Some("console".to_owned()),
    )
}

fn environment(access_policy_json: &str, draining: bool) -> AdmissionEnvironmentSnapshot {
    AdmissionEnvironmentSnapshot::host_snapshot(
        "a".repeat(64),
        access_policy_json.to_owned(),
        r#"{"mode":"followup"}"#.to_owned(),
        draining,
        draining.then(|| "daemon.draining".to_owned()),
        8,
    )
}

fn command_with(
    origin: AdmissionOrigin,
    session: &str,
    suffix: &str,
    caller: AdmissionCaller,
    environment: AdmissionEnvironmentSnapshot,
    intent: ResolvedRuntimeAuthorityIntent,
    queue_intent: Option<AdmissionQueueIntent>,
) -> RunAdmissionCommand {
    let verified = HostVerifiedRunAdmission::test_only(
        origin,
        caller,
        environment,
        intent,
        None,
        None,
        queue_intent,
    );
    RunAdmissionCommand::from_verified(
        format!("admission_{suffix}"),
        format!("scope_{session}"),
        format!("key_{suffix}"),
        format!("trace_{suffix}"),
        format!("run_{suffix}"),
        format!("attempt_{suffix}"),
        JournalRunAdmissionSessionSelector {
            session_id: Some(session.to_owned()),
            session_key: Some(session.to_owned()),
            session_label: None,
            require_existing: false,
            reset_session: false,
        },
        verified,
    )
}

fn command(origin: AdmissionOrigin, session: &str, suffix: &str) -> RunAdmissionCommand {
    command_with(
        origin,
        session,
        suffix,
        caller("principal", "device"),
        environment(r#"{"allow":true}"#, false),
        v2_intent(session),
        None,
    )
}

#[test]
fn all_five_sealed_origins_admit_through_one_controller() {
    for (index, origin) in [
        AdmissionOrigin::Console,
        AdmissionOrigin::Channel,
        AdmissionOrigin::Cron,
        AdmissionOrigin::Internal,
        AdmissionOrigin::Delegation,
    ]
    .into_iter()
    .enumerate()
    {
        let temp = TempDir::new().unwrap();
        let store = store(&temp.path().join("journal.sqlite3"));
        let outcome = RunAdmissionController::new(&store)
            .admit(command(origin, &format!("session_{index}"), &format!("origin_{index}")))
            .unwrap();
        assert!(matches!(outcome, RunAdmissionControllerOutcome::Admitted { .. }));
    }
}

#[test]
fn command_surface_has_no_caller_writable_authority_fields() {
    let source = include_str!("../run_admission.rs");
    let command = source
        .split("pub(crate) struct RunAdmissionCommand {")
        .nth(1)
        .unwrap()
        .split('}')
        .next()
        .unwrap();
    for forbidden in [
        "pub origin:",
        "pub environment:",
        "pub profile:",
        "pub delegated_admission_json:",
        "access_allowed",
    ] {
        assert!(!command.contains(forbidden), "{forbidden} must remain sealed");
    }
    assert!(!source.contains("pub access_allowed:"));
    assert!(!source.contains("SessionCanarySelector"));
}

#[test]
fn production_proof_minting_is_dispatcher_only() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/application/runtime_kernel_v2");
    let mut pending = vec![root];
    let mut forbidden_callsites = Vec::new();
    while let Some(path) = pending.pop() {
        for entry in fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
                continue;
            }
            if path.extension().and_then(|value| value.to_str()) != Some("rs")
                || path.file_name().and_then(|value| value.to_str()) == Some("dispatcher.rs")
            {
                continue;
            }
            let source = fs::read_to_string(&path).unwrap();
            for constructor in ["console", "channel", "cron", "internal", "delegation"] {
                if source.contains(&format!("HostVerifiedRunAdmission::{constructor}(")) {
                    forbidden_callsites.push(path.clone());
                }
            }
            if source.contains("HostVerifiedSessionAuthorityMigration::configured_profile_change(")
            {
                forbidden_callsites.push(path.clone());
            }
        }
    }
    assert!(
        forbidden_callsites.is_empty(),
        "only runtime_kernel_v2/dispatcher.rs may mint production admission proofs: \
         {forbidden_callsites:?}"
    );
}

#[test]
fn access_policy_and_drain_denials_never_issue_authority() {
    let temp = TempDir::new().unwrap();
    let store = store(&temp.path().join("journal.sqlite3"));
    let controller = RunAdmissionController::new(&store);

    let denied = command_with(
        AdmissionOrigin::Console,
        "session_denied",
        "denied",
        caller("principal", "device"),
        environment(r#"{"allow":false}"#, false),
        v2_intent("session_denied"),
        None,
    );
    let (outcome, marker) = controller.admit_and_then(denied, |_| "provider_called").unwrap();
    assert!(matches!(outcome, RunAdmissionControllerOutcome::Rejected { .. }));
    assert_eq!(marker, None);

    let drain = command_with(
        AdmissionOrigin::Internal,
        "session_drain",
        "drain",
        caller("principal", "device"),
        environment(r#"{"allow":true}"#, true),
        v2_intent("session_drain"),
        None,
    );
    let (outcome, marker) = controller.admit_and_then(drain, |_| "provider_called").unwrap();
    assert!(matches!(outcome, RunAdmissionControllerOutcome::Rejected { .. }));
    assert_eq!(marker, None);
}

#[test]
fn explicit_ingress_block_is_durable_and_never_reaches_provider_work() {
    let temp = TempDir::new().unwrap();
    let store = store(&temp.path().join("journal.sqlite3"));
    let blocked = command_with(
        AdmissionOrigin::Channel,
        "session_channel_blocked",
        "channel_blocked",
        caller("principal", "device"),
        environment(r#"{"allow":true}"#, false)
            .with_ingress_block("runtime.channel_v2_adapter_unavailable".to_owned()),
        v2_intent("session_channel_blocked"),
        None,
    );
    let (outcome, marker) =
        RunAdmissionController::new(&store).admit_and_then(blocked, |_| "provider_called").unwrap();
    let RunAdmissionControllerOutcome::Rejected { journal } = outcome else {
        panic!("host-blocked channel ingress must reject");
    };
    assert_eq!(journal.reason_code, "runtime.channel_v2_adapter_unavailable");
    assert_eq!(marker, None);
    assert!(journal.allocated_run_id.is_none());
    assert!(journal.run_lease.is_none());
}

#[test]
fn identity_mismatch_and_queue_outcome_have_no_new_authority() {
    let temp = TempDir::new().unwrap();
    let store = store(&temp.path().join("journal.sqlite3"));
    let controller = RunAdmissionController::new(&store);
    controller.admit(command(AdmissionOrigin::Console, "session_guard", "initial")).unwrap();

    let mismatch = command_with(
        AdmissionOrigin::Channel,
        "session_guard",
        "mismatch",
        caller("principal", "other-device"),
        environment(r#"{"allow":true}"#, false),
        v2_intent("session_guard"),
        Some(queue("mismatch")),
    );
    assert!(matches!(
        controller.admit(mismatch),
        Err(RunAdmissionControllerError::Journal(JournalError::SessionIdentityMismatch { .. }))
    ));

    let queued = command_with(
        AdmissionOrigin::Channel,
        "session_guard",
        "queued",
        caller("principal", "device"),
        environment(r#"{"allow":true}"#, false),
        v2_intent("session_guard"),
        Some(queue("queued")),
    );
    let (outcome, marker) = controller.admit_and_then(queued, |_| "tool_called").unwrap();
    assert!(matches!(outcome, RunAdmissionControllerOutcome::Queued { .. }));
    assert_eq!(marker, None);
}

#[test]
fn exact_principal_canary_decision_is_persisted_without_session_resampling() {
    let session = "session_principal_canary";
    let principal = "principal-cohort";
    let suffix = "principal_canary";
    let config = RuntimeKernelProfileConfigV1::new(
        RuntimeKernelVersion::V2Canary,
        5_000,
        RuntimeKernelCompatibilityOverridesV1::none(),
    )
    .unwrap();
    let identities = identities(session, suffix);
    let decision = (0..512)
        .find_map(|index| {
            let key = [u8::try_from(index % 251).unwrap(); 32];
            let principal_selector = SessionCanarySelector::new_with_identity(
                5_000,
                &key,
                CanarySamplingIdentity::Principal,
            )
            .unwrap();
            let session_selector = SessionCanarySelector::new_with_identity(
                5_000,
                &key,
                CanarySamplingIdentity::Session,
            )
            .unwrap();
            let principal_intent = resolve_runtime_authority_intent_for_principal(
                &config,
                &identities.session_id,
                Some(principal),
                V2RuntimeAvailability::Ready,
                RuntimeAuthorityProgressEvidence::pristine(),
                Some(&principal_selector),
            )
            .unwrap();
            let session_decision = resolve_runtime_authority(
                &config,
                &identities,
                V2RuntimeAvailability::Ready,
                RuntimeAuthorityProgressEvidence::pristine(),
                Some(&session_selector),
            )
            .unwrap();
            (principal_intent.selected_runtime() == Some(RuntimeAuthority::V2)
                && session_decision.selected_runtime() == Some(RuntimeAuthority::Legacy))
            .then_some(principal_intent)
        })
        .expect("test keys should expose principal/session cohort divergence");
    let expected = decision.bind_generation(RuntimeGeneration::new(1).unwrap()).unwrap();

    let temp = TempDir::new().unwrap();
    let store = store(&temp.path().join("journal.sqlite3"));
    let outcome = RunAdmissionController::new(&store)
        .admit(command_with(
            AdmissionOrigin::Console,
            session,
            suffix,
            caller(principal, "device"),
            environment(r#"{"allow":true}"#, false),
            decision,
            None,
        ))
        .unwrap();
    let RunAdmissionControllerOutcome::Admitted { journal, token } = outcome else {
        panic!("exact principal-selected V2 authority should admit");
    };
    assert_eq!(token.authority_decision(), &expected);
    let pin = journal.session_authority_pin.expect("admission must bind its session pin");
    assert_eq!(pin.configured_profile, JournalRuntimeProfile::V2Canary);
    assert_eq!(pin.selected_runtime, JournalRuntimeAuthority::V2);
    assert_eq!(pin.reason, JournalRuntimeAuthorityReason::V2CanarySessionSelected);
}

#[test]
fn committed_token_bridges_exact_decision_and_rejects_stale_lease() {
    let temp = TempDir::new().unwrap();
    let store = store(&temp.path().join("journal.sqlite3"));
    let controller = RunAdmissionController::new(&store);
    let outcome =
        controller.admit(command(AdmissionOrigin::Cron, "session_bridge", "bridge")).unwrap();
    let RunAdmissionControllerOutcome::Admitted { token, .. } = outcome else {
        panic!("fresh V2 request should admit");
    };
    let expected = token.authority_decision().clone();
    let run_id = token.run_id().to_owned();
    let session_id = token.identities().session_id.as_str().to_owned();
    let proof = TestHostRuntimeSelectionAuthorityProof::from_persisted_v2_admission_for_test(
        &store,
        *token,
        SelectionEpochsV1::new(1, 1).unwrap(),
    )
    .unwrap();
    assert_eq!(proof.decision_for_test(), &expected);
    drop(proof);

    let tampered_outcome =
        controller.admit(command(AdmissionOrigin::Internal, "session_tamper", "tamper")).unwrap();
    let RunAdmissionControllerOutcome::Admitted { token: mut tampered, .. } = tampered_outcome
    else {
        panic!("fresh V2 request should admit");
    };
    tampered.tamper_authority_digest_for_test();
    assert!(matches!(
        TestHostRuntimeSelectionAuthorityProof::from_persisted_v2_admission_for_test(
            &store,
            *tampered,
            SelectionEpochsV1::new(1, 1).unwrap(),
        ),
        Err(RuntimeSelectionError::AuthorityProofMismatch)
    ));

    let stale_outcome =
        controller.admit(command(AdmissionOrigin::Internal, "session_stale", "stale")).unwrap();
    let RunAdmissionControllerOutcome::Admitted { token: mut stale, .. } = stale_outcome else {
        panic!("fresh V2 request should admit");
    };
    stale.tamper_run_lease_generation_for_test();
    assert!(matches!(
        TestHostRuntimeSelectionAuthorityProof::from_persisted_v2_admission_for_test(
            &store,
            *stale,
            SelectionEpochsV1::new(1, 1).unwrap(),
        ),
        Err(RuntimeSelectionError::AuthorityProofMismatch)
    ));

    assert_eq!(run_id, "run_bridge");
    assert_eq!(session_id, "session_bridge");
}

#[test]
fn uppercase_workspace_digest_is_rejected_as_noncanonical() {
    let temp = TempDir::new().unwrap();
    let store = store(&temp.path().join("journal.sqlite3"));
    let request = command_with(
        AdmissionOrigin::Console,
        "session_digest",
        "uppercase",
        caller("principal", "device"),
        AdmissionEnvironmentSnapshot::host_snapshot(
            "A".repeat(64),
            r#"{"allow":true}"#.to_owned(),
            r#"{"mode":"followup"}"#.to_owned(),
            false,
            None,
            8,
        ),
        v2_intent("session_digest"),
        None,
    );
    assert!(matches!(
        RunAdmissionController::new(&store).admit(request),
        Err(RunAdmissionControllerError::InvalidSnapshot(_))
    ));
}

fn queue(suffix: &str) -> AdmissionQueueIntent {
    AdmissionQueueIntent::verified(
        format!("queued_{suffix}"),
        "follow up".to_owned(),
        "followup".to_owned(),
        "default".to_owned(),
        "{}".to_owned(),
        RunAdmissionDisposition::DurableQueue,
    )
}
