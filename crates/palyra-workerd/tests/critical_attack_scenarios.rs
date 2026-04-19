use std::{fs, path::PathBuf};

use palyra_workerd::{
    WorkerAttestation, WorkerFleetManager, WorkerFleetPolicy, WorkerLifecycleError,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct AttackCorpus {
    worker_attestation: Vec<WorkerAttestationScenario>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ExpectedWorkerError {
    MissingEgressProxyBinding,
    DigestMismatchImage,
}

#[derive(Debug, Deserialize)]
struct WorkerAttestationScenario {
    id: String,
    severity: String,
    expected_audit_surface: String,
    expected_error: ExpectedWorkerError,
    worker_id: String,
    image_digest_sha256: String,
    build_digest_sha256: String,
    artifact_digest_sha256: String,
    egress_proxy_attested: bool,
    expected_image_digest_sha256: Option<String>,
}

fn load_corpus() -> AttackCorpus {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("fixtures")
        .join("phase6")
        .join("critical_attack_scenarios.json");
    let raw = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("failed to read attack corpus {}: {error}", path.display()));
    serde_json::from_str(&raw)
        .unwrap_or_else(|error| panic!("failed to parse attack corpus {}: {error}", path.display()))
}

#[test]
fn critical_worker_attestation_attack_corpus_stays_fail_closed() {
    let corpus = load_corpus();
    assert!(!corpus.worker_attestation.is_empty(), "worker corpus must contain attack scenarios");
    for scenario in corpus.worker_attestation {
        assert!(
            !scenario.severity.trim().is_empty(),
            "scenario {} must define a severity",
            scenario.id
        );
        assert!(
            !scenario.expected_audit_surface.trim().is_empty(),
            "scenario {} must define an audit surface",
            scenario.id
        );

        let mut manager = WorkerFleetManager::default();
        let mut policy = WorkerFleetPolicy::default();
        policy.attestation.image_digest_sha256 = scenario.expected_image_digest_sha256.clone();

        let error = manager
            .register_worker(
                WorkerAttestation {
                    worker_id: scenario.worker_id,
                    image_digest_sha256: scenario.image_digest_sha256,
                    build_digest_sha256: scenario.build_digest_sha256,
                    artifact_digest_sha256: scenario.artifact_digest_sha256,
                    egress_proxy_attested: scenario.egress_proxy_attested,
                    issued_at_unix_ms: 1_000,
                    expires_at_unix_ms: 10_000,
                },
                &policy,
                2_000,
            )
            .expect_err("critical worker attestation scenario should fail closed");

        match scenario.expected_error {
            ExpectedWorkerError::MissingEgressProxyBinding => assert!(matches!(
                error,
                WorkerLifecycleError::Attestation(
                    palyra_workerd::WorkerAttestationError::MissingEgressProxyBinding
                )
            )),
            ExpectedWorkerError::DigestMismatchImage => assert!(matches!(
                error,
                WorkerLifecycleError::Attestation(
                    palyra_workerd::WorkerAttestationError::DigestMismatch { field: "image" }
                )
            )),
        }
    }
}
