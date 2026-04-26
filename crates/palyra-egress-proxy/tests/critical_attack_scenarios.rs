use std::{fs, net::SocketAddr, path::PathBuf};

use palyra_egress_proxy::{validate_resolved_addrs, EgressPolicyError};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct AttackCorpus {
    egress: Vec<EgressScenario>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ExpectedEgressError {
    PrivateTargetBlocked,
}

#[derive(Debug, Deserialize)]
struct EgressScenario {
    id: String,
    severity: String,
    expected_audit_surface: String,
    resolved_addrs: Vec<String>,
    allow_private_targets: bool,
    expected_error: ExpectedEgressError,
}

fn load_corpus() -> AttackCorpus {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("fixtures")
        .join("security")
        .join("critical_attack_scenarios.json");
    let raw = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("failed to read attack corpus {}: {error}", path.display()));
    serde_json::from_str(&raw)
        .unwrap_or_else(|error| panic!("failed to parse attack corpus {}: {error}", path.display()))
}

#[test]
fn critical_egress_attack_corpus_blocks_private_and_rebinding_targets() {
    let corpus = load_corpus();
    assert!(!corpus.egress.is_empty(), "egress corpus must contain attack scenarios");
    for scenario in corpus.egress {
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
        let addrs = scenario
            .resolved_addrs
            .iter()
            .map(|value| {
                value.parse::<SocketAddr>().unwrap_or_else(|error| {
                    panic!("scenario {} has invalid socket address {value}: {error}", scenario.id)
                })
            })
            .collect::<Vec<_>>();
        let error = validate_resolved_addrs(addrs.as_slice(), scenario.allow_private_targets)
            .expect_err("critical egress attack scenario should fail closed");
        match scenario.expected_error {
            ExpectedEgressError::PrivateTargetBlocked => {
                assert_eq!(error, EgressPolicyError::PrivateTargetBlocked)
            }
        }
    }
}
