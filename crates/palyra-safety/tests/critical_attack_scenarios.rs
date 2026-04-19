use std::{fs, path::PathBuf};

use palyra_safety::{
    inspect_text, redact_text_for_export, transform_text_for_prompt, SafetyAction,
    SafetyContentKind, SafetyPhase, SafetySourceKind, TrustLabel,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct AttackCorpus {
    safety: Vec<SafetyScenario>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum SafetyScenarioMode {
    ScanOnly,
    PromptTransform,
    ExportRedaction,
}

#[derive(Debug, Deserialize)]
struct SafetyScenario {
    id: String,
    severity: String,
    expected_audit_surface: String,
    mode: SafetyScenarioMode,
    input: String,
    source: SafetySourceKind,
    content_kind: SafetyContentKind,
    trust_label: TrustLabel,
    expected_action: SafetyAction,
    expected_codes: Vec<String>,
    expect_wrapper: bool,
    expect_blocked: bool,
    expect_redacted: bool,
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
fn critical_attack_corpus_metadata_is_complete() {
    let corpus = load_corpus();
    assert!(!corpus.safety.is_empty(), "safety corpus must contain attack scenarios");
    for scenario in &corpus.safety {
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
    }
}

#[test]
fn critical_attack_corpus_enforces_fail_closed_safety_actions() {
    let corpus = load_corpus();
    for scenario in corpus.safety {
        let scan = inspect_text(
            scenario.input.as_str(),
            match scenario.mode {
                SafetyScenarioMode::ExportRedaction => SafetyPhase::Export,
                _ => SafetyPhase::PrePrompt,
            },
            scenario.source,
            scenario.content_kind,
            scenario.trust_label,
        );
        assert_eq!(
            scan.recommended_action, scenario.expected_action,
            "scenario {} produced unexpected recommended action",
            scenario.id
        );
        let finding_codes = scan.finding_codes();
        for expected_code in &scenario.expected_codes {
            assert!(
                finding_codes.iter().any(|code| code == expected_code),
                "scenario {} must emit finding code {} (actual={finding_codes:?})",
                scenario.id,
                expected_code
            );
        }

        match scenario.mode {
            SafetyScenarioMode::ScanOnly => {}
            SafetyScenarioMode::PromptTransform => {
                let outcome = transform_text_for_prompt(
                    scenario.input.as_str(),
                    scenario.source,
                    scenario.content_kind,
                    scenario.trust_label,
                );
                assert_eq!(
                    outcome.wrapper_applied, scenario.expect_wrapper,
                    "scenario {} wrapper state drifted",
                    scenario.id
                );
                assert_eq!(
                    outcome.blocked, scenario.expect_blocked,
                    "scenario {} blocked state drifted",
                    scenario.id
                );
            }
            SafetyScenarioMode::ExportRedaction => {
                let outcome = redact_text_for_export(
                    scenario.input.as_str(),
                    scenario.source,
                    scenario.content_kind,
                    scenario.trust_label,
                );
                assert_eq!(
                    outcome.redacted, scenario.expect_redacted,
                    "scenario {} export redaction drifted",
                    scenario.id
                );
                if scenario.expect_redacted {
                    assert!(
                        outcome.redacted_text.contains("[REDACTED_SECRET]"),
                        "scenario {} should redact secrets in export output",
                        scenario.id
                    );
                }
            }
        }
    }
}
