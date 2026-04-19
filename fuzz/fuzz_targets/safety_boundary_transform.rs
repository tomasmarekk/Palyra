#![no_main]

use libfuzzer_sys::fuzz_target;
use palyra_safety::{
    inspect_text, redact_text_for_export, transform_text_for_prompt, SafetyContentKind, SafetyPhase,
    SafetySourceKind, TrustLabel,
};

const SOURCES: &[SafetySourceKind] = &[
    SafetySourceKind::Workspace,
    SafetySourceKind::HttpFetch,
    SafetySourceKind::Browser,
    SafetySourceKind::Webhook,
    SafetySourceKind::ContextReference,
    SafetySourceKind::ToolOutput,
];

const CONTENT_KINDS: &[SafetyContentKind] = &[
    SafetyContentKind::PlainText,
    SafetyContentKind::WorkspaceDocument,
    SafetyContentKind::HttpResponse,
    SafetyContentKind::BrowserObservation,
    SafetyContentKind::BrowserConsole,
    SafetyContentKind::WebhookPayload,
    SafetyContentKind::ContextReference,
    SafetyContentKind::SupportBundle,
];

const TRUST_LABELS: &[TrustLabel] = &[
    TrustLabel::TrustedLocal,
    TrustLabel::ExternalUntrusted,
    TrustLabel::Mixed,
];

fuzz_target!(|data: &[u8]| {
    let Ok(input) = std::str::from_utf8(data) else {
        return;
    };
    let source = SOURCES[input.len() % SOURCES.len()];
    let content_kind = CONTENT_KINDS[input.len() % CONTENT_KINDS.len()];
    let trust_label = TRUST_LABELS[input.len() % TRUST_LABELS.len()];

    let _ = inspect_text(input, SafetyPhase::PrePrompt, source, content_kind, trust_label);
    let prompt_outcome = transform_text_for_prompt(input, source, content_kind, trust_label);
    let export_outcome = redact_text_for_export(input, source, content_kind, trust_label);

    let _ = prompt_outcome.scan.finding_codes();
    let _ = export_outcome.scan.finding_codes();
});
