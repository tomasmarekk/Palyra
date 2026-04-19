#[test]
fn configured_secret_inventory_text_lines_are_redacted() {
    let rendered = super::secrets_text::render_configured_secret_inventory_lines().join("\n");

    assert!(rendered.contains("configured_secrets summary=<redacted>"));
    assert!(rendered.contains("use --json for structured output"));
    assert!(!rendered.contains("super-secret"));
    assert!(!rendered.contains("vault://"));
}

#[test]
fn configured_secret_explain_text_lines_are_redacted() {
    let rendered = super::secrets_text::render_configured_secret_explain_lines().join("\n");

    assert!(rendered.contains("configured_secret detail=<redacted>"));
    assert!(rendered.contains("use --json for structured output"));
    assert!(!rendered.contains("super-secret"));
    assert!(!rendered.contains("vault://"));
}
