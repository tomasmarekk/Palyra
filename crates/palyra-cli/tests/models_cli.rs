use std::{
    fs,
    io::{Read, Write},
    net::TcpListener,
    process::{Command, Output},
    thread,
};

use anyhow::{Context, Result};
use serde_json::Value;
use tempfile::TempDir;

fn run_cli(workdir: &TempDir, args: &[&str]) -> Result<Output> {
    Command::new(env!("CARGO_BIN_EXE_palyra"))
        .current_dir(workdir.path())
        .args(args)
        .output()
        .with_context(|| format!("failed to execute palyra {}", args.join(" ")))
}

#[test]
fn models_set_updates_text_and_embeddings_defaults() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("palyra.toml");
    fs::write(&config_path, "version = 1\n")
        .with_context(|| format!("failed to write {}", config_path.display()))?;
    let config_path_string = config_path.to_string_lossy().into_owned();

    let text_output = run_cli(
        &workdir,
        &["models", "set", "gpt-4.1-mini", "--path", &config_path_string, "--json"],
    )?;
    assert!(
        text_output.status.success(),
        "models set should succeed: {}",
        String::from_utf8_lossy(&text_output.stderr)
    );

    let embeddings_output = run_cli(
        &workdir,
        &[
            "models",
            "set-embeddings",
            "text-embedding-3-large",
            "--path",
            &config_path_string,
            "--dims",
            "3072",
            "--json",
        ],
    )?;
    assert!(
        embeddings_output.status.success(),
        "models set-embeddings should succeed: {}",
        String::from_utf8_lossy(&embeddings_output.stderr)
    );

    let status_output =
        run_cli(&workdir, &["models", "status", "--path", &config_path_string, "--json"])?;
    assert!(
        status_output.status.success(),
        "models status should succeed: {}",
        String::from_utf8_lossy(&status_output.stderr)
    );
    let status_stdout =
        String::from_utf8(status_output.stdout).context("stdout was not valid UTF-8")?;
    assert!(
        status_stdout.contains("\"provider_kind\": \"openai_compatible\""),
        "models status should report openai_compatible provider kind: {status_stdout}"
    );
    assert!(
        status_stdout.contains("\"text_model\": \"gpt-4.1-mini\""),
        "models status should report the configured text model: {status_stdout}"
    );
    assert!(
        status_stdout.contains("\"embeddings_model\": \"text-embedding-3-large\""),
        "models status should report the configured embeddings model: {status_stdout}"
    );
    assert!(
        status_stdout.contains("\"embeddings_dims\": 3072"),
        "models status should report embeddings dims: {status_stdout}"
    );

    let config_body = fs::read_to_string(&config_path)
        .with_context(|| format!("failed to read {}", config_path.display()))?;
    assert!(
        config_body.contains("kind = \"openai_compatible\""),
        "models set should persist provider kind: {config_body}"
    );
    assert!(
        config_body.contains("openai_base_url = \"https://api.openai.com/v1\""),
        "models set should persist the default OpenAI base URL: {config_body}"
    );
    assert!(
        config_body.contains("openai_model = \"gpt-4.1-mini\""),
        "models set should persist the text model: {config_body}"
    );
    assert!(
        config_body.contains("openai_embeddings_model = \"text-embedding-3-large\""),
        "models set-embeddings should persist the embeddings model: {config_body}"
    );
    assert!(
        config_body.contains("openai_embeddings_dims = 3072"),
        "models set-embeddings should persist embeddings dims: {config_body}"
    );
    Ok(())
}

#[test]
fn bare_config_command_falls_back_to_status_using_global_config_path() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("palyra.toml");
    fs::write(
        &config_path,
        "version = 1\n[model_provider]\nkind = \"openai_compatible\"\nopenai_model = \"gpt-4o-mini\"\n",
    )
    .with_context(|| format!("failed to write {}", config_path.display()))?;
    let config_path_string = config_path.to_string_lossy().into_owned();

    let output =
        run_cli(&workdir, &["--config", &config_path_string, "--output-format", "json", "config"])?;
    assert!(
        output.status.success(),
        "bare config command should fall back to status: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout was not valid UTF-8")?;
    assert!(
        stdout.contains("\"path\":"),
        "config status output should include the resolved path: {stdout}"
    );
    assert!(
        stdout.contains("\"parsed\": true"),
        "config status should confirm the config parsed successfully: {stdout}"
    );
    assert!(
        stdout.contains("\"provider_kind\": \"openai_compatible\""),
        "config status should surface the effective provider kind: {stdout}"
    );
    Ok(())
}

#[test]
fn models_list_reports_registry_providers_and_models() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("palyra.toml");
    fs::write(
        &config_path,
        r#"
version = 1
[model_provider]
kind = "openai_compatible"
default_chat_model_id = "claude-3-5-sonnet-latest"
default_embeddings_model_id = "text-embedding-3-small"
failover_enabled = true
response_cache_enabled = true

[[model_provider.providers]]
provider_id = "openai-primary"
display_name = "OpenAI"
kind = "openai_compatible"
base_url = "https://api.openai.com/v1"
auth_profile_id = "openai-default"

[[model_provider.providers]]
provider_id = "anthropic-primary"
display_name = "Anthropic"
kind = "anthropic"
base_url = "https://api.anthropic.com"
auth_profile_id = "anthropic-default"

[[model_provider.models]]
model_id = "gpt-4o-mini"
provider_id = "openai-primary"
role = "chat"
enabled = true
metadata_source = "static"
tool_calls = true
json_mode = true
vision = true
cost_tier = "standard"
latency_tier = "standard"

[[model_provider.models]]
model_id = "claude-3-5-sonnet-latest"
provider_id = "anthropic-primary"
role = "chat"
enabled = true
metadata_source = "discovery"
tool_calls = true
json_mode = true
vision = true
cost_tier = "premium"
latency_tier = "high"

[[model_provider.models]]
model_id = "text-embedding-3-small"
provider_id = "openai-primary"
role = "embeddings"
enabled = true
metadata_source = "static"
embeddings = true
cost_tier = "low"
latency_tier = "standard"
"#,
    )
    .with_context(|| format!("failed to write {}", config_path.display()))?;
    let config_path_string = config_path.to_string_lossy().into_owned();

    let output = run_cli(&workdir, &["models", "list", "--path", &config_path_string, "--json"])?;
    assert!(
        output.status.success(),
        "models list should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout was not valid UTF-8")?;
    assert!(
        stdout.contains("\"registry_provider_count\": 2"),
        "models list should report provider registry count: {stdout}"
    );
    assert!(
        stdout.contains("\"provider_id\": \"anthropic-primary\""),
        "models list should include anthropic provider entry: {stdout}"
    );
    assert!(
        stdout.contains("\"model_id\": \"claude-3-5-sonnet-latest\""),
        "models list should include registry model entries: {stdout}"
    );
    assert!(
        stdout.contains("\"registry_valid\": true"),
        "models list should report registry validation status: {stdout}"
    );
    Ok(())
}

#[test]
fn models_set_updates_registry_default_chat_model_when_registry_exists() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("palyra.toml");
    fs::write(
        &config_path,
        r#"
version = 1
[model_provider]
kind = "openai_compatible"
default_chat_model_id = "gpt-4o-mini"

[[model_provider.providers]]
provider_id = "openai-primary"
kind = "openai_compatible"

[[model_provider.providers]]
provider_id = "anthropic-primary"
kind = "anthropic"

[[model_provider.models]]
model_id = "gpt-4o-mini"
provider_id = "openai-primary"
role = "chat"
enabled = true

[[model_provider.models]]
model_id = "claude-3-5-sonnet-latest"
provider_id = "anthropic-primary"
role = "chat"
enabled = true
"#,
    )
    .with_context(|| format!("failed to write {}", config_path.display()))?;
    let config_path_string = config_path.to_string_lossy().into_owned();

    let output = run_cli(
        &workdir,
        &["models", "set", "claude-3-5-sonnet-latest", "--path", &config_path_string, "--json"],
    )?;
    assert!(
        output.status.success(),
        "models set should succeed for registry config: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let config_body = fs::read_to_string(&config_path)
        .with_context(|| format!("failed to read {}", config_path.display()))?;
    assert!(
        config_body.contains("default_chat_model_id = \"claude-3-5-sonnet-latest\""),
        "models set should update the registry default chat model: {config_body}"
    );
    assert!(
        !config_body.contains("openai_base_url = \"https://api.openai.com/v1\""),
        "registry-aware models set should not inject legacy base_url defaults into registry configs: {config_body}"
    );
    Ok(())
}

#[test]
fn models_set_rejects_chat_model_absent_from_live_discovery_cache() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let state_root = workdir.path().join("state");
    fs::create_dir_all(&state_root)
        .with_context(|| format!("failed to create {}", state_root.display()))?;
    let server = MockProviderServer::spawn(vec![MockProviderResponse {
        status_line: "200 OK",
        body: r#"{"data":[{"id":"MiniMax-M2.7"},{"id":"MiniMax-M2.5"}]}"#,
        expected_header: Some("authorization: Bearer sk-minimax-test".to_owned()),
    }])?;
    let config_path = workdir.path().join("palyra.toml");
    fs::write(
        &config_path,
        format!(
            r#"
version = 1
[model_provider]
kind = "anthropic"
auth_provider_kind = "minimax"
allow_private_base_url = true
anthropic_base_url = "{base_url}"
anthropic_model = "MiniMax-M2.7"
anthropic_api_key = "sk-minimax-test"
"#,
            base_url = server.base_url
        ),
    )
    .with_context(|| format!("failed to write {}", config_path.display()))?;
    let config_path_string = config_path.to_string_lossy().into_owned();
    let state_root_string = state_root.to_string_lossy().into_owned();

    let discover = run_cli(
        &workdir,
        &[
            "--state-root",
            &state_root_string,
            "models",
            "discover",
            "--path",
            &config_path_string,
            "--refresh",
            "--json",
        ],
    )?;
    assert!(
        discover.status.success(),
        "models discover should succeed before selection validation: {}",
        String::from_utf8_lossy(&discover.stderr)
    );

    let output = run_cli(
        &workdir,
        &[
            "--state-root",
            &state_root_string,
            "models",
            "set",
            "definitely-not-a-real-model",
            "--path",
            &config_path_string,
            "--json",
        ],
    )?;
    assert!(!output.status.success(), "models set should reject an undiscovered model id");
    assert_eq!(
        output.status.code(),
        Some(2),
        "undiscovered model selection should be classified as validation"
    );
    let stderr = String::from_utf8(output.stderr).context("stderr was not valid UTF-8")?;
    assert!(
        stderr.contains("error[validation_error]"),
        "stderr should classify the selection as validation: {stderr}"
    );
    assert!(
        stderr.contains("live-discovered provider models") && stderr.contains("--allow-custom"),
        "stderr should explain the live discovery guard and explicit override: {stderr}"
    );
    let config_body = fs::read_to_string(&config_path)
        .with_context(|| format!("failed to read {}", config_path.display()))?;
    assert!(
        config_body.contains("anthropic_model = \"MiniMax-M2.7\""),
        "rejected model selection must not mutate the config: {config_body}"
    );
    assert!(
        !config_body.contains("definitely-not-a-real-model"),
        "rejected model id must not be persisted: {config_body}"
    );
    server.finish()?;
    Ok(())
}

#[test]
fn models_set_allows_custom_chat_model_with_explicit_override() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("palyra.toml");
    fs::write(
        &config_path,
        r#"
version = 1
[model_provider]
kind = "openai_compatible"
openai_model = "gpt-4.1-mini"
"#,
    )
    .with_context(|| format!("failed to write {}", config_path.display()))?;
    let config_path_string = config_path.to_string_lossy().into_owned();

    let output = run_cli(
        &workdir,
        &[
            "models",
            "set",
            "operator-owned-custom-model",
            "--path",
            &config_path_string,
            "--allow-custom",
            "--json",
        ],
    )?;
    assert!(
        output.status.success(),
        "explicit custom model override should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let config_body = fs::read_to_string(&config_path)
        .with_context(|| format!("failed to read {}", config_path.display()))?;
    assert!(
        config_body.contains("openai_model = \"operator-owned-custom-model\""),
        "custom override should persist the requested model: {config_body}"
    );
    Ok(())
}

#[test]
fn models_list_preserves_minimax_identity_for_legacy_anthropic_configs() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("palyra.toml");
    fs::write(
        &config_path,
        r#"
version = 1
[model_provider]
kind = "anthropic"
auth_provider_kind = "minimax"
anthropic_base_url = "https://api.minimax.io/anthropic"
anthropic_model = "MiniMax-M2.7"
anthropic_api_key_vault_ref = "global/minimax_api_key"
"#,
    )
    .with_context(|| format!("failed to write {}", config_path.display()))?;
    let config_path_string = config_path.to_string_lossy().into_owned();

    let output = run_cli(&workdir, &["models", "list", "--path", &config_path_string, "--json"])?;
    assert!(
        output.status.success(),
        "models list should succeed for legacy minimax config: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout was not valid UTF-8")?;
    let payload: Value = serde_json::from_str(stdout.as_str()).context("stdout was not JSON")?;

    assert_eq!(
        payload.pointer("/providers/0/provider_id").and_then(Value::as_str),
        Some("minimax-primary"),
        "legacy minimax configs should expose a minimax provider id: {payload}"
    );
    assert_eq!(
        payload.pointer("/providers/0/display_name").and_then(Value::as_str),
        Some("MiniMax"),
        "legacy minimax configs should expose the provider display name: {payload}"
    );
    assert_eq!(
        payload.pointer("/providers/0/protocol_compatibility").and_then(Value::as_str),
        Some("anthropic_compatible"),
        "legacy minimax configs should identify the protocol compatibility layer: {payload}"
    );
    assert_eq!(
        payload.pointer("/registry_models/0/provider_id").and_then(Value::as_str),
        Some("minimax-primary"),
        "legacy minimax configs should keep registry models attached to the minimax provider: {payload}"
    );
    assert_eq!(
        payload.pointer("/registry_models/0/vision").and_then(Value::as_bool),
        Some(false),
        "legacy minimax configs should not advertise unsupported vision capability: {payload}"
    );
    let text_models = payload
        .get("models")
        .and_then(Value::as_array)
        .context("models list should include catalog entries")?;
    let minimax_entry = text_models
        .iter()
        .find(|entry| {
            entry.get("target").and_then(Value::as_str) == Some("text")
                && entry.get("id").and_then(Value::as_str) == Some("MiniMax-M2.7")
        })
        .context("configured MiniMax model should appear in models list")?;
    assert_eq!(
        minimax_entry.get("configured").and_then(Value::as_bool),
        Some(true),
        "configured MiniMax model should be marked configured: {payload}"
    );
    assert_eq!(
        minimax_entry.get("preferred").and_then(Value::as_bool),
        Some(true),
        "configured MiniMax model should be marked as the effective preferred chat model: {payload}"
    );
    let openai_entry = text_models
        .iter()
        .find(|entry| {
            entry.get("target").and_then(Value::as_str) == Some("text")
                && entry.get("id").and_then(Value::as_str) == Some("gpt-4o-mini")
        })
        .context("curated OpenAI model should remain listed")?;
    assert_eq!(
        openai_entry.get("preferred").and_then(Value::as_bool),
        Some(false),
        "unconfigured OpenAI curated model should not be marked preferred: {payload}"
    );
    Ok(())
}

#[test]
fn models_status_and_explain_distinguish_minimax_vendor_from_anthropic_protocol() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("palyra.toml");
    fs::write(
        &config_path,
        r#"
version = 1
[model_provider]
kind = "anthropic"
auth_provider_kind = "minimax"
anthropic_base_url = "https://api.minimax.io/anthropic"
anthropic_model = "MiniMax-M2.7"
anthropic_api_key_vault_ref = "global/minimax_api_key"
"#,
    )
    .with_context(|| format!("failed to write {}", config_path.display()))?;
    let config_path_string = config_path.to_string_lossy().into_owned();

    let status_output =
        run_cli(&workdir, &["models", "status", "--path", &config_path_string, "--json"])?;
    assert!(
        status_output.status.success(),
        "models status should succeed for legacy minimax config: {}",
        String::from_utf8_lossy(&status_output.stderr)
    );
    let status_stdout =
        String::from_utf8(status_output.stdout).context("stdout was not valid UTF-8")?;
    let display_name_index = status_stdout
        .find("\"provider_display_name\"")
        .context("status JSON should include provider_display_name")?;
    let provider_kind_index = status_stdout
        .find("\"provider_kind\"")
        .context("status JSON should include provider_kind")?;
    assert!(
        display_name_index < provider_kind_index,
        "status JSON should lead with the provider display name before compatibility kind: {status_stdout}"
    );
    let status_payload: Value =
        serde_json::from_str(status_stdout.as_str()).context("status stdout was not JSON")?;
    assert_eq!(
        status_payload.get("provider_kind").and_then(Value::as_str),
        Some("anthropic"),
        "status should preserve the protocol-backed provider kind for compatibility: {status_payload}"
    );
    assert_eq!(
        status_payload.get("provider_id").and_then(Value::as_str),
        Some("minimax-primary"),
        "status should expose the MiniMax provider id: {status_payload}"
    );
    assert_eq!(
        status_payload.get("provider_display_name").and_then(Value::as_str),
        Some("MiniMax"),
        "status should expose the MiniMax display name: {status_payload}"
    );
    assert_eq!(
        status_payload.get("protocol_compatibility").and_then(Value::as_str),
        Some("anthropic_compatible"),
        "status should expose the protocol compatibility layer separately: {status_payload}"
    );
    assert_eq!(
        status_payload.get("auth_provider_kind").and_then(Value::as_str),
        Some("minimax"),
        "status should expose the MiniMax auth provider selection: {status_payload}"
    );
    assert_eq!(
        status_payload.get("endpoint_base_url").and_then(Value::as_str),
        Some("https://api.minimax.io/anthropic"),
        "status should expose the effective MiniMax endpoint: {status_payload}"
    );
    assert_eq!(
        status_payload.get("default_chat_model_id").and_then(Value::as_str),
        Some("MiniMax-M2.7"),
        "status should expose the effective default chat model used by routing: {status_payload}"
    );

    let text_status_output =
        run_cli(&workdir, &["models", "status", "--path", &config_path_string])?;
    assert!(
        text_status_output.status.success(),
        "models text status should succeed for legacy minimax config: {}",
        String::from_utf8_lossy(&text_status_output.stderr)
    );
    let text_status_stdout =
        String::from_utf8(text_status_output.stdout).context("stdout was not valid UTF-8")?;
    assert!(
        text_status_stdout.contains("base_url=https://api.minimax.io/anthropic"),
        "text status should show the effective MiniMax endpoint: {text_status_stdout}"
    );
    assert!(
        text_status_stdout.contains("openai_base_url=none"),
        "text status should label the provider-specific OpenAI endpoint separately: {text_status_stdout}"
    );
    assert!(
        !text_status_stdout.contains("models.status.provider base_url=none"),
        "text status must not hide a configured MiniMax endpoint as none: {text_status_stdout}"
    );
    assert!(
        text_status_stdout.contains("default_chat_model=MiniMax-M2.7"),
        "text status should expose the effective default chat model: {text_status_stdout}"
    );
    assert!(
        !text_status_stdout.contains(" default_chat_model=none"),
        "text status must not make the effective MiniMax text model look unset: {text_status_stdout}"
    );

    let explain_output =
        run_cli(&workdir, &["models", "explain", "--path", &config_path_string, "--json"])?;
    assert!(
        explain_output.status.success(),
        "models explain should succeed for legacy minimax config: {}",
        String::from_utf8_lossy(&explain_output.stderr)
    );
    let explain_stdout =
        String::from_utf8(explain_output.stdout).context("stdout was not valid UTF-8")?;
    let explain_payload: Value =
        serde_json::from_str(explain_stdout.as_str()).context("explain stdout was not JSON")?;
    assert_eq!(
        explain_payload.pointer("/candidates/0/provider_display_name").and_then(Value::as_str),
        Some("MiniMax"),
        "explain should expose the MiniMax provider display name: {explain_payload}"
    );
    assert_eq!(
        explain_payload.pointer("/candidates/0/protocol_compatibility").and_then(Value::as_str),
        Some("anthropic_compatible"),
        "explain should expose the protocol compatibility layer separately: {explain_payload}"
    );
    assert_eq!(
        explain_payload.pointer("/candidates/0/auth_provider_kind").and_then(Value::as_str),
        Some("minimax"),
        "explain should expose the MiniMax auth provider selection: {explain_payload}"
    );
    Ok(())
}

#[test]
fn models_status_does_not_mix_deterministic_provider_with_stale_minimax_auth() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("palyra.toml");
    fs::write(
        &config_path,
        r#"
version = 1
[model_provider]
kind = "deterministic"
auth_provider_kind = "minimax"
anthropic_base_url = "https://api.minimax.io/anthropic"
anthropic_model = "MiniMax-M3"
anthropic_api_key_vault_ref = "global/minimax_api_key"
openai_model = "deterministic"
"#,
    )
    .with_context(|| format!("failed to write {}", config_path.display()))?;
    let config_path_string = config_path.to_string_lossy().into_owned();

    let status_output =
        run_cli(&workdir, &["models", "status", "--path", &config_path_string, "--json"])?;
    assert!(
        status_output.status.success(),
        "models status should succeed for deterministic config: {}",
        String::from_utf8_lossy(&status_output.stderr)
    );
    let status_stdout =
        String::from_utf8(status_output.stdout).context("stdout was not valid UTF-8")?;
    let status_payload: Value =
        serde_json::from_str(status_stdout.as_str()).context("status stdout was not JSON")?;

    assert_eq!(
        status_payload.get("provider_id").and_then(Value::as_str),
        Some("deterministic-primary"),
        "status should report the deterministic provider identity: {status_payload}"
    );
    assert_eq!(
        status_payload.get("provider_kind").and_then(Value::as_str),
        Some("deterministic"),
        "status should report deterministic provider kind: {status_payload}"
    );
    assert_eq!(
        status_payload.get("auth_provider_kind"),
        Some(&Value::Null),
        "deterministic status must not inherit stale MiniMax auth kind: {status_payload}"
    );
    assert_eq!(
        status_payload.get("endpoint_base_url"),
        Some(&Value::Null),
        "deterministic status must not inherit stale MiniMax endpoint: {status_payload}"
    );
    assert_eq!(
        status_payload.get("api_key_configured").and_then(Value::as_bool),
        Some(false),
        "deterministic status must not report stale MiniMax API-key state: {status_payload}"
    );
    assert_eq!(
        status_payload.get("default_chat_model_id").and_then(Value::as_str),
        Some("deterministic"),
        "status should keep the deterministic chat model explicit: {status_payload}"
    );
    Ok(())
}

#[test]
fn models_set_preserves_minimax_legacy_anthropic_provider() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("palyra.toml");
    fs::write(
        &config_path,
        r#"
version = 1
[model_provider]
kind = "anthropic"
auth_provider_kind = "minimax"
anthropic_base_url = "https://api.minimax.io/anthropic"
anthropic_model = "MiniMax-M2.7"
anthropic_api_key_vault_ref = "global/minimax_api_key"
openai_model = "deterministic"
"#,
    )
    .with_context(|| format!("failed to write {}", config_path.display()))?;
    let config_path_string = config_path.to_string_lossy().into_owned();

    let output = run_cli(
        &workdir,
        &["models", "set", "MiniMax-M2.7", "--path", &config_path_string, "--json"],
    )?;
    assert!(
        output.status.success(),
        "models set should preserve MiniMax legacy config: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout was not valid UTF-8")?;
    let payload: Value = serde_json::from_str(stdout.as_str()).context("stdout was not JSON")?;
    assert_eq!(
        payload.get("provider_kind").and_then(Value::as_str),
        Some("anthropic"),
        "models set should report the preserved provider kind: {payload}"
    );

    let config_body = fs::read_to_string(&config_path)
        .with_context(|| format!("failed to read {}", config_path.display()))?;
    assert!(
        config_body.contains("kind = \"anthropic\""),
        "models set must preserve the Anthropic-compatible provider kind: {config_body}"
    );
    assert!(
        config_body.contains("auth_provider_kind = \"minimax\""),
        "models set must preserve MiniMax auth provider selection: {config_body}"
    );
    assert!(
        config_body.contains("anthropic_model = \"MiniMax-M2.7\""),
        "models set should update the Anthropic-compatible model key: {config_body}"
    );
    assert!(
        !config_body.contains("openai_base_url"),
        "models set must not inject an OpenAI base URL into MiniMax configs: {config_body}"
    );
    assert!(
        !config_body.contains("openai_model"),
        "models set must clear stale OpenAI text model keys from MiniMax configs: {config_body}"
    );
    Ok(())
}

#[test]
fn models_set_embeddings_for_minimax_legacy_config_returns_validation_guidance() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("palyra.toml");
    fs::write(
        &config_path,
        r#"
version = 1
[model_provider]
kind = "anthropic"
auth_provider_kind = "minimax"
anthropic_base_url = "https://api.minimax.io/anthropic"
anthropic_model = "MiniMax-M2.7"
anthropic_api_key_vault_ref = "global/minimax_api_key"
"#,
    )
    .with_context(|| format!("failed to write {}", config_path.display()))?;
    let config_path_string = config_path.to_string_lossy().into_owned();

    let output = run_cli(
        &workdir,
        &[
            "models",
            "set-embeddings",
            "text-embedding-3-small",
            "--path",
            &config_path_string,
            "--json",
        ],
    )?;
    assert!(
        !output.status.success(),
        "models set-embeddings should reject unsupported legacy MiniMax configs"
    );
    assert_eq!(
        output.status.code(),
        Some(2),
        "unsupported embeddings setup should be classified as a validation error"
    );
    let stderr = String::from_utf8(output.stderr).context("stderr was not valid UTF-8")?;
    assert!(
        stderr.contains("error[validation_error]"),
        "stderr should classify the operator mistake as validation, not internal: {stderr}"
    );
    assert!(
        stderr.contains("model_provider.default_embeddings_model_id"),
        "stderr should point to the supported registry default field: {stderr}"
    );
    assert!(
        stderr.contains("hash fallback"),
        "stderr should name the safe degraded memory mode: {stderr}"
    );

    let config_body = fs::read_to_string(&config_path)
        .with_context(|| format!("failed to read {}", config_path.display()))?;
    assert!(
        !config_body.contains("default_embeddings_model_id"),
        "rejected embeddings update must not partially mutate the config: {config_body}"
    );
    Ok(())
}

#[test]
fn models_test_connection_discovers_live_models_with_cache() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let state_root = workdir.path().join("state");
    fs::create_dir_all(&state_root)
        .with_context(|| format!("failed to create {}", state_root.display()))?;
    let server = MockProviderServer::spawn(vec![MockProviderResponse {
        status_line: "200 OK",
        body: r#"{"data":[{"id":"gpt-4.1-mini"},{"id":"text-embedding-3-large"}]}"#,
        expected_header: Some("authorization: Bearer sk-openai-test".to_owned()),
    }])?;
    let config_path = workdir.path().join("palyra.toml");
    fs::write(
        &config_path,
        format!(
            r#"
version = 1
[model_provider]
kind = "openai_compatible"
allow_private_base_url = true
health_ttl_ms = 600000
discovery_ttl_ms = 600000

[[model_provider.providers]]
provider_id = "openai-primary"
display_name = "OpenAI"
kind = "openai_compatible"
base_url = "{base_url}"
api_key = "sk-openai-test"

[[model_provider.models]]
model_id = "gpt-4.1-mini"
provider_id = "openai-primary"
role = "chat"
enabled = true
"#,
            base_url = server.base_url
        ),
    )
    .with_context(|| format!("failed to write {}", config_path.display()))?;
    let config_path_string = config_path.to_string_lossy().into_owned();
    let state_root_string = state_root.to_string_lossy().into_owned();

    let first = run_cli(
        &workdir,
        &[
            "--state-root",
            &state_root_string,
            "models",
            "test-connection",
            "--path",
            &config_path_string,
            "--json",
        ],
    )?;
    assert!(
        first.status.success(),
        "models test-connection should succeed: {}",
        String::from_utf8_lossy(&first.stderr)
    );
    let first_stdout = String::from_utf8(first.stdout).context("stdout was not valid UTF-8")?;
    assert!(
        first_stdout.contains("\"state\": \"ok\""),
        "test-connection should report success: {first_stdout}"
    );
    assert!(
        first_stdout.contains("\"discovered_model_ids\": ["),
        "test-connection should include discovered models: {first_stdout}"
    );
    assert!(
        first_stdout.contains("\"live_discovery_verified\": true"),
        "live test-connection should mark discovery as verified: {first_stdout}"
    );
    assert!(
        first_stdout.contains("\"cache_status\": \"miss\""),
        "first live probe should miss cache: {first_stdout}"
    );

    let second = run_cli(
        &workdir,
        &[
            "--state-root",
            &state_root_string,
            "models",
            "test-connection",
            "--path",
            &config_path_string,
            "--json",
        ],
    )?;
    assert!(
        second.status.success(),
        "models discover should succeed from cache: {}",
        String::from_utf8_lossy(&second.stderr)
    );
    let second_stdout = String::from_utf8(second.stdout).context("stdout was not valid UTF-8")?;
    assert!(
        second_stdout.contains("\"cache_status\": \"hit\""),
        "discover should reuse cached provider check when TTL is fresh: {second_stdout}"
    );
    server.finish()?;
    Ok(())
}

#[test]
fn models_test_connection_redacts_provider_auth_failures() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let state_root = workdir.path().join("state");
    fs::create_dir_all(&state_root)
        .with_context(|| format!("failed to create {}", state_root.display()))?;
    let server = MockProviderServer::spawn(vec![MockProviderResponse {
        status_line: "401 Unauthorized",
        body: r#"{"error":"authorization=Bearer sk-secret-token invalid"}"#,
        expected_header: Some("authorization: Bearer sk-secret-token".to_owned()),
    }])?;
    let config_path = workdir.path().join("palyra.toml");
    fs::write(
        &config_path,
        format!(
            r#"
version = 1
[model_provider]
kind = "openai_compatible"
allow_private_base_url = true

[[model_provider.providers]]
provider_id = "openai-primary"
kind = "openai_compatible"
base_url = "{base_url}"
api_key = "sk-secret-token"
"#,
            base_url = server.base_url
        ),
    )
    .with_context(|| format!("failed to write {}", config_path.display()))?;
    let config_path_string = config_path.to_string_lossy().into_owned();
    let state_root_string = state_root.to_string_lossy().into_owned();

    let output = run_cli(
        &workdir,
        &[
            "--state-root",
            &state_root_string,
            "models",
            "test-connection",
            "--path",
            &config_path_string,
            "--refresh",
            "--json",
        ],
    )?;
    assert!(
        output.status.success(),
        "models test-connection should return a structured failure payload: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout was not valid UTF-8")?;
    assert!(
        stdout.contains("\"state\": \"auth_failed\""),
        "provider auth failures should be classified: {stdout}"
    );
    assert!(
        !stdout.contains("sk-secret-token"),
        "provider failure output must redact bearer material: {stdout}"
    );
    server.finish()?;
    Ok(())
}

#[test]
fn models_test_connection_falls_back_to_registry_when_discovery_is_unsupported() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let state_root = workdir.path().join("state");
    fs::create_dir_all(&state_root)
        .with_context(|| format!("failed to create {}", state_root.display()))?;
    let server = MockProviderServer::spawn(vec![MockProviderResponse {
        status_line: "404 Not Found",
        body: r#"{"error":"not found"}"#,
        expected_header: Some("authorization: Bearer sk-minimax-test".to_owned()),
    }])?;
    let config_path = workdir.path().join("palyra.toml");
    fs::write(
        &config_path,
        format!(
            r#"
version = 1
[model_provider]
kind = "anthropic"
auth_provider_kind = "minimax"
allow_private_base_url = true

[[model_provider.providers]]
provider_id = "minimax-primary"
kind = "anthropic"
auth_provider_kind = "minimax"
base_url = "{base_url}"
api_key = "sk-minimax-test"

[[model_provider.models]]
model_id = "MiniMax-M2.7"
provider_id = "minimax-primary"
role = "chat"
enabled = true
"#,
            base_url = server.base_url
        ),
    )
    .with_context(|| format!("failed to write {}", config_path.display()))?;
    let config_path_string = config_path.to_string_lossy().into_owned();
    let state_root_string = state_root.to_string_lossy().into_owned();

    let output = run_cli(
        &workdir,
        &[
            "--state-root",
            &state_root_string,
            "models",
            "test-connection",
            "--path",
            &config_path_string,
            "--refresh",
            "--json",
        ],
    )?;
    assert!(
        output.status.success(),
        "models test-connection should return a structured fallback payload: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout was not valid UTF-8")?;
    let payload: Value = serde_json::from_str(stdout.as_str()).context("stdout was not JSON")?;
    let provider = payload
        .pointer("/providers/0")
        .context("test-connection output should include the provider")?;

    assert_eq!(provider.get("state").and_then(Value::as_str), Some("verification_incomplete"));
    assert_eq!(provider.get("live_discovery_verified").and_then(Value::as_bool), Some(false));
    assert_eq!(provider.get("discovery_source").and_then(Value::as_str), Some("registry_fallback"));
    assert_eq!(
        provider.get("discovered_model_ids").and_then(Value::as_array).map(Vec::len),
        Some(0)
    );
    assert_eq!(
        provider
            .get("configured_model_ids")
            .and_then(Value::as_array)
            .and_then(|models| models.first())
            .and_then(Value::as_str),
        Some("MiniMax-M2.7")
    );
    assert!(
        provider.get("message").and_then(Value::as_str).is_some_and(|message| {
            message.contains("confirms endpoint and credentials")
                && message.contains("not model usability")
        }),
        "fallback message should clarify the connection-only verification scope: {payload}"
    );
    server.finish()?;
    Ok(())
}

#[test]
fn models_test_connection_accepts_empty_minimax_live_discovery() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let state_root = workdir.path().join("state");
    fs::create_dir_all(&state_root)
        .with_context(|| format!("failed to create {}", state_root.display()))?;
    let server = MockProviderServer::spawn(vec![
        MockProviderResponse {
            status_line: "200 OK",
            body: r#"{"data":[]}"#,
            expected_header: Some("authorization: Bearer sk-minimax-test".to_owned()),
        },
        MockProviderResponse {
            status_line: "200 OK",
            body: r#"{"data":[]}"#,
            expected_header: Some("authorization: Bearer sk-minimax-test".to_owned()),
        },
    ])?;
    let config_path = workdir.path().join("palyra.toml");
    fs::write(
        &config_path,
        format!(
            r#"
version = 1
[model_provider]
kind = "anthropic"
auth_provider_kind = "minimax"
allow_private_base_url = true

[[model_provider.providers]]
provider_id = "minimax-primary"
kind = "anthropic"
auth_provider_kind = "minimax"
base_url = "{base_url}"
api_key = "sk-minimax-test"

[[model_provider.models]]
model_id = "MiniMax-M2.7"
provider_id = "minimax-primary"
role = "chat"
enabled = true
"#,
            base_url = server.base_url
        ),
    )
    .with_context(|| format!("failed to write {}", config_path.display()))?;
    let config_path_string = config_path.to_string_lossy().into_owned();
    let state_root_string = state_root.to_string_lossy().into_owned();

    for command in ["test-connection", "discover"] {
        let output = run_cli(
            &workdir,
            &[
                "--state-root",
                &state_root_string,
                "models",
                command,
                "--path",
                &config_path_string,
                "--refresh",
                "--json",
            ],
        )?;
        assert!(
            output.status.success(),
            "models {command} should return a structured MiniMax payload: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout).context("stdout was not valid UTF-8")?;
        let payload: Value =
            serde_json::from_str(stdout.as_str()).context("stdout was not JSON")?;
        let provider =
            payload.pointer("/providers/0").context("models output should include the provider")?;

        assert_eq!(provider.get("state").and_then(Value::as_str), Some("ok"));
        assert_eq!(provider.get("live_discovery_verified").and_then(Value::as_bool), Some(true));
        assert_eq!(provider.get("discovery_source").and_then(Value::as_str), Some("live"));
        assert_eq!(
            provider.get("discovered_model_ids").and_then(Value::as_array).map(Vec::len),
            Some(0)
        );
        assert_eq!(
            provider
                .get("configured_model_ids")
                .and_then(Value::as_array)
                .and_then(|models| models.first())
                .and_then(Value::as_str),
            Some("MiniMax-M2.7")
        );
        assert!(
            provider.get("message").and_then(Value::as_str).is_some_and(|message| {
                message.contains("MiniMax-compatible")
                    && message.contains("configured model registry")
            }),
            "empty MiniMax discovery should explain configured registry source: {payload}"
        );
    }

    server.finish()?;
    Ok(())
}

#[test]
fn models_test_connection_accepts_minimax_auth_provider_filter_alias() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let state_root = workdir.path().join("state");
    fs::create_dir_all(&state_root)
        .with_context(|| format!("failed to create {}", state_root.display()))?;
    let server = MockProviderServer::spawn(vec![MockProviderResponse {
        status_line: "404 Not Found",
        body: r#"{"error":"not found"}"#,
        expected_header: Some("authorization: Bearer sk-minimax-test".to_owned()),
    }])?;
    let config_path = workdir.path().join("palyra.toml");
    fs::write(
        &config_path,
        format!(
            r#"
version = 1
[model_provider]
kind = "anthropic"
auth_provider_kind = "minimax"
allow_private_base_url = true
anthropic_base_url = "{base_url}"
anthropic_model = "MiniMax-M2.7"
anthropic_api_key = "sk-minimax-test"
"#,
            base_url = server.base_url
        ),
    )
    .with_context(|| format!("failed to write {}", config_path.display()))?;
    let config_path_string = config_path.to_string_lossy().into_owned();
    let state_root_string = state_root.to_string_lossy().into_owned();

    let output = run_cli(
        &workdir,
        &[
            "--state-root",
            &state_root_string,
            "models",
            "test-connection",
            "--path",
            &config_path_string,
            "--provider",
            "minimax",
            "--refresh",
            "--json",
        ],
    )?;
    assert!(
        output.status.success(),
        "models test-connection should accept auth_provider_kind filter aliases: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout was not valid UTF-8")?;
    let payload: Value = serde_json::from_str(stdout.as_str()).context("stdout was not JSON")?;
    assert_eq!(payload.get("provider_filter").and_then(Value::as_str), Some("minimax"));
    assert_eq!(
        payload.pointer("/providers/0/provider_id").and_then(Value::as_str),
        Some("minimax-primary"),
        "auth provider alias should resolve the configured MiniMax provider: {payload}"
    );
    assert_eq!(
        payload.pointer("/providers/0/state").and_then(Value::as_str),
        Some("verification_incomplete"),
        "unsupported live discovery should still return a structured provider result: {payload}"
    );

    let invalid = run_cli(
        &workdir,
        &[
            "--state-root",
            &state_root_string,
            "models",
            "test-connection",
            "--path",
            &config_path_string,
            "--provider",
            "missing-provider",
            "--json",
        ],
    )?;
    assert!(
        !invalid.status.success(),
        "unknown provider filters should fail before network probing"
    );
    assert_eq!(
        invalid.status.code(),
        Some(2),
        "unknown provider filters should be validation errors"
    );
    let stderr = String::from_utf8(invalid.stderr).context("stderr was not valid UTF-8")?;
    assert!(
        stderr.contains("error[validation_error]"),
        "unknown provider filters should not be internal errors: {stderr}"
    );
    assert!(
        stderr.contains("models list --json"),
        "unknown provider filter guidance should point to provider discovery: {stderr}"
    );
    server.finish()?;
    Ok(())
}

#[test]
fn models_explain_reports_primary_and_failover_candidates() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let config_path = workdir.path().join("palyra.toml");
    fs::write(
        &config_path,
        r#"
version = 1
[model_provider]
kind = "openai_compatible"
default_chat_model_id = "gpt-4.1-mini"
failover_enabled = true
response_cache_enabled = true

[[model_provider.providers]]
provider_id = "openai-primary"
kind = "openai_compatible"

[[model_provider.providers]]
provider_id = "anthropic-primary"
kind = "anthropic"

[[model_provider.models]]
model_id = "gpt-4.1-mini"
provider_id = "openai-primary"
role = "chat"
enabled = true
tool_calls = true
json_mode = true
vision = true
cost_tier = "standard"
latency_tier = "standard"

[[model_provider.models]]
model_id = "claude-3-5-sonnet-latest"
provider_id = "anthropic-primary"
role = "chat"
enabled = true
tool_calls = true
json_mode = true
vision = true
cost_tier = "premium"
latency_tier = "high"
"#,
    )
    .with_context(|| format!("failed to write {}", config_path.display()))?;
    let config_path_string = config_path.to_string_lossy().into_owned();

    let output =
        run_cli(&workdir, &["models", "explain", "--path", &config_path_string, "--json"])?;
    assert!(
        output.status.success(),
        "models explain should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).context("stdout was not valid UTF-8")?;
    assert!(
        stdout.contains("\"resolved_model_id\": \"gpt-4.1-mini\""),
        "models explain should keep the configured default as primary: {stdout}"
    );
    assert!(
        stdout.contains("\"provider_id\": \"anthropic-primary\""),
        "models explain should include cross-provider fallback candidates: {stdout}"
    );
    assert!(
        stdout.contains("Response cache is enabled"),
        "models explain should surface cache posture in the explanation: {stdout}"
    );
    Ok(())
}

struct MockProviderResponse {
    status_line: &'static str,
    body: &'static str,
    expected_header: Option<String>,
}

struct MockProviderServer {
    base_url: String,
    handle: Option<thread::JoinHandle<Result<()>>>,
}

impl MockProviderServer {
    fn spawn(responses: Vec<MockProviderResponse>) -> Result<Self> {
        let listener =
            TcpListener::bind("127.0.0.1:0").context("failed to bind mock provider server")?;
        let address = listener.local_addr().context("failed to read mock provider server addr")?;
        let handle = thread::spawn(move || -> Result<()> {
            for response in responses {
                let (mut stream, _) =
                    listener.accept().context("failed to accept probe request")?;
                let mut request = Vec::new();
                let mut buffer = [0_u8; 4096];
                loop {
                    let read = stream.read(&mut buffer).context("failed to read probe request")?;
                    if read == 0 {
                        break;
                    }
                    request.extend_from_slice(&buffer[..read]);
                    if request.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                let request_text = String::from_utf8_lossy(request.as_slice()).to_string();
                assert!(
                    request_text.starts_with("GET /v1/models "),
                    "unexpected request line: {request_text}"
                );
                if let Some(expected_header) = response.expected_header.as_deref() {
                    assert!(
                        request_text.contains(expected_header),
                        "expected header '{expected_header}' in request: {request_text}"
                    );
                }
                let body_bytes = response.body.as_bytes();
                let reply = format!(
                    "HTTP/1.1 {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    response.status_line,
                    body_bytes.len(),
                    response.body
                );
                stream
                    .write_all(reply.as_bytes())
                    .context("failed to write mock provider response")?;
                stream.flush().context("failed to flush mock provider response")?;
            }
            Ok(())
        });
        Ok(Self { base_url: format!("http://{}", address), handle: Some(handle) })
    }

    fn finish(mut self) -> Result<()> {
        if let Some(handle) = self.handle.take() {
            handle.join().map_err(|_| anyhow::anyhow!("mock provider thread panicked"))??;
        }
        Ok(())
    }
}
