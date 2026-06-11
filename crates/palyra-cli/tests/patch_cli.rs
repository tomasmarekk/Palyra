//! Pins `palyra patch apply` stdin handling: a single validation payload on parse errors,
//! workspace-root resolution independent of the shell cwd, and tolerance for Windows
//! pipeline control-line whitespace.

use std::io::Write;
use std::path::Path;
use std::process::{Command, Output, Stdio};

use anyhow::{Context, Result};
use serde_json::Value;
use tempfile::TempDir;

fn run_cli_with_stdin(workdir: &TempDir, args: &[&str], stdin_payload: &[u8]) -> Result<Output> {
    run_cli_with_stdin_and_env(workdir, args, stdin_payload, &[])
}

fn run_cli_with_stdin_and_env(
    workdir: &TempDir,
    args: &[&str],
    stdin_payload: &[u8],
    extra_env: &[(&str, &Path)],
) -> Result<Output> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_palyra"));
    command
        .current_dir(workdir.path())
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env("XDG_CONFIG_HOME", workdir.path().join("xdg-config"))
        .env("XDG_STATE_HOME", workdir.path().join("xdg-state"))
        .env("HOME", workdir.path().join("home"))
        .env("LOCALAPPDATA", workdir.path().join("localappdata"))
        .env("APPDATA", workdir.path().join("appdata"));
    for (key, value) in extra_env {
        command.env(key, value);
    }
    let mut child =
        command.spawn().with_context(|| format!("failed to spawn palyra {}", args.join(" ")))?;
    let stdin = child.stdin.as_mut().context("palyra command stdin was not available")?;
    stdin.write_all(stdin_payload).context("failed to write stdin payload")?;
    child
        .wait_with_output()
        .with_context(|| format!("failed to wait for palyra {}", args.join(" ")))
}

#[test]
fn patch_apply_json_parse_error_emits_single_validation_payload() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let output = run_cli_with_stdin(
        &workdir,
        &["patch", "apply", "--stdin", "--dry-run", "--json"],
        b"not a patch",
    )?;

    assert_eq!(
        output.status.code(),
        Some(2),
        "malformed patches should fail with validation exit code; stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "JSON patch failures should not emit an additional text error: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let payload: Value =
        serde_json::from_slice(&output.stdout).context("patch failure stdout was not JSON")?;
    assert_eq!(payload.get("success").and_then(Value::as_bool), Some(false));
    assert_eq!(payload.get("error_kind").and_then(Value::as_str), Some("validation_error"));
    assert_eq!(payload.get("exit_code").and_then(Value::as_u64), Some(2));
    assert_eq!(payload.pointer("/parse_error/line").and_then(Value::as_u64), Some(1));
    assert_eq!(payload.pointer("/parse_error/column").and_then(Value::as_u64), Some(1));
    assert!(
        payload
            .get("error")
            .and_then(Value::as_str)
            .is_some_and(|value| { value.contains("patch parse error") }),
        "payload should include the parser failure: {payload}"
    );
    Ok(())
}

#[test]
fn patch_apply_uses_configured_workspace_root_not_shell_cwd() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary shell cwd")?;
    let state_root = workdir.path().join("state");
    let config_path = state_root.join("config").join("palyra.toml");
    let workspace_root = state_root.join("workspace");
    std::fs::create_dir_all(config_path.parent().context("config parent missing")?)?;
    std::fs::create_dir_all(workspace_root.as_path())?;
    std::fs::write(
        config_path.as_path(),
        r#"
[tool_call.process_runner]
workspace_root = "workspace"
"#,
    )?;

    let output = run_cli_with_stdin_and_env(
        &workdir,
        &["patch", "apply", "--stdin", "--json"],
        b"*** Begin Patch\n*** Add File: e2e-cli/patch-test.txt\n+Palyra E2E patch smoke\n*** End Patch\n",
        &[("PALYRA_CONFIG", config_path.as_path()), ("PALYRA_STATE_ROOT", state_root.as_path())],
    )?;

    assert!(
        output.status.success(),
        "patch apply should succeed; stdout={}; stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let payload: Value =
        serde_json::from_slice(&output.stdout).context("patch success stdout was not JSON")?;
    assert_eq!(
        payload.pointer("/files_touched/0/path").and_then(Value::as_str),
        Some("e2e-cli/patch-test.txt")
    );
    let workspace_root_text = workspace_root.to_string_lossy();
    assert!(
        payload.get("workspace_roots").and_then(Value::as_array).is_some_and(|roots| roots
            .iter()
            .any(|root| {
                root.as_str().is_some_and(|root| root == workspace_root_text.as_ref())
            })),
        "payload should expose the configured workspace root: {payload}"
    );
    assert!(
        workspace_root.join("e2e-cli").join("patch-test.txt").is_file(),
        "patch output should be under configured workspace root"
    );
    assert!(
        !workdir.path().join("e2e-cli").join("patch-test.txt").exists(),
        "patch output must not be written under shell cwd"
    );
    Ok(())
}

#[test]
fn patch_apply_accepts_windows_pipeline_control_line_whitespace() -> Result<()> {
    let workdir = TempDir::new().context("failed to create temporary workdir")?;
    let workspace_arg = workdir.path().to_string_lossy().into_owned();
    let output = run_cli_with_stdin(
        &workdir,
        &["patch", "apply", "--workspace-root", workspace_arg.as_str(), "--stdin", "--json"],
        b"*** Begin Patch \r\n*** Add File: cli-patch-e2e.txt \r\n+palyra cli patch ok\r\n*** End Patch \r\n",
    )?;

    assert!(
        output.status.success(),
        "Windows-style piped patch should succeed; stdout={}; stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let payload: Value =
        serde_json::from_slice(&output.stdout).context("patch success stdout was not JSON")?;
    assert_eq!(
        payload.pointer("/files_touched/0/path").and_then(Value::as_str),
        Some("cli-patch-e2e.txt")
    );
    assert_eq!(
        std::fs::read_to_string(workdir.path().join("cli-patch-e2e.txt"))
            .context("created file should read")?,
        "palyra cli patch ok\n"
    );
    Ok(())
}
