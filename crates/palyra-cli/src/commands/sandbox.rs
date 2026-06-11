//! `palyra sandbox`: read-only list/explain views of the runtime tool policy
//! for the process-runner and Wasm runtimes.
//! The policy snapshot comes from the daemon admin status payload and goes
//! through the standard secret-redaction pass before rendering.

use crate::*;

/// Runs a `palyra sandbox` subcommand against the live runtime tool policy.
///
/// # Errors
/// Returns an error when the daemon admin surface is unreachable or its status
/// payload lacks a tool policy section.
pub(crate) fn run_sandbox(command: SandboxCommand) -> Result<()> {
    let policy = load_runtime_tool_policy_snapshot()?;
    match command {
        SandboxCommand::List { json } => emit_sandbox_list(&policy, output::preferred_json(json)),
        SandboxCommand::Explain { runtime, json } => {
            emit_sandbox_explain(&policy, runtime, output::preferred_json(json))
        }
    }
}

fn load_runtime_tool_policy_snapshot() -> Result<Value> {
    let root_context = app::current_root_context()
        .ok_or_else(|| anyhow!("CLI root context is unavailable for sandbox command"))?;
    let connection = root_context.resolve_http_connection(
        app::ConnectionOverrides::default(),
        app::ConnectionDefaults::ADMIN,
    )?;
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()
        .context("failed to build HTTP client")?;
    let payload = fetch_admin_status_payload_raw(
        &client,
        connection.base_url.as_str(),
        connection.token,
        connection.principal,
        connection.device_id,
        Some(connection.channel),
        Some(connection.trace_id),
    )?;
    sandbox_tool_policy_from_admin_status_payload(&payload)
}

fn sandbox_tool_policy_from_admin_status_payload(payload: &Value) -> Result<Value> {
    let allowed_secret_handles =
        payload.pointer("/tool_call_policy/wasm_runtime/allowed_secrets").cloned();
    let mut policy = payload
        .get("tool_call_policy")
        .cloned()
        .ok_or_else(|| anyhow!("daemon admin status did not include tool_call_policy"))?;
    redact_json_value_tree(&mut policy, None);
    restore_wasm_allowed_secret_handles(&mut policy, allowed_secret_handles);
    Ok(policy)
}

// The generic redaction pass blanks anything under a "secrets" key, but the
// Wasm allowlist holds secret *handles* (names operators must be able to
// audit), not secret values, so the pre-redaction copy is restored here; a
// unit test pins this behavior.
fn restore_wasm_allowed_secret_handles(policy: &mut Value, allowed_secret_handles: Option<Value>) {
    let Some(handles) = allowed_secret_handles.filter(is_json_string_array) else {
        return;
    };
    if let Some(target) = policy.pointer_mut("/wasm_runtime/allowed_secrets") {
        *target = handles;
    }
}

fn is_json_string_array(value: &Value) -> bool {
    value.as_array().is_some_and(|items| items.iter().all(Value::is_string))
}

fn emit_sandbox_list(policy: &Value, json_output: bool) -> Result<()> {
    let process_runner = sandbox_process_runner_view(policy);
    let wasm_runtime = sandbox_wasm_runtime_view(policy);
    let payload = json!({
        "source": "runtime",
        "allowed_tools": policy.get("allowed_tools").cloned().unwrap_or_else(|| json!([])),
        "runtimes": [process_runner, wasm_runtime],
        "recreate_supported": false,
    });

    if json_output {
        return output::print_json_pretty(&payload, "failed to encode sandbox list as JSON");
    }

    println!("sandbox.list source=runtime recreate_supported=false runtimes={}", 2);
    println!(
        "sandbox.runtime name=process_runner status={} tool_allowlisted={} tier={} executor={} egress_mode={} allow_interpreters={} allowed_executables={} allowed_egress_hosts={} allowed_dns_suffixes={}",
        process_runner
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("unknown"),
        process_runner
            .get("tool_allowlisted")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        process_runner
            .get("tier")
            .and_then(Value::as_str)
            .unwrap_or("unknown"),
        process_runner
            .get("executor")
            .and_then(Value::as_str)
            .unwrap_or("unknown"),
        process_runner
            .get("egress_enforcement_mode")
            .and_then(Value::as_str)
            .unwrap_or("unknown"),
        process_runner
            .get("allow_interpreters")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        join_json_string_list(process_runner.get("allowed_executables")),
        join_json_string_list(process_runner.get("allowed_egress_hosts")),
        join_json_string_list(process_runner.get("allowed_dns_suffixes"))
    );
    println!(
        "sandbox.runtime name=wasm_runtime status={} tool_allowlisted={} inline_modules={} allowed_http_hosts={} allowed_secrets={} allowed_storage_prefixes={} allowed_channels={}",
        wasm_runtime
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("unknown"),
        wasm_runtime
            .get("tool_allowlisted")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        wasm_runtime
            .get("allow_inline_modules")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        join_json_string_list(wasm_runtime.get("allowed_http_hosts")),
        join_json_string_list(wasm_runtime.get("allowed_secrets")),
        join_json_string_list(wasm_runtime.get("allowed_storage_prefixes")),
        join_json_string_list(wasm_runtime.get("allowed_channels"))
    );
    println!(
        "sandbox.hint value=Palyra intentionally exposes list/explain only; recreate is not implemented because the runtime does not manage disposable sandbox images."
    );
    std::io::stdout().flush().context("stdout flush failed")
}

fn emit_sandbox_explain(
    policy: &Value,
    runtime: SandboxRuntimeArg,
    json_output: bool,
) -> Result<()> {
    let process_runner = sandbox_process_runner_view(policy);
    let wasm_runtime = sandbox_wasm_runtime_view(policy);
    let payload = match runtime {
        SandboxRuntimeArg::All => json!({
            "source": "runtime",
            "process_runner": process_runner,
            "wasm_runtime": wasm_runtime,
        }),
        SandboxRuntimeArg::ProcessRunner => json!({
            "source": "runtime",
            "process_runner": process_runner,
        }),
        SandboxRuntimeArg::WasmRuntime => json!({
            "source": "runtime",
            "wasm_runtime": wasm_runtime,
        }),
    };

    if json_output {
        return output::print_json_pretty(&payload, "failed to encode sandbox explain as JSON");
    }

    match runtime {
        SandboxRuntimeArg::All | SandboxRuntimeArg::ProcessRunner => {
            println!(
                "sandbox.explain runtime=process_runner status={} tool_allowlisted={} tier={} isolation={} executor={} workspace_root={} egress_mode={} allow_interpreters={}",
                process_runner
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown"),
                process_runner
                    .get("tool_allowlisted")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                process_runner
                    .get("tier")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown"),
                process_runner
                    .get("isolation")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown"),
                process_runner
                    .get("executor")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown"),
                process_runner
                    .get("workspace_root")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown"),
                process_runner
                    .get("egress_enforcement_mode")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown"),
                process_runner
                    .get("allow_interpreters")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
            );
            println!(
                "sandbox.explain.process_runner_limits cpu_time_limit_ms={} memory_limit_bytes={} max_output_bytes={} allowed_executables={} allowed_egress_hosts={} allowed_dns_suffixes={}",
                process_runner
                    .get("cpu_time_limit_ms")
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
                process_runner
                    .get("memory_limit_bytes")
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
                process_runner
                    .get("max_output_bytes")
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
                join_json_string_list(process_runner.get("allowed_executables")),
                join_json_string_list(process_runner.get("allowed_egress_hosts")),
                join_json_string_list(process_runner.get("allowed_dns_suffixes"))
            );
            println!(
                "sandbox.explain.process_runner_hint value={}",
                process_runner
                    .get("operator_hint")
                    .and_then(Value::as_str)
                    .unwrap_or("Use `palyra policy explain` for per-request approval posture.")
            );
        }
        SandboxRuntimeArg::WasmRuntime => {}
    }

    match runtime {
        SandboxRuntimeArg::All | SandboxRuntimeArg::WasmRuntime => {
            println!(
                "sandbox.explain runtime=wasm_runtime status={} tool_allowlisted={} isolation={} inline_modules={} max_module_size_bytes={} fuel_budget={} max_memory_bytes={} max_instances={}",
                wasm_runtime
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown"),
                wasm_runtime
                    .get("tool_allowlisted")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                wasm_runtime
                    .get("isolation")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown"),
                wasm_runtime
                    .get("allow_inline_modules")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                wasm_runtime
                    .get("max_module_size_bytes")
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
                wasm_runtime
                    .get("fuel_budget")
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
                wasm_runtime
                    .get("max_memory_bytes")
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
                wasm_runtime
                    .get("max_instances")
                    .and_then(Value::as_u64)
                    .unwrap_or(0)
            );
            println!(
                "sandbox.explain.wasm_runtime_capabilities allowed_http_hosts={} allowed_secrets={} allowed_storage_prefixes={} allowed_channels={} max_table_elements={}",
                join_json_string_list(wasm_runtime.get("allowed_http_hosts")),
                join_json_string_list(wasm_runtime.get("allowed_secrets")),
                join_json_string_list(wasm_runtime.get("allowed_storage_prefixes")),
                join_json_string_list(wasm_runtime.get("allowed_channels")),
                wasm_runtime
                    .get("max_table_elements")
                    .and_then(Value::as_u64)
                    .unwrap_or(0)
            );
            println!(
                "sandbox.explain.wasm_runtime_hint value={}",
                wasm_runtime
                    .get("operator_hint")
                    .and_then(Value::as_str)
                    .unwrap_or("Use `palyra security audit` and `palyra doctor --json` when sandbox posture does not match expectations.")
            );
        }
        SandboxRuntimeArg::ProcessRunner => {}
    }

    std::io::stdout().flush().context("stdout flush failed")
}

fn sandbox_process_runner_view(policy: &Value) -> Value {
    let allowlisted = policy.get("allowed_tools").and_then(Value::as_array).is_some_and(|tools| {
        tools.iter().filter_map(Value::as_str).any(|tool| tool == "palyra.process.run")
    });
    let enabled =
        policy.pointer("/process_runner/enabled").and_then(Value::as_bool).unwrap_or(false);
    let tier = policy.pointer("/process_runner/tier").and_then(Value::as_str).unwrap_or("unknown");
    let status = sandbox_status_label(enabled, allowlisted);
    json!({
        "name": "process_runner",
        "status": status,
        "tool_allowlisted": allowlisted,
        "enabled": enabled,
        "tier": tier,
        "isolation": if tier == "c" { "tier_c_os_enforced" } else { "tier_b_in_process" },
        "executor": process_runner_executor_label(tier),
        "workspace_root": policy.pointer("/process_runner/workspace_root").and_then(Value::as_str).unwrap_or("unknown"),
        "allow_interpreters": policy.pointer("/process_runner/allow_interpreters").and_then(Value::as_bool).unwrap_or(false),
        "egress_enforcement_mode": policy
            .pointer("/process_runner/egress_enforcement_mode")
            .and_then(Value::as_str)
            .unwrap_or("unknown"),
        "allowed_executables": policy.pointer("/process_runner/allowed_executables").cloned().unwrap_or_else(|| json!([])),
        "allowed_egress_hosts": policy.pointer("/process_runner/allowed_egress_hosts").cloned().unwrap_or_else(|| json!([])),
        "allowed_dns_suffixes": policy.pointer("/process_runner/allowed_dns_suffixes").cloned().unwrap_or_else(|| json!([])),
        "cpu_time_limit_ms": policy.pointer("/process_runner/cpu_time_limit_ms").and_then(Value::as_u64).unwrap_or(0),
        "memory_limit_bytes": policy.pointer("/process_runner/memory_limit_bytes").and_then(Value::as_u64).unwrap_or(0),
        "max_output_bytes": policy.pointer("/process_runner/max_output_bytes").and_then(Value::as_u64).unwrap_or(0),
        "operator_hint": if !enabled {
            "Process execution is fail-closed by runtime policy until tool_call.process_runner.enabled=true and palyra.process.run is allowlisted."
        } else if tier == "c" {
            "Tier C delegates execution to OS-backed backends and keeps strict mode offline-only."
        } else {
            "Tier B keeps execution local. Local desktop profiles may set process_runner.allowed_executables=['*'] with egress_enforcement_mode='none' for host-wide agent execution; restrictive profiles can use explicit executable allowlists, egress controls, and allow_interpreters=false."
        },
    })
}

fn sandbox_wasm_runtime_view(policy: &Value) -> Value {
    let allowlisted = policy.get("allowed_tools").and_then(Value::as_array).is_some_and(|tools| {
        tools.iter().filter_map(Value::as_str).any(|tool| tool == "palyra.plugin.run")
    });
    let enabled = policy.pointer("/wasm_runtime/enabled").and_then(Value::as_bool).unwrap_or(false);
    json!({
        "name": "wasm_runtime",
        "status": sandbox_status_label(enabled, allowlisted),
        "tool_allowlisted": allowlisted,
        "enabled": enabled,
        "isolation": "tier_a_wasm_runtime",
        "allow_inline_modules": policy.pointer("/wasm_runtime/allow_inline_modules").and_then(Value::as_bool).unwrap_or(false),
        "max_module_size_bytes": policy.pointer("/wasm_runtime/max_module_size_bytes").and_then(Value::as_u64).unwrap_or(0),
        "fuel_budget": policy.pointer("/wasm_runtime/fuel_budget").and_then(Value::as_u64).unwrap_or(0),
        "max_memory_bytes": policy.pointer("/wasm_runtime/max_memory_bytes").and_then(Value::as_u64).unwrap_or(0),
        "max_table_elements": policy.pointer("/wasm_runtime/max_table_elements").and_then(Value::as_u64).unwrap_or(0),
        "max_instances": policy.pointer("/wasm_runtime/max_instances").and_then(Value::as_u64).unwrap_or(0),
        "allowed_http_hosts": policy.pointer("/wasm_runtime/allowed_http_hosts").cloned().unwrap_or_else(|| json!([])),
        "allowed_secrets": policy.pointer("/wasm_runtime/allowed_secrets").cloned().unwrap_or_else(|| json!([])),
        "allowed_storage_prefixes": policy.pointer("/wasm_runtime/allowed_storage_prefixes").cloned().unwrap_or_else(|| json!([])),
        "allowed_channels": policy.pointer("/wasm_runtime/allowed_channels").cloned().unwrap_or_else(|| json!([])),
        "operator_hint": if !enabled {
            "WASM execution is fail-closed until tool_call.wasm_runtime.enabled=true and palyra.plugin.run is allowlisted."
        } else {
            "WASM runs with explicit capability handles only; keep inline modules disabled in production unless the operator intentionally opts in."
        },
    })
}

fn sandbox_status_label(enabled: bool, allowlisted: bool) -> &'static str {
    if enabled && allowlisted {
        "available"
    } else if enabled {
        "not_allowlisted"
    } else {
        "disabled"
    }
}

fn process_runner_executor_label(tier: &str) -> &'static str {
    match tier {
        "c" => {
            if cfg!(target_os = "linux") {
                "sandbox_tier_c_linux_bubblewrap"
            } else if cfg!(target_os = "macos") {
                "sandbox_tier_c_macos_sandbox_exec"
            } else if cfg!(windows) {
                "sandbox_tier_c_windows_job_object"
            } else {
                "sandbox_tier_c"
            }
        }
        _ => "sandbox_tier_b",
    }
}

fn join_json_string_list(value: Option<&Value>) -> String {
    let values = value
        .and_then(Value::as_array)
        .map(|entries| {
            entries
                .iter()
                .filter_map(Value::as_str)
                .filter(|entry| !entry.trim().is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if values.is_empty() {
        "none".to_owned()
    } else {
        values.join(",")
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    use super::{
        join_json_string_list, process_runner_executor_label, sandbox_status_label,
        sandbox_tool_policy_from_admin_status_payload, sandbox_wasm_runtime_view,
    };

    #[test]
    fn sandbox_status_tracks_enablement_and_allowlist() {
        assert_eq!(sandbox_status_label(true, true), "available");
        assert_eq!(sandbox_status_label(true, false), "not_allowlisted");
        assert_eq!(sandbox_status_label(false, true), "disabled");
    }

    #[test]
    fn executor_label_tracks_selected_tier() {
        assert_eq!(process_runner_executor_label("b"), "sandbox_tier_b");
        assert!(process_runner_executor_label("c").starts_with("sandbox_tier_c_"));
    }

    #[test]
    fn sandbox_policy_preserves_wasm_allowed_secret_handles_after_redaction() {
        let payload = json!({
            "tool_call_policy": {
                "allowed_tools": ["palyra.plugin.run"],
                "admin_token": "operator-secret",
                "wasm_runtime": {
                    "enabled": true,
                    "allow_inline_modules": false,
                    "max_module_size_bytes": 1024,
                    "fuel_budget": 1000,
                    "max_memory_bytes": 4096,
                    "max_table_elements": 8,
                    "max_instances": 2,
                    "allowed_http_hosts": ["api.example.test"],
                    "allowed_secrets": ["db_password", "api_token"],
                    "allowed_storage_prefixes": ["plugin/"],
                    "allowed_channels": ["stable"]
                }
            }
        });

        let policy = sandbox_tool_policy_from_admin_status_payload(&payload)
            .expect("tool policy should be extracted");
        let wasm_runtime = sandbox_wasm_runtime_view(&policy);

        assert_eq!(policy.pointer("/admin_token").and_then(Value::as_str), Some("<redacted>"));
        assert_eq!(
            join_json_string_list(wasm_runtime.get("allowed_secrets")),
            "db_password,api_token"
        );
    }
}
