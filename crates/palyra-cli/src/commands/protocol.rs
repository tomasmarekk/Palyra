//! Protocol introspection: version constants, runtime validation summary,
//! and canonical ID validation.
//!
//! Full protocol contract validation needs a source checkout; the validate
//! output therefore includes the handoff commands for the host platform.

use crate::*;

/// Dispatches a `palyra protocol` subcommand.
///
/// # Errors
/// Fails when a canonical ID is invalid, runtime method introspection fails,
/// or output encoding fails.
pub(crate) fn run_protocol(command: ProtocolCommand) -> Result<()> {
    match command {
        ProtocolCommand::Version { json } => {
            if output::preferred_json(json) {
                return output::print_json_pretty(
                    &json!({
                        "protocol_major": CANONICAL_PROTOCOL_MAJOR,
                        "json_envelope_version": CANONICAL_JSON_ENVELOPE_VERSION,
                    }),
                    "failed to encode protocol version output as JSON",
                );
            }
            if output::preferred_ndjson(json, false) {
                output::print_json_line(
                    &json!({
                        "protocol_major": CANONICAL_PROTOCOL_MAJOR,
                        "json_envelope_version": CANONICAL_JSON_ENVELOPE_VERSION,
                    }),
                    "failed to encode protocol version output as NDJSON",
                )?;
                return std::io::stdout().flush().context("stdout flush failed");
            }
            println!(
                "protocol.major={} json.envelope.v={}",
                CANONICAL_PROTOCOL_MAJOR, CANONICAL_JSON_ENVELOPE_VERSION
            );
            std::io::stdout().flush().context("stdout flush failed")
        }
        ProtocolCommand::Validate { json } => {
            let payload = protocol_validate_output_value();
            if output::preferred_json(json) {
                return output::print_json_pretty(
                    &payload,
                    "failed to encode protocol validation output as JSON",
                );
            }
            if output::preferred_ndjson(json, false) {
                output::print_json_line(
                    &payload,
                    "failed to encode protocol validation output as NDJSON",
                )?;
                return std::io::stdout().flush().context("stdout flush failed");
            }
            println!(
                "protocol.validate status=ok protocol_major={} json.envelope.v={}",
                CANONICAL_PROTOCOL_MAJOR, CANONICAL_JSON_ENVELOPE_VERSION
            );
            println!(
                "protocol.validate.handoff source_checkout_commands=\"{}\"",
                protocol_validate_handoff_commands().join("; ")
            );
            std::io::stdout().flush().context("stdout flush failed")
        }
        ProtocolCommand::Methods { json } => {
            let payload = load_runtime_method_registry_snapshot()?;
            if output::preferred_json(json) {
                return output::print_json_pretty(
                    &payload,
                    "failed to encode protocol methods output as JSON",
                );
            }
            if output::preferred_ndjson(json, false) {
                output::print_json_line(
                    &payload,
                    "failed to encode protocol methods output as NDJSON",
                )?;
                return std::io::stdout().flush().context("stdout flush failed");
            }
            print_protocol_methods_summary(&payload)
        }
        ProtocolCommand::ValidateId { id, json } => {
            validate_canonical_id(&id).with_context(|| format!("invalid canonical ID: {}", id))?;
            if output::preferred_json(json) {
                return output::print_json_pretty(
                    &json!({
                        "valid": true,
                        "id": id,
                    }),
                    "failed to encode canonical ID validation output as JSON",
                );
            }
            if output::preferred_ndjson(json, false) {
                output::print_json_line(
                    &json!({
                        "valid": true,
                        "id": id,
                    }),
                    "failed to encode canonical ID validation output as NDJSON",
                )?;
                return std::io::stdout().flush().context("stdout flush failed");
            }
            println!("canonical_id.valid=true id={id}");
            std::io::stdout().flush().context("stdout flush failed")
        }
    }
}

fn load_runtime_method_registry_snapshot() -> Result<Value> {
    let root_context = app::current_root_context()
        .ok_or_else(|| anyhow!("CLI root context is unavailable for protocol methods command"))?;
    let connection = root_context.resolve_http_connection(
        app::ConnectionOverrides::default(),
        app::ConnectionDefaults::ADMIN,
    )?;
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()
        .context("failed to build HTTP client")?;
    let mut payload = fetch_admin_json_payload_raw(
        &client,
        AdminJsonFetchRequest {
            base_url: connection.base_url.as_str(),
            path: "admin/v1/methods",
            token: connection.token,
            principal: connection.principal,
            device_id: connection.device_id,
            channel: Some(connection.channel),
            trace_id: Some(connection.trace_id),
        },
    )?;
    redact_json_value_tree(&mut payload, None);
    Ok(payload)
}

fn print_protocol_methods_summary(payload: &Value) -> Result<()> {
    let methods = payload
        .get("methods")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("method registry payload did not include methods"))?;
    let scopes = payload
        .get("scopes")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("method registry payload did not include scopes"))?;
    let schema_version = payload.get("schema_version").and_then(Value::as_u64).unwrap_or(0);
    let registry_version =
        payload.get("registry_version").and_then(Value::as_str).unwrap_or("unknown");
    println!(
        "protocol.methods schema_version={} registry_version={} methods={} scopes={}",
        schema_version,
        registry_version,
        methods.len(),
        scopes.len()
    );

    let mut surfaces = BTreeMap::<String, usize>::new();
    for method in methods {
        let surface = method.get("surface").and_then(Value::as_str).unwrap_or("unknown");
        *surfaces.entry(surface.to_owned()).or_default() += 1;
    }
    for (surface, count) in surfaces {
        println!("protocol.methods.surface name={surface} count={count}");
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn protocol_validate_output_value() -> serde_json::Value {
    json!({
        "valid": true,
        "protocol_major": CANONICAL_PROTOCOL_MAJOR,
        "json_envelope_version": CANONICAL_JSON_ENVELOPE_VERSION,
        "checks": [
            {
                "name": "runtime_protocol_constants",
                "status": "ok",
                "protocol_major": CANONICAL_PROTOCOL_MAJOR,
                "json_envelope_version": CANONICAL_JSON_ENVELOPE_VERSION,
            }
        ],
        "source_checkout_handoff": {
            "available": true,
            "commands": protocol_validate_handoff_commands(),
        }
    })
}

fn protocol_validate_handoff_commands() -> Vec<&'static str> {
    if cfg!(windows) {
        vec![
            "pwsh scripts/protocol/validate-proto.ps1",
            "pwsh scripts/protocol/check-generated-stubs.ps1",
        ]
    } else {
        vec![
            "bash scripts/protocol/validate-proto.sh",
            "bash scripts/protocol/check-generated-stubs.sh",
        ]
    }
}
