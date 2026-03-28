use palyra_control_plane as control_plane;
use serde_json::Value;

use crate::args::NodesCommand;
use crate::*;

pub(crate) fn run_nodes(command: NodesCommand) -> Result<()> {
    let runtime = build_runtime()?;
    runtime.block_on(run_nodes_async(command))
}

async fn run_nodes_async(command: NodesCommand) -> Result<()> {
    let context =
        client::control_plane::connect_admin_console(app::ConnectionOverrides::default()).await?;

    match command {
        NodesCommand::List { json, ndjson } => {
            let json = output::preferred_json(json);
            let ndjson = output::preferred_ndjson(json, ndjson);
            let envelope = context.client.list_nodes().await?;
            if json {
                output::print_json_pretty(&envelope, "failed to encode nodes list as JSON")
            } else if ndjson {
                for node in &envelope.nodes {
                    output::print_json_line(
                        &json!({ "type": "node", "node": node }),
                        "failed to encode nodes list item as NDJSON",
                    )?;
                }
                Ok(())
            } else {
                println!("nodes.list count={}", envelope.nodes.len());
                for node in &envelope.nodes {
                    println!(
                        "node device_id={} platform={} registered_at_unix_ms={} last_seen_at_unix_ms={} last_event={} capabilities={}",
                        node.device_id,
                        node.platform,
                        node.registered_at_unix_ms,
                        node.last_seen_at_unix_ms,
                        option_text(node.last_event_name.as_deref()),
                        capability_summary(&node.capabilities),
                    );
                }
                std::io::stdout().flush().context("stdout flush failed")
            }
        }
        NodesCommand::Pending { json, ndjson } => {
            let json = output::preferred_json(json);
            let ndjson = output::preferred_ndjson(json, ndjson);
            let envelope = context.client.list_pending_nodes().await?;
            if json {
                output::print_json_pretty(&envelope, "failed to encode pending nodes as JSON")
            } else if ndjson {
                for request in &envelope.requests {
                    output::print_json_line(
                        &json!({ "type": "node_pairing_request", "request": request }),
                        "failed to encode pending node item as NDJSON",
                    )?;
                }
                Ok(())
            } else {
                println!("nodes.pending count={}", envelope.requests.len());
                for request in &envelope.requests {
                    println!(
                        "nodes.pending.request request_id={} device_id={} state={} method={} approval_id={} issued_by={}",
                        request.request_id,
                        request.device_id,
                        pairing_state_text(request.state),
                        pairing_method_text(request.method),
                        request.approval_id,
                        request.code_issued_by,
                    );
                }
                std::io::stdout().flush().context("stdout flush failed")
            }
        }
        NodesCommand::Approve { request_id, reason, json } => {
            let envelope = context
                .client
                .approve_node_pairing_request(
                    request_id.as_str(),
                    &control_plane::NodePairingDecisionRequest { reason },
                )
                .await?;
            emit_node_pairing_request(
                "nodes.approve",
                &envelope.request,
                output::preferred_json(json),
            )
        }
        NodesCommand::Reject { request_id, reason, json } => {
            let envelope = context
                .client
                .reject_node_pairing_request(
                    request_id.as_str(),
                    &control_plane::NodePairingDecisionRequest { reason },
                )
                .await?;
            emit_node_pairing_request(
                "nodes.reject",
                &envelope.request,
                output::preferred_json(json),
            )
        }
        NodesCommand::Describe { device_id, json } | NodesCommand::Status { device_id, json } => {
            let envelope = context.client.get_node(device_id.as_str()).await?;
            emit_node_envelope("nodes.describe", &envelope, output::preferred_json(json))
        }
        NodesCommand::Invoke {
            device_id,
            capability,
            input_json,
            input_stdin,
            max_payload_bytes,
            json,
        } => {
            let input_json = resolve_optional_json_input(input_json, input_stdin)?;
            let envelope = context
                .client
                .invoke_node(
                    device_id.as_str(),
                    &control_plane::NodeInvokeRequest { capability, input_json, max_payload_bytes },
                )
                .await?;
            if output::preferred_json(json) {
                output::print_json_pretty(&envelope, "failed to encode node invoke output as JSON")
            } else {
                println!(
                    "nodes.invoke device_id={} capability={} success={} error={}",
                    envelope.device_id,
                    envelope.capability,
                    envelope.success,
                    option_text(Some(envelope.error.as_str())),
                );
                if let Some(output_json) = envelope.output_json.as_ref() {
                    println!(
                        "nodes.invoke.output_json={}",
                        serde_json::to_string(output_json)
                            .context("failed to encode node invoke output payload")?
                    );
                }
                std::io::stdout().flush().context("stdout flush failed")
            }
        }
    }
}

fn emit_node_envelope(
    event: &str,
    envelope: &control_plane::NodeEnvelope,
    json_output: bool,
) -> Result<()> {
    if json_output {
        return output::print_json_pretty(envelope, "failed to encode node output as JSON");
    }

    let node = &envelope.node;
    println!(
        "{event} device_id={} platform={} registered_at_unix_ms={} last_seen_at_unix_ms={} last_event={} capabilities={}",
        node.device_id,
        node.platform,
        node.registered_at_unix_ms,
        node.last_seen_at_unix_ms,
        option_text(node.last_event_name.as_deref()),
        capability_summary(&node.capabilities),
    );
    std::io::stdout().flush().context("stdout flush failed")
}

fn emit_node_pairing_request(
    event: &str,
    request: &control_plane::NodePairingRequestView,
    json_output: bool,
) -> Result<()> {
    if json_output {
        return output::print_json_pretty(request, "failed to encode node pairing request as JSON");
    }

    println!(
        "{event} request_id={} device_id={} state={} method={} approval_id={} reason={}",
        request.request_id,
        request.device_id,
        pairing_state_text(request.state),
        pairing_method_text(request.method),
        request.approval_id,
        option_text(request.decision_reason.as_deref()),
    );
    std::io::stdout().flush().context("stdout flush failed")
}

fn resolve_optional_json_input(input_json: Option<String>, input_stdin: bool) -> Result<Value> {
    if let Some(input_json) = input_json {
        return serde_json::from_str(input_json.as_str())
            .context("failed to parse --input-json as JSON");
    }
    if input_stdin {
        let mut buffer = String::new();
        std::io::stdin()
            .read_to_string(&mut buffer)
            .context("failed to read JSON input from stdin")?;
        if buffer.trim().is_empty() {
            return Ok(Value::Null);
        }
        return serde_json::from_str(buffer.as_str()).context("failed to parse stdin as JSON");
    }
    Ok(Value::Null)
}

fn capability_summary(capabilities: &[control_plane::NodeCapabilityView]) -> String {
    if capabilities.is_empty() {
        "none".to_owned()
    } else {
        capabilities
            .iter()
            .map(|capability| format!("{}:{}", capability.name, capability.available))
            .collect::<Vec<_>>()
            .join(",")
    }
}

fn option_text(value: Option<&str>) -> &str {
    value.filter(|inner| !inner.trim().is_empty()).unwrap_or("none")
}

fn pairing_method_text(method: control_plane::NodePairingMethod) -> &'static str {
    match method {
        control_plane::NodePairingMethod::Pin => "pin",
        control_plane::NodePairingMethod::Qr => "qr",
    }
}

fn pairing_state_text(state: control_plane::NodePairingRequestState) -> &'static str {
    match state {
        control_plane::NodePairingRequestState::PendingApproval => "pending_approval",
        control_plane::NodePairingRequestState::Approved => "approved",
        control_plane::NodePairingRequestState::Rejected => "rejected",
        control_plane::NodePairingRequestState::Completed => "completed",
        control_plane::NodePairingRequestState::Expired => "expired",
    }
}
