use palyra_control_plane as control_plane;
use palyra_identity::{IdentityManager, DEFAULT_CERT_VALIDITY};

use crate::args::PairingStateArg;
use crate::*;

pub(crate) fn run_pairing(command: PairingCommand) -> Result<()> {
    let runtime = build_runtime()?;
    runtime.block_on(run_pairing_async(command))
}

async fn run_pairing_async(command: PairingCommand) -> Result<()> {
    match command {
        PairingCommand::List { client_kind, state, json, ndjson } => {
            let context =
                client::control_plane::connect_admin_console(app::ConnectionOverrides::default())
                    .await?;
            let json = output::preferred_json(json);
            let ndjson = output::preferred_ndjson(json, ndjson);
            let envelope = context
                .client
                .list_node_pairing_requests(Some(&control_plane::NodePairingListQuery {
                    client_kind,
                    state: state.map(pairing_state_arg_to_model),
                }))
                .await?;
            if json {
                output::print_json_pretty(&envelope, "failed to encode pairing list as JSON")
            } else if ndjson {
                for code in &envelope.codes {
                    output::print_json_line(
                        &json!({ "type": "pairing_code", "code": code }),
                        "failed to encode pairing code as NDJSON",
                    )?;
                }
                for request in &envelope.requests {
                    output::print_json_line(
                        &json!({ "type": "pairing_request", "request": request }),
                        "failed to encode pairing request as NDJSON",
                    )?;
                }
                Ok(())
            } else {
                println!(
                    "pairing.list requests={} codes={}",
                    envelope.requests.len(),
                    envelope.codes.len()
                );
                for code in &envelope.codes {
                    println!(
                        "pairing.code code={} method={} issued_by={} expires_at_unix_ms={}",
                        code.code,
                        pairing_method_text(code.method),
                        code.issued_by,
                        code.expires_at_unix_ms,
                    );
                }
                for request in &envelope.requests {
                    println!(
                        "pairing.request request_id={} device_id={} client_kind={} state={} method={} approval_id={}",
                        request.request_id,
                        request.device_id,
                        request.client_kind,
                        pairing_state_text(request.state),
                        pairing_method_text(request.method),
                        request.approval_id,
                    );
                }
                std::io::stdout().flush().context("stdout flush failed")
            }
        }
        PairingCommand::Code { method, issued_by, ttl_ms, json } => {
            let context =
                client::control_plane::connect_admin_console(app::ConnectionOverrides::default())
                    .await?;
            let envelope = context
                .client
                .mint_node_pairing_code(&control_plane::NodePairingCodeMintRequest {
                    method: pairing_method_arg_to_model(method),
                    issued_by,
                    ttl_ms,
                })
                .await?;
            if output::preferred_json(json) {
                output::print_json_pretty(&envelope, "failed to encode pairing code as JSON")
            } else {
                println!(
                    "pairing.code code={} method={} issued_by={} expires_at_unix_ms={}",
                    envelope.code.code,
                    pairing_method_text(envelope.code.method),
                    envelope.code.issued_by,
                    envelope.code.expires_at_unix_ms,
                );
                std::io::stdout().flush().context("stdout flush failed")
            }
        }
        PairingCommand::Approve { request_id, reason, json } => {
            let context =
                client::control_plane::connect_admin_console(app::ConnectionOverrides::default())
                    .await?;
            let envelope = context
                .client
                .approve_node_pairing_request(
                    request_id.as_str(),
                    &control_plane::NodePairingDecisionRequest { reason },
                )
                .await?;
            emit_pairing_request("pairing.approve", &envelope.request, output::preferred_json(json))
        }
        PairingCommand::Reject { request_id, reason, json } => {
            let context =
                client::control_plane::connect_admin_console(app::ConnectionOverrides::default())
                    .await?;
            let envelope = context
                .client
                .reject_node_pairing_request(
                    request_id.as_str(),
                    &control_plane::NodePairingDecisionRequest { reason },
                )
                .await?;
            emit_pairing_request("pairing.reject", &envelope.request, output::preferred_json(json))
        }
        PairingCommand::Pair {
            device_id,
            client_kind,
            method,
            proof,
            proof_stdin,
            allow_insecure_proof_arg,
            store_dir,
            approve,
            simulate_rotation,
        } => run_legacy_pairing(LegacyPairingArgs {
            device_id,
            client_kind,
            method,
            proof,
            proof_stdin,
            allow_insecure_proof_arg,
            store_dir,
            approve,
            simulate_rotation,
        }),
    }
}

struct LegacyPairingArgs {
    device_id: String,
    client_kind: PairingClientKindArg,
    method: PairingMethodArg,
    proof: Option<String>,
    proof_stdin: bool,
    allow_insecure_proof_arg: bool,
    store_dir: Option<String>,
    approve: bool,
    simulate_rotation: bool,
}

fn run_legacy_pairing(args: LegacyPairingArgs) -> Result<()> {
    if !args.approve {
        anyhow::bail!(
            "decision=deny_by_default approval_required=true reason=pairing requires explicit --approve"
        );
    }

    let store_root = resolve_identity_store_root(args.store_dir)?;
    let store = build_identity_store(&store_root)?;
    let mut manager = IdentityManager::with_store(store.clone())
        .context("failed to initialize identity manager")?;
    let proof = resolve_pairing_proof(args.proof, args.proof_stdin, args.allow_insecure_proof_arg)?;
    let pairing_method = build_pairing_method(args.method, &proof);

    let started_at = SystemTime::now();
    let session = manager
        .start_pairing(to_identity_client_kind(args.client_kind), pairing_method, started_at)
        .context("failed to start pairing session")?;
    let device =
        DeviceIdentity::generate(&args.device_id).context("failed to generate device identity")?;

    let hello = manager
        .build_device_hello(&session, &device, &proof)
        .context("failed to build device pairing hello")?;
    let completed_at = SystemTime::now();
    let result = manager
        .complete_pairing(hello, completed_at)
        .context("failed to complete pairing handshake")?;
    if let Err(store_error) = device.store(store.as_ref()) {
        let rollback = manager.revoke_device(
            &args.device_id,
            "device identity persistence failed after pairing",
            SystemTime::now(),
        );
        if let Err(rollback_error) = rollback {
            let _ = (store_error, rollback_error);
            anyhow::bail!(
                "failed to persist device identity after pairing; rollback revoke also failed"
            );
        }
        let _ = store_error;
        anyhow::bail!("failed to persist device identity after pairing; pairing was rolled back");
    }

    println!(
        "pairing.status=paired device_id={} client_kind={} method={} store_root={}",
        result.device.device_id,
        result.device.client_kind.as_str(),
        args.method.as_str(),
        store_root.display(),
    );

    if args.simulate_rotation {
        manager
            .rotate_device_certificate_if_due(
                &args.device_id,
                SystemTime::now() + DEFAULT_CERT_VALIDITY,
            )
            .context("failed to rotate certificate in simulation mode")?;
        println!("pairing.rotation=simulated rotated=true");
    }

    std::io::stdout().flush().context("stdout flush failed")
}

fn emit_pairing_request(
    event: &str,
    request: &control_plane::NodePairingRequestView,
    json_output: bool,
) -> Result<()> {
    if json_output {
        return output::print_json_pretty(request, "failed to encode pairing request as JSON");
    }

    println!(
        "{event} request_id={} device_id={} client_kind={} state={} method={} approval_id={} reason={}",
        request.request_id,
        request.device_id,
        request.client_kind,
        pairing_state_text(request.state),
        pairing_method_text(request.method),
        request.approval_id,
        option_text(request.decision_reason.as_deref()),
    );
    std::io::stdout().flush().context("stdout flush failed")
}

fn pairing_method_arg_to_model(method: PairingMethodArg) -> control_plane::NodePairingMethod {
    match method {
        PairingMethodArg::Pin => control_plane::NodePairingMethod::Pin,
        PairingMethodArg::Qr => control_plane::NodePairingMethod::Qr,
    }
}

fn pairing_state_arg_to_model(state: PairingStateArg) -> control_plane::NodePairingRequestState {
    match state {
        PairingStateArg::PendingApproval => control_plane::NodePairingRequestState::PendingApproval,
        PairingStateArg::Approved => control_plane::NodePairingRequestState::Approved,
        PairingStateArg::Rejected => control_plane::NodePairingRequestState::Rejected,
        PairingStateArg::Completed => control_plane::NodePairingRequestState::Completed,
        PairingStateArg::Expired => control_plane::NodePairingRequestState::Expired,
    }
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

fn option_text(value: Option<&str>) -> &str {
    value.filter(|inner| !inner.trim().is_empty()).unwrap_or("none")
}
