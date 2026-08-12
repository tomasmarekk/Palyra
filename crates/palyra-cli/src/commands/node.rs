//! Node host lifecycle commands: pairing bootstrap, install/start/stop/uninstall,
//! and the foreground capability loop that serves gateway dispatches over mTLS.
//! Local node-host state (config, process metadata, identity store) lives under
//! the CLI state root in the `node-host` directory.

use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{Read, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[cfg(windows)]
use std::os::windows::process::CommandExt;

use anyhow::{anyhow, Context, Result};
use palyra_identity::{
    build_device_pairing_hello, DeviceIdentity, PairingClientKind, PairingSession,
};
use palyra_workerd::{
    remote_protocol::verify_authenticated_delivery_hmac_sha256,
    transport_adapters::CanonicalWorkerStdioAdapter, WorkerRemoteToolRequestEnvelope,
    WORKER_REMOTE_TOOL_PROTOCOL,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity},
    Request,
};
use ulid::Ulid;

use crate::args::NodeCommand;
use crate::proto::palyra::{common::v1 as common_v1, node::v1 as node_v1};
use crate::*;

const NODE_HOST_CONFIG_SCHEMA_VERSION: u32 = 1;
const NODE_HOST_PROCESS_SCHEMA_VERSION: u32 = 1;
const NODE_HOST_STATE_DIR: &str = "node-host";
const NODE_HOST_CONFIG_FILE_NAME: &str = "node-host.json";
const NODE_HOST_PROCESS_FILE_NAME: &str = "node-host-process.json";
const NODE_HOST_STDOUT_LOG_FILE_NAME: &str = "node-host.stdout.log";
const NODE_HOST_STDERR_LOG_FILE_NAME: &str = "node-host.stderr.log";
const NODE_HOST_WORKER_REPLAY_FILE_NAME: &str = "networked-worker-replay.v1.json";
const NODE_HOST_CERTIFICATE_SECRET_KEY_SUFFIX: &str = "node-mtls-client.json";
const DEFAULT_NODE_POLL_INTERVAL_MS: u64 = 1_000;
const NODE_HOST_START_POLL_MS: u64 = 750;
const NODE_HOST_MAX_RECONNECT_ATTEMPTS: u32 = 8;
const NODE_HOST_RECONNECT_STABLE_MS: u64 = 60_000;
const NODE_WORKER_REPLAY_SCHEMA_VERSION: u32 = 1;
const NODE_WORKER_REPLAY_MAX_RECORDS: usize = 2_048;
const NODE_WORKER_CHILD_MAX_TIMEOUT_MS: u64 = 120_000;
const NETWORKED_WORKER_DELIVERY_FENCE_CAPABILITY: &str =
    "protocol:palyra.networked_worker.delivery_fence.v2";

#[cfg(windows)]
const DETACHED_PROCESS: u32 = 0x0000_0008;
#[cfg(windows)]
const CREATE_NO_WINDOW: u32 = 0x0800_0000;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NodeHostConfig {
    schema_version: u32,
    grpc_url: String,
    device_id: String,
    poll_interval_ms: u64,
    identity_store_dir: String,
    installed_at_unix_ms: u64,
    paired_at_unix_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NodeHostProcessMetadata {
    schema_version: u32,
    pid: u32,
    stdout_log_path: String,
    stderr_log_path: String,
    started_at_unix_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredNodeClientCertificate {
    certificate_pem: String,
    private_key_pem: String,
    cert_expires_at_unix_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NodeWorkerReplayRegistry {
    schema_version: u32,
    records: BTreeMap<String, NodeWorkerReplayRecord>,
}

impl Default for NodeWorkerReplayRegistry {
    fn default() -> Self {
        Self { schema_version: NODE_WORKER_REPLAY_SCHEMA_VERSION, records: BTreeMap::new() }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NodeWorkerReplayRecord {
    request_id: String,
    state: NodeWorkerReplayState,
    updated_at_unix_ms: u64,
    #[serde(default)]
    reconcile_after_unix_ms: u64,
    result_sha256: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum NodeWorkerReplayState {
    InFlight,
    OutcomeUnknown,
    Settled,
}

#[derive(Debug, Clone, Serialize)]
struct NodeLifecyclePayload {
    action: String,
    installed: bool,
    paired: bool,
    running: bool,
    device_id: Option<String>,
    grpc_url: Option<String>,
    identity_store_dir: Option<String>,
    cert_expires_at_unix_ms: Option<u64>,
    pid: Option<u32>,
    stdout_log_path: Option<String>,
    stderr_log_path: Option<String>,
    detail: String,
}

#[derive(Debug, Clone, Serialize)]
struct NodeRunPayload {
    action: &'static str,
    device_id: String,
    grpc_url: String,
    poll_interval_ms: u64,
    paired: bool,
    capability_count: usize,
}

#[derive(Debug, Clone)]
struct NodeClientMaterial {
    gateway_ca_certificate_pem: String,
    certificate: StoredNodeClientCertificate,
}

#[derive(Debug, Clone)]
struct LocalCapabilityResult {
    success: bool,
    output_json: Value,
    error: String,
}

#[derive(Debug, Clone, Copy)]
struct NodeCapabilityDescriptor {
    name: &'static str,
    requires_local_mediation: bool,
}

const NODE_CAPABILITY_DESCRIPTORS: [NodeCapabilityDescriptor; 10] = [
    NodeCapabilityDescriptor { name: "echo", requires_local_mediation: false },
    NodeCapabilityDescriptor { name: "system.health", requires_local_mediation: false },
    NodeCapabilityDescriptor { name: "system.identity", requires_local_mediation: false },
    NodeCapabilityDescriptor { name: "desktop.open_url", requires_local_mediation: true },
    NodeCapabilityDescriptor { name: "desktop.open_path", requires_local_mediation: true },
    NodeCapabilityDescriptor {
        name: NETWORKED_WORKER_DELIVERY_FENCE_CAPABILITY,
        requires_local_mediation: false,
    },
    NodeCapabilityDescriptor { name: "tool:palyra.fs.read_file", requires_local_mediation: false },
    NodeCapabilityDescriptor { name: "tool:palyra.fs.list_dir", requires_local_mediation: false },
    NodeCapabilityDescriptor { name: "tool:palyra.fs.search", requires_local_mediation: false },
    NodeCapabilityDescriptor {
        name: "tool:palyra.fs.apply_patch",
        requires_local_mediation: false,
    },
];

/// Runs a `palyra node` subcommand on a dedicated Tokio runtime.
///
/// # Errors
/// Returns an error when pairing, gateway connectivity, or local node-host
/// state operations fail.
pub(crate) fn run_node(command: NodeCommand) -> Result<()> {
    let runtime = build_runtime()?;
    runtime.block_on(run_node_async(command))
}

async fn run_node_async(command: NodeCommand) -> Result<()> {
    match command {
        NodeCommand::Run {
            grpc_url,
            gateway_ca_file,
            device_id,
            method,
            pairing_code,
            pairing_code_stdin,
            allow_insecure_pairing_code_arg,
            poll_interval_ms,
            json,
        } => {
            let mut config = resolve_node_host_config(grpc_url, device_id, poll_interval_ms)?;
            ensure_node_pairing_material(
                &mut config,
                method,
                pairing_code,
                pairing_code_stdin,
                allow_insecure_pairing_code_arg,
                gateway_ca_file,
            )
            .await?;
            write_node_host_config(&config)?;
            run_node_foreground(&config, output::preferred_json(json)).await
        }
        NodeCommand::Status { json } => emit_node_lifecycle_payload(
            build_node_status_payload("status", "node host status snapshot")?,
            output::preferred_json(json),
        ),
        NodeCommand::Install {
            grpc_url,
            gateway_ca_file,
            device_id,
            method,
            pairing_code,
            pairing_code_stdin,
            allow_insecure_pairing_code_arg,
            start,
            json,
        } => {
            let mut config =
                resolve_node_host_config(grpc_url, device_id, Some(DEFAULT_NODE_POLL_INTERVAL_MS))?;
            ensure_node_pairing_material(
                &mut config,
                method,
                pairing_code,
                pairing_code_stdin,
                allow_insecure_pairing_code_arg,
                gateway_ca_file,
            )
            .await?;
            write_node_host_config(&config)?;
            if start {
                run_node_start(output::preferred_json(json))
            } else {
                emit_node_lifecycle_payload(
                    build_node_status_payload("install", "node host configuration installed")?,
                    output::preferred_json(json),
                )
            }
        }
        NodeCommand::Start { json } => run_node_start(output::preferred_json(json)),
        NodeCommand::Stop { json } => run_node_stop(output::preferred_json(json)),
        NodeCommand::Restart { json } => {
            let json_output = output::preferred_json(json);
            run_node_stop(json_output)?;
            run_node_start(json_output)
        }
        NodeCommand::Uninstall { json } => run_node_uninstall(output::preferred_json(json)),
    }
}

async fn run_node_foreground(config: &NodeHostConfig, json_output: bool) -> Result<()> {
    let store = build_identity_store(Path::new(config.identity_store_dir.as_str()))?;
    let device = DeviceIdentity::load(store.as_ref(), config.device_id.as_str())
        .map_err(anyhow::Error::from)
        .with_context(|| format!("failed to load node device identity {}", config.device_id))?;
    let mut reconnect_attempt = 0_u32;
    let mut emit_started_payload = true;
    loop {
        let connection_started = std::time::Instant::now();
        let mut connection_established = false;
        match run_node_connection(
            config,
            &device,
            emit_started_payload,
            json_output,
            &mut connection_established,
        )
        .await
        {
            Ok(()) => return Ok(()),
            Err(error) => {
                reconnect_attempt = next_reconnect_attempt(
                    reconnect_attempt,
                    connection_established,
                    connection_started.elapsed(),
                );
                if reconnect_attempt > NODE_HOST_MAX_RECONNECT_ATTEMPTS {
                    return Err(error).context(format!(
                        "node host exhausted {NODE_HOST_MAX_RECONNECT_ATTEMPTS} reconnect attempts"
                    ));
                }
                emit_started_payload = false;
                let exponent = reconnect_attempt.saturating_sub(1).min(4);
                let backoff_ms = 250_u64.saturating_mul(1_u64 << exponent);
                sleep(Duration::from_millis(backoff_ms)).await;
            }
        }
    }
}

async fn run_node_connection(
    config: &NodeHostConfig,
    device: &DeviceIdentity,
    emit_started_payload: bool,
    json_output: bool,
    connection_established: &mut bool,
) -> Result<()> {
    // Certificate material is reloaded for every connection so an operator
    // rotation takes effect without restarting the durable node host.
    let client_material = load_node_client_material(config)?;
    let mut client = connect_node_service(
        config.grpc_url.as_str(),
        client_material.gateway_ca_certificate_pem.as_str(),
        Some(&client_material.certificate),
    )
    .await?;
    let capabilities = supported_capabilities()
        .iter()
        .map(|descriptor| node_v1::DeviceCapability {
            name: descriptor.name.to_owned(),
            available: true,
        })
        .collect::<Vec<_>>();
    let capability_count = capabilities.len();
    let response = client
        .register_node(Request::new(node_v1::RegisterNodeRequest {
            v: RUN_STREAM_REQUEST_VERSION,
            device_id: Some(canonical_id(config.device_id.as_str())),
            platform: node_platform_label(),
            capabilities,
            replay: None,
        }))
        .await
        .context("failed to register node host")?
        .into_inner();
    if !response.accepted {
        anyhow::bail!("node registration failed: {}", response.reason);
    }
    *connection_established = true;

    if emit_started_payload {
        emit_node_run_payload(
            &NodeRunPayload {
                action: "run",
                device_id: config.device_id.clone(),
                grpc_url: config.grpc_url.clone(),
                poll_interval_ms: config.poll_interval_ms,
                paired: true,
                capability_count,
            },
            json_output,
        )?;
    }

    let (sender, receiver) = mpsc::channel::<node_v1::NodeEventRequest>(16);
    sender
        .send(build_node_event_request(
            config.device_id.as_str(),
            if emit_started_payload { "node.started" } else { "node.reconnected" },
            json!({
                "device_id": config.device_id,
                "platform": node_platform_label(),
                "capabilities": supported_capabilities()
                    .iter()
                    .map(|descriptor| json!({
                        "name": descriptor.name,
                        "requires_local_mediation": descriptor.requires_local_mediation,
                    }))
                    .collect::<Vec<_>>(),
                "started_at_unix_ms": now_unix_ms(),
                "reconnected": !emit_started_payload,
            }),
        )?)
        .await
        .context("failed to queue node startup event")?;
    let mut responses = client
        .stream_node_events(Request::new(ReceiverStream::new(receiver)))
        .await
        .context("failed to open node event stream")?
        .into_inner();
    let heartbeat_sender = sender.clone();
    let heartbeat_device_id = config.device_id.clone();
    let heartbeat_interval_ms = config.poll_interval_ms.max(100);
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_millis(heartbeat_interval_ms)).await;
            let Ok(request) = build_node_event_request(
                heartbeat_device_id.as_str(),
                "node.heartbeat",
                json!({
                    "device_id": heartbeat_device_id.clone(),
                    "heartbeat_at_unix_ms": now_unix_ms(),
                }),
            ) else {
                break;
            };
            if heartbeat_sender.send(request).await.is_err() {
                break;
            }
        }
    });

    loop {
        tokio::select! {
            message = responses.message() => {
                let Some(message) = message.context("failed to receive node event stream message")? else {
                    anyhow::bail!("node event stream closed unexpectedly");
                };
                if let Some(dispatch) = message.dispatch {
                    if capability_requires_local_mediation(dispatch.capability.as_str()) {
                        sender
                            .send(build_node_event_request(
                                config.device_id.as_str(),
                                "capability.awaiting_local_mediation",
                                build_capability_lifecycle_payload(&dispatch, "awaiting_local_mediation")?,
                            )?)
                            .await
                            .context("failed to send capability mediation event to gateway")?;
                    }
                    let result_payload =
                        match execute_dispatched_capability(&mut client, &dispatch, config, device)
                            .await
                        {
                            Ok(payload) => payload,
                            Err(error) => capability_failure_payload(&dispatch, &error)?,
                        };
                    sender
                        .send(build_node_event_request(
                            config.device_id.as_str(),
                            "capability.result",
                            result_payload,
                        )?)
                        .await
                        .context("failed to send capability result to gateway")?;
                }
            }
            _ = tokio::signal::ctrl_c() => {
                let _ = sender
                    .send(build_node_event_request(
                        config.device_id.as_str(),
                        "node.stopping",
                        json!({
                            "device_id": config.device_id,
                            "stopped_at_unix_ms": now_unix_ms(),
                            "reason": "signal",
                        }),
                    )?)
                    .await;
                return Ok(());
            }
        }
    }
}

fn next_reconnect_attempt(
    current_attempt: u32,
    connection_established: bool,
    connection_uptime: Duration,
) -> u32 {
    if connection_established
        && connection_uptime >= Duration::from_millis(NODE_HOST_RECONNECT_STABLE_MS)
    {
        1
    } else {
        current_attempt.saturating_add(1)
    }
}

fn capability_failure_payload(
    dispatch: &node_v1::NodeCapabilityDispatch,
    _error: &anyhow::Error,
) -> Result<Value> {
    let request_id = dispatch
        .request_id
        .as_ref()
        .map(|value| value.ulid.as_str())
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| anyhow!("capability dispatch is missing request_id"))?;
    let delivery_attempt_id = dispatch
        .networked_worker_reservation
        .as_ref()
        .and_then(|reservation| reservation.delivery_attempt_id.as_ref())
        .map(|value| value.ulid.as_str())
        .filter(|value| !value.trim().is_empty());
    let run_generation =
        dispatch.networked_worker_reservation.as_ref().map(|value| value.run_generation);
    let reason_code = if dispatch.networked_worker_reservation.is_some() {
        "networked_worker_dispatch_failed"
    } else {
        "node_capability_failed"
    };
    Ok(json!({
        "request_id": request_id,
        "delivery_attempt_id": delivery_attempt_id,
        "run_generation": run_generation,
        "success": false,
        "output_json": Value::Null,
        "error": "node capability failed closed",
        "reason_code": reason_code,
    }))
}

fn run_node_start(json_output: bool) -> Result<()> {
    let config = load_node_host_config_required()?;
    let _ = load_node_client_material(&config)
        .context("node host cannot start before pairing material is installed")?;
    if let Some(metadata) = read_node_host_process_metadata()? {
        if process_is_running(metadata.pid) {
            return emit_node_lifecycle_payload(
                build_node_status_payload("start", "node host is already running")?,
                json_output,
            );
        }
        remove_node_host_process_metadata()?;
    }

    let binary = support::lifecycle::current_cli_binary_path()?;
    let state_dir = node_host_state_dir(true)?;
    let stdout_log_path = state_dir.join(NODE_HOST_STDOUT_LOG_FILE_NAME);
    let stderr_log_path = state_dir.join(NODE_HOST_STDERR_LOG_FILE_NAME);
    let stdout = File::create(stdout_log_path.as_path())
        .with_context(|| format!("failed to create {}", stdout_log_path.display()))?;
    let stderr = File::create(stderr_log_path.as_path())
        .with_context(|| format!("failed to create {}", stderr_log_path.display()))?;
    let root_context = app::current_root_context()
        .ok_or_else(|| anyhow!("CLI root context is unavailable for node host lifecycle"))?;

    let mut command = Command::new(binary.as_path());
    command
        .arg("node")
        .arg("run")
        .arg("--json")
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .env("PALYRA_STATE_ROOT", root_context.state_root());
    if let Some(config_path) = root_context.config_path() {
        command.env("PALYRA_CONFIG", config_path);
    }
    #[cfg(windows)]
    command.creation_flags(DETACHED_PROCESS | CREATE_NO_WINDOW);

    let child = command
        .spawn()
        .with_context(|| format!("failed to start node host binary {}", binary.display()))?;
    let metadata = NodeHostProcessMetadata {
        schema_version: NODE_HOST_PROCESS_SCHEMA_VERSION,
        pid: child.id(),
        stdout_log_path: stdout_log_path.display().to_string(),
        stderr_log_path: stderr_log_path.display().to_string(),
        started_at_unix_ms: now_unix_ms(),
    };
    write_node_host_process_metadata(&metadata)?;
    // Give the detached child a short window to fail fast so startup crashes
    // surface here with log paths instead of silently backgrounding.
    std::thread::sleep(Duration::from_millis(NODE_HOST_START_POLL_MS));

    if !process_is_running(metadata.pid) {
        anyhow::bail!(
            "node host exited before startup completed; inspect {} and {}",
            stdout_log_path.display(),
            stderr_log_path.display()
        );
    }

    emit_node_lifecycle_payload(
        build_node_status_payload("start", "node host start requested")?,
        json_output,
    )
}

fn run_node_stop(json_output: bool) -> Result<()> {
    let Some(metadata) = read_node_host_process_metadata()? else {
        return emit_node_lifecycle_payload(
            build_node_status_payload("stop", "no CLI-managed node host process metadata found")?,
            json_output,
        );
    };

    if process_is_running(metadata.pid) {
        terminate_process(metadata.pid)
            .with_context(|| format!("failed to stop node host process {}", metadata.pid))?;
    }
    remove_node_host_process_metadata()?;
    emit_node_lifecycle_payload(
        build_node_status_payload("stop", "node host stop requested")?,
        json_output,
    )
}

fn run_node_uninstall(json_output: bool) -> Result<()> {
    if let Some(metadata) = read_node_host_process_metadata()? {
        if process_is_running(metadata.pid) {
            terminate_process(metadata.pid)
                .with_context(|| format!("failed to stop node host process {}", metadata.pid))?;
        }
        remove_node_host_process_metadata()?;
    }

    if let Some(config) = read_node_host_config()? {
        let identity_store_dir = PathBuf::from(config.identity_store_dir);
        if identity_store_dir.exists() {
            support::lifecycle::ensure_safe_removal_target(
                identity_store_dir.as_path(),
                "node identity store",
            )?;
            fs::remove_dir_all(identity_store_dir.as_path()).with_context(|| {
                format!("failed to remove node identity store {}", identity_store_dir.display())
            })?;
        }
    }

    let state_dir = node_host_state_dir(false)?;
    if state_dir.exists() {
        support::lifecycle::ensure_safe_removal_target(state_dir.as_path(), "node host state dir")?;
        fs::remove_dir_all(state_dir.as_path()).with_context(|| {
            format!("failed to remove node host state dir {}", state_dir.display())
        })?;
    }

    emit_node_lifecycle_payload(
        NodeLifecyclePayload {
            action: "uninstall".to_owned(),
            installed: false,
            paired: false,
            running: false,
            device_id: None,
            grpc_url: None,
            identity_store_dir: None,
            cert_expires_at_unix_ms: None,
            pid: None,
            stdout_log_path: None,
            stderr_log_path: None,
            detail: "node host configuration and local identity material removed".to_owned(),
        },
        json_output,
    )
}

async fn ensure_node_pairing_material(
    config: &mut NodeHostConfig,
    method: Option<PairingMethodArg>,
    pairing_code: Option<String>,
    pairing_code_stdin: bool,
    allow_insecure_pairing_code_arg: bool,
    gateway_ca_file: Option<String>,
) -> Result<()> {
    if load_node_client_material(config).is_ok() {
        return Ok(());
    }

    let method = method.ok_or_else(|| {
        anyhow!("node pairing bootstrap requires --method when local pairing material is absent")
    })?;
    let pairing_code = resolve_pairing_code_input(
        pairing_code,
        pairing_code_stdin,
        allow_insecure_pairing_code_arg,
    )?
    .ok_or_else(|| {
        anyhow!(
            "node pairing bootstrap requires --pairing-code-stdin, or --pairing-code with --allow-insecure-pairing-code-arg, when local pairing material is absent"
        )
    })?;
    let gateway_ca_certificate_pem = match gateway_ca_file.map(PathBuf::from) {
        Some(gateway_ca_file) => {
            fs::read_to_string(gateway_ca_file.as_path()).with_context(|| {
                format!("failed to read gateway CA certificate file {}", gateway_ca_file.display())
            })?
        }
        None => {
            let gateway_identity_store_dir = std::env::var("PALYRA_GATEWAY_IDENTITY_STORE_DIR")
                .ok()
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty());
            load_gateway_ca_certificate_pem(gateway_identity_store_dir).context(
                "node pairing bootstrap requires --gateway-ca-file or PALYRA_GATEWAY_IDENTITY_STORE_DIR with gateway trust material",
            )?
        }
    };

    let store = build_identity_store(Path::new(config.identity_store_dir.as_str()))?;
    let device = DeviceIdentity::generate(config.device_id.as_str())
        .map_err(anyhow::Error::from)
        .with_context(|| {
        format!("failed to generate node device identity {}", config.device_id)
    })?;
    let mut client =
        connect_node_service(config.grpc_url.as_str(), gateway_ca_certificate_pem.as_str(), None)
            .await?;
    let begin = client
        .begin_pairing_session(Request::new(node_v1::BeginPairingSessionRequest {
            v: RUN_STREAM_REQUEST_VERSION,
            client_kind: "node".to_owned(),
            method: Some(pairing_method_to_proto(method, pairing_code.as_str())),
            replay: None,
        }))
        .await
        .context("failed to begin node pairing session")?
        .into_inner();
    let session = PairingSession {
        session_id: begin.session_id.clone(),
        protocol_version: begin.v.max(1),
        client_kind: PairingClientKind::Node,
        method: build_pairing_method(method, pairing_code.as_str()),
        gateway_ephemeral_public: begin
            .gateway_ephemeral_public
            .as_slice()
            .try_into()
            .map_err(|_| anyhow!("gateway pairing public key length mismatch"))?,
        challenge: begin
            .challenge
            .as_slice()
            .try_into()
            .map_err(|_| anyhow!("gateway pairing challenge length mismatch"))?,
        expires_at_unix_ms: begin.expires_at_unix_ms,
    };
    let hello = build_device_pairing_hello(&session, &device, pairing_code.as_str())
        .map_err(anyhow::Error::from)
        .context("failed to build remote node pairing hello")?;
    let complete = client
        .complete_pairing_session(Request::new(node_v1::CompletePairingSessionRequest {
            v: hello.protocol_version,
            session_id: hello.session_id.clone(),
            device_id: Some(canonical_id(hello.device_id.as_str())),
            client_kind: hello.client_kind.as_str().to_owned(),
            proof: hello.proof.clone(),
            device_signing_public: hello.device_signing_public.to_vec(),
            device_x25519_public: hello.device_x25519_public.to_vec(),
            challenge_signature: hello.challenge_signature.to_vec(),
            transcript_mac: hello.transcript_mac.to_vec(),
        }))
        .await
        .context("failed to complete remote node pairing session")?
        .into_inner();

    let mut latest_status = node_v1::GetPairingRequestStatusResponse {
        v: complete.v,
        status: if complete.paired {
            "completed".to_owned()
        } else {
            "pending_approval".to_owned()
        },
        reason: complete.reason.clone(),
        paired: complete.paired,
        approval_id: String::new(),
        identity_fingerprint: complete.identity_fingerprint.clone(),
        transcript_hash: complete.transcript_hash.clone(),
        mtls_client_certificate_pem: complete.mtls_client_certificate_pem.clone(),
        mtls_client_private_key_pem: complete.mtls_client_private_key_pem.clone(),
        gateway_ca_certificate_pem: complete.gateway_ca_certificate_pem.clone(),
        cert_expires_at_unix_ms: complete.cert_expires_at_unix_ms,
    };
    while !latest_status.paired {
        match latest_status.status.as_str() {
            "rejected" => {
                anyhow::bail!("node pairing request was rejected: {}", latest_status.reason)
            }
            "expired" => anyhow::bail!("node pairing request expired"),
            _ => {}
        }

        sleep(Duration::from_millis(config.poll_interval_ms.max(250))).await;
        latest_status = client
            .get_pairing_request_status(Request::new(node_v1::GetPairingRequestStatusRequest {
                v: RUN_STREAM_REQUEST_VERSION,
                session_id: hello.session_id.clone(),
                device_id: Some(canonical_id(config.device_id.as_str())),
            }))
            .await
            .context("failed to poll node pairing request status")?
            .into_inner();
    }

    let certificate_pem = required_nonempty_text(
        latest_status.mtls_client_certificate_pem,
        "paired mTLS client certificate",
    )?;
    let private_key_pem = required_nonempty_text(
        latest_status.mtls_client_private_key_pem,
        "paired mTLS client private key",
    )?;
    let gateway_ca_certificate_pem = required_nonempty_text(
        latest_status.gateway_ca_certificate_pem,
        "paired gateway CA certificate",
    )?;
    device
        .store(store.as_ref())
        .map_err(anyhow::Error::from)
        .with_context(|| format!("failed to persist node device identity {}", config.device_id))?;
    store_node_client_material(
        store.as_ref(),
        config.device_id.as_str(),
        &StoredNodeClientCertificate {
            certificate_pem,
            private_key_pem,
            cert_expires_at_unix_ms: latest_status.cert_expires_at_unix_ms,
        },
        gateway_ca_certificate_pem.as_str(),
    )?;
    config.paired_at_unix_ms = Some(now_unix_ms());
    Ok(())
}

fn resolve_pairing_code_input(
    pairing_code: Option<String>,
    pairing_code_stdin: bool,
    allow_insecure_pairing_code_arg: bool,
) -> Result<Option<String>> {
    if pairing_code.is_some() && pairing_code_stdin {
        anyhow::bail!("use either --pairing-code or --pairing-code-stdin, not both");
    }
    if pairing_code_stdin {
        let mut raw = String::new();
        std::io::stdin()
            .read_to_string(&mut raw)
            .context("failed to read node pairing code from stdin")?;
        return Ok(normalize_pairing_code_input(raw));
    }
    if let Some(pairing_code) = pairing_code {
        if !allow_insecure_pairing_code_arg {
            anyhow::bail!(
                "refusing --pairing-code without --allow-insecure-pairing-code-arg because command-line arguments can be exposed through process lists; use --pairing-code-stdin instead"
            );
        }
        return Ok(normalize_pairing_code_input(pairing_code));
    }
    Ok(None)
}

fn normalize_pairing_code_input(value: String) -> Option<String> {
    let normalized = value.trim().to_owned();
    (!normalized.is_empty()).then_some(normalized)
}

fn build_node_status_payload(action: &str, detail: &str) -> Result<NodeLifecyclePayload> {
    let config = read_node_host_config()?;
    let metadata = read_node_host_process_metadata()?;
    let running = metadata.as_ref().is_some_and(|value| process_is_running(value.pid));
    let paired_material = config.as_ref().and_then(|value| load_node_client_material(value).ok());

    Ok(NodeLifecyclePayload {
        action: action.to_owned(),
        installed: config.is_some(),
        paired: paired_material.is_some(),
        running,
        device_id: config.as_ref().map(|value| value.device_id.clone()),
        grpc_url: config.as_ref().map(|value| value.grpc_url.clone()),
        identity_store_dir: config.as_ref().map(|value| value.identity_store_dir.clone()),
        cert_expires_at_unix_ms: paired_material
            .as_ref()
            .map(|value| value.certificate.cert_expires_at_unix_ms),
        pid: metadata.as_ref().filter(|_| running).map(|value| value.pid),
        stdout_log_path: metadata.as_ref().map(|value| value.stdout_log_path.clone()),
        stderr_log_path: metadata.as_ref().map(|value| value.stderr_log_path.clone()),
        detail: detail.to_owned(),
    })
}

fn resolve_node_host_config(
    grpc_url: Option<String>,
    device_id: Option<String>,
    poll_interval_ms: Option<u64>,
) -> Result<NodeHostConfig> {
    let mut config = read_node_host_config()?.unwrap_or_else(default_node_host_config);
    if let Some(grpc_url) = grpc_url {
        config.grpc_url = resolve_node_rpc_grpc_url(Some(grpc_url))?;
    }
    if let Some(device_id) =
        device_id.map(|value| value.trim().to_owned()).filter(|value| !value.is_empty())
    {
        if config.paired_at_unix_ms.is_some() && config.device_id != device_id {
            anyhow::bail!(
                "node host is already paired as device_id={}; uninstall or clear local state before changing device_id",
                config.device_id
            );
        }
        config.device_id = device_id;
    }
    if let Some(poll_interval_ms) = poll_interval_ms {
        config.poll_interval_ms = poll_interval_ms.max(100);
    }
    Ok(config)
}

fn default_node_host_config() -> NodeHostConfig {
    let identity_store_dir = node_host_identity_store_dir()
        .unwrap_or_else(|_| PathBuf::from(NODE_HOST_STATE_DIR).join("identity"));
    NodeHostConfig {
        schema_version: NODE_HOST_CONFIG_SCHEMA_VERSION,
        grpc_url: resolve_node_rpc_grpc_url(None)
            .unwrap_or_else(|_| "https://127.0.0.1:7444".to_owned()),
        device_id: Ulid::generate().to_string(),
        poll_interval_ms: DEFAULT_NODE_POLL_INTERVAL_MS,
        identity_store_dir: identity_store_dir.display().to_string(),
        installed_at_unix_ms: now_unix_ms(),
        paired_at_unix_ms: None,
    }
}

fn resolve_node_rpc_grpc_url(explicit: Option<String>) -> Result<String> {
    if let Some(explicit) =
        explicit.map(|value| value.trim().to_owned()).filter(|value| !value.is_empty())
    {
        let mut parsed = reqwest::Url::parse(explicit.as_str())
            .with_context(|| format!("invalid node gRPC URL {explicit}"))?;
        if parsed.scheme() == "http" {
            parsed
                .set_scheme("https")
                .map_err(|_| anyhow!("failed to convert node gRPC URL to https"))?;
        }
        return Ok(parsed.to_string());
    }

    // By convention the gateway exposes the node RPC listener one port above
    // its admin gRPC port, always over TLS.
    let admin_grpc_url = client::grpc::resolve_url(None)?;
    let mut parsed = reqwest::Url::parse(admin_grpc_url.as_str())
        .with_context(|| format!("invalid gateway gRPC URL {admin_grpc_url}"))?;
    let port = parsed
        .port_or_known_default()
        .ok_or_else(|| anyhow!("gateway gRPC URL does not include a resolvable port"))?;
    parsed
        .set_scheme("https")
        .map_err(|_| anyhow!("failed to convert gateway gRPC URL to https"))?;
    parsed
        .set_port(Some(port.saturating_add(1)))
        .map_err(|_| anyhow!("failed to derive node RPC port from gateway gRPC URL"))?;
    Ok(parsed.to_string())
}

async fn connect_node_service(
    grpc_url: &str,
    gateway_ca_certificate_pem: &str,
    identity: Option<&StoredNodeClientCertificate>,
) -> Result<node_v1::node_service_client::NodeServiceClient<Channel>> {
    let mut tls_config = ClientTlsConfig::new()
        .domain_name("palyrad-node-rpc")
        .ca_certificate(Certificate::from_pem(gateway_ca_certificate_pem));
    if let Some(identity) = identity {
        tls_config = tls_config.identity(Identity::from_pem(
            identity.certificate_pem.clone(),
            identity.private_key_pem.clone(),
        ));
    }
    let endpoint = Endpoint::from_shared(grpc_url.to_owned())
        .with_context(|| format!("invalid node gRPC URL {grpc_url}"))?
        .tls_config(tls_config)
        .context("failed to configure node gRPC TLS client")?;
    let channel = endpoint
        .connect()
        .await
        .with_context(|| format!("failed to connect node gRPC endpoint {grpc_url}"))?;
    Ok(node_v1::node_service_client::NodeServiceClient::new(channel))
}

async fn execute_dispatched_capability(
    client: &mut node_v1::node_service_client::NodeServiceClient<Channel>,
    dispatch: &node_v1::NodeCapabilityDispatch,
    config: &NodeHostConfig,
    device: &DeviceIdentity,
) -> Result<Value> {
    if dispatch.networked_worker_reservation.is_some() {
        return execute_networked_worker_capability(client, dispatch, config, device).await;
    }
    execute_generic_dispatched_capability(dispatch, config, device)
}

fn execute_generic_dispatched_capability(
    dispatch: &node_v1::NodeCapabilityDispatch,
    config: &NodeHostConfig,
    device: &DeviceIdentity,
) -> Result<Value> {
    let request_id = dispatch
        .request_id
        .as_ref()
        .map(|value| value.ulid.as_str())
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| anyhow!("capability dispatch is missing request_id"))?;
    let input_json = if dispatch.input_json.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(dispatch.input_json.as_slice())
            .context("failed to decode dispatched capability input JSON")?
    };
    let result =
        execute_local_capability(dispatch.capability.as_str(), &input_json, config, device);
    let payload_limit = usize::try_from(dispatch.max_payload_bytes).unwrap_or(usize::MAX).max(1);
    let had_output = !result.output_json.is_null();
    // Oversized outputs are dropped rather than truncated so the gateway never
    // receives partial JSON; the error field below explains the omission.
    let output_json = if result.success {
        let encoded = serde_json::to_vec(&result.output_json)
            .context("failed to encode local capability output")?;
        if encoded.len() > payload_limit {
            Value::Null
        } else {
            result.output_json
        }
    } else {
        Value::Null
    };
    let error = if result.success && output_json.is_null() && had_output {
        format!("capability output exceeds max_payload_bytes={}", dispatch.max_payload_bytes)
    } else {
        result.error
    };

    Ok(json!({
        "request_id": request_id,
        "success": error.is_empty(),
        "output_json": output_json,
        "error": error,
    }))
}

async fn execute_networked_worker_capability(
    client: &mut node_v1::node_service_client::NodeServiceClient<Channel>,
    dispatch: &node_v1::NodeCapabilityDispatch,
    config: &NodeHostConfig,
    device: &DeviceIdentity,
) -> Result<Value> {
    let request_id = dispatch
        .request_id
        .as_ref()
        .map(|value| value.ulid.as_str())
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| anyhow!("networked worker dispatch is missing request_id"))?;
    let reservation = dispatch
        .networked_worker_reservation
        .as_ref()
        .ok_or_else(|| anyhow!("networked worker dispatch is missing delivery reservation"))?;
    let reservation_request_id = reservation
        .request_id
        .as_ref()
        .map(|value| value.ulid.as_str())
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| anyhow!("networked worker reservation is missing request_id"))?;
    let delivery_attempt_id = reservation
        .delivery_attempt_id
        .as_ref()
        .map(|value| value.ulid.as_str())
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| anyhow!("networked worker reservation is missing delivery_attempt_id"))?;
    if request_id != reservation_request_id
        || reservation.v == 0
        || reservation.protocol != WORKER_REMOTE_TOOL_PROTOCOL
        || reservation.worker_id != config.device_id
        || reservation.fleet_generation == 0
        || reservation.run_generation == 0
        || reservation.expires_at_unix_ms <= now_unix_ms()
        || reservation.fetch_token.trim().is_empty()
    {
        anyhow::bail!("networked worker reservation does not match this authenticated node");
    }

    let response = client
        .fetch_networked_worker_payload(Request::new(node_v1::FetchNetworkedWorkerPayloadRequest {
            v: RUN_STREAM_REQUEST_VERSION,
            device_id: Some(canonical_id(config.device_id.as_str())),
            request_id: Some(canonical_id(request_id)),
            delivery_attempt_id: Some(canonical_id(delivery_attempt_id)),
            fetch_token: reservation.fetch_token.clone(),
        }))
        .await
        .context("failed to fetch reserved networked worker payload")?
        .into_inner();
    validate_networked_worker_payload_response(
        &response,
        reservation,
        request_id,
        delivery_attempt_id,
        dispatch.max_payload_bytes,
    )?;
    let request =
        serde_json::from_slice::<WorkerRemoteToolRequestEnvelope>(response.input_json.as_slice())
            .context("failed to decode canonical networked worker request")?;
    validate_networked_worker_request_binding(&request, dispatch, reservation, config, device)?;

    let idempotency_key = request
        .canonical_protocol
        .as_ref()
        .map(|protocol| protocol.task.idempotency_key.as_str())
        .ok_or_else(|| anyhow!("networked worker request omitted canonical protocol binding"))?;
    admit_networked_worker_replay(
        idempotency_key,
        request.request_id.as_str(),
        u64::try_from(request.lease.expires_at_unix_ms).unwrap_or(u64::MAX),
    )?;

    let acknowledgement = client
        .acknowledge_networked_worker_payload(Request::new(
            node_v1::AcknowledgeNetworkedWorkerPayloadRequest {
                v: RUN_STREAM_REQUEST_VERSION,
                device_id: Some(canonical_id(config.device_id.as_str())),
                request_id: Some(canonical_id(request_id)),
                delivery_attempt_id: Some(canonical_id(delivery_attempt_id)),
                fetch_token: reservation.fetch_token.clone(),
            },
        ))
        .await
        .context("failed to acknowledge networked worker payload")?
        .into_inner();
    if !acknowledgement.acknowledged {
        anyhow::bail!(
            "networked worker payload acknowledgement rejected: {}",
            acknowledgement.reason
        );
    }

    let observed_at_unix_ms = i64::try_from(now_unix_ms()).unwrap_or(i64::MAX);
    let adapter = (|| -> Result<CanonicalWorkerStdioAdapter> {
        let workerd_executable = resolve_sibling_workerd_binary(
            support::lifecycle::current_cli_binary_path()?.as_path(),
        )?;
        let lease_remaining_ms = request
            .lease
            .expires_at_unix_ms
            .saturating_sub(observed_at_unix_ms)
            .try_into()
            .unwrap_or(0_u64);
        let task_wall_time_ms = request
            .canonical_protocol
            .as_ref()
            .map_or(0, |protocol| protocol.task.resource_limits.wall_time_ms);
        let child_timeout_ms =
            lease_remaining_ms.min(task_wall_time_ms).min(NODE_WORKER_CHILD_MAX_TIMEOUT_MS);
        CanonicalWorkerStdioAdapter::local_workerd(workerd_executable, child_timeout_ms)
            .context("networked worker child process is unavailable")
    })();
    let result = match adapter {
        Ok(adapter) => {
            let request_for_worker = request.clone();
            tokio::task::spawn_blocking(move || {
                adapter.execute(&request_for_worker, observed_at_unix_ms)
            })
            .await
            .map_err(|error| anyhow!("networked worker execution task failed: {error}"))
            .and_then(|result| result.map_err(anyhow::Error::from))
        }
        Err(error) => Err(error),
    }
    .and_then(|result| {
        let receipt_unix_ms = i64::try_from(now_unix_ms()).unwrap_or(i64::MAX);
        result
            .validate_against_request(&request, receipt_unix_ms)
            .context("networked worker child result arrived after its authority expired")?;
        Ok(result)
    });
    let result = match result {
        Ok(result) => result,
        Err(error) => {
            settle_networked_worker_replay(
                idempotency_key,
                request.request_id.as_str(),
                sha256_hex(error.to_string().as_bytes()),
            )?;
            return Ok(json!({
                "request_id": request_id,
                "delivery_attempt_id": delivery_attempt_id,
                "run_generation": reservation.run_generation,
                "success": false,
                "output_json": Value::Null,
                "error": "networked worker execution failed",
                "reason_code": "networked_worker_execution_failed",
            }));
        }
    };
    let encoded =
        serde_json::to_vec(&result).context("failed to encode networked worker result envelope")?;
    if encoded.len() > usize::try_from(dispatch.max_payload_bytes).unwrap_or(usize::MAX) {
        settle_networked_worker_replay(
            idempotency_key,
            request.request_id.as_str(),
            sha256_hex(encoded.as_slice()),
        )?;
        return Ok(json!({
            "request_id": request_id,
            "delivery_attempt_id": delivery_attempt_id,
            "run_generation": reservation.run_generation,
            "success": false,
            "output_json": Value::Null,
            "error": "networked worker result exceeds max_payload_bytes",
            "reason_code": "networked_worker_result_oversized",
        }));
    }
    settle_networked_worker_replay(
        idempotency_key,
        request.request_id.as_str(),
        sha256_hex(encoded.as_slice()),
    )?;
    Ok(json!({
        "request_id": request_id,
        "delivery_attempt_id": delivery_attempt_id,
        "run_generation": reservation.run_generation,
        "success": true,
        "output_json": result,
        "error": "",
        "reason_code": "networked_worker_completed",
    }))
}

fn resolve_sibling_workerd_binary(current_cli_binary: &Path) -> Result<PathBuf> {
    if !current_cli_binary.is_absolute() {
        anyhow::bail!("current CLI executable path is not absolute");
    }
    let parent = current_cli_binary.parent().ok_or_else(|| {
        anyhow!("current CLI executable has no install directory: {}", current_cli_binary.display())
    })?;
    let executable_name = if cfg!(windows) { "palyra-workerd.exe" } else { "palyra-workerd" };
    let workerd = parent.join(executable_name);
    if !workerd.is_file() {
        anyhow::bail!("isolated network worker executable is unavailable at {}", workerd.display());
    }
    workerd
        .canonicalize()
        .with_context(|| format!("failed to resolve isolated worker {}", workerd.display()))
}

fn validate_networked_worker_payload_response(
    response: &node_v1::FetchNetworkedWorkerPayloadResponse,
    reservation: &node_v1::NetworkedWorkerDeliveryReservation,
    request_id: &str,
    delivery_attempt_id: &str,
    expected_max_payload_bytes: u64,
) -> Result<()> {
    let response_request_id =
        response.request_id.as_ref().map(|value| value.ulid.as_str()).unwrap_or_default();
    let response_delivery_attempt_id =
        response.delivery_attempt_id.as_ref().map(|value| value.ulid.as_str()).unwrap_or_default();
    let observed_sha256 = sha256_hex(response.input_json.as_slice());
    if response_request_id != request_id
        || response_delivery_attempt_id != delivery_attempt_id
        || response.request_sha256 != reservation.request_sha256
        || response.request_sha256 != observed_sha256
        || response.max_payload_bytes != expected_max_payload_bytes
        || response.input_json.len()
            > usize::try_from(response.max_payload_bytes).unwrap_or(usize::MAX)
    {
        anyhow::bail!("networked worker payload response failed identity or digest validation");
    }
    if !verify_authenticated_delivery_hmac_sha256(
        reservation.fetch_token.as_str(),
        response.request_sha256.as_str(),
        response.input_json.as_slice(),
        response.authenticated_delivery_hmac_sha256.as_str(),
    ) {
        anyhow::bail!("networked worker payload authentication failed");
    }
    Ok(())
}

fn validate_networked_worker_request_binding(
    request: &WorkerRemoteToolRequestEnvelope,
    dispatch: &node_v1::NodeCapabilityDispatch,
    reservation: &node_v1::NetworkedWorkerDeliveryReservation,
    config: &NodeHostConfig,
    device: &DeviceIdentity,
) -> Result<()> {
    let now = i64::try_from(now_unix_ms()).unwrap_or(i64::MAX);
    request.validate(now).context("networked worker request contract validation failed")?;
    let expected_capability = request.tool_kind.required_capability();
    let lease_id =
        reservation.lease_id.as_ref().map(|value| value.ulid.as_str()).unwrap_or_default();
    let run_id = reservation.run_id.as_ref().map(|value| value.ulid.as_str()).unwrap_or_default();
    if request.lease.worker_id != config.device_id
        || request.worker_identity.worker_id != config.device_id
        || request.lease.lease_id != lease_id
        || request.lease.run_id != run_id
        || request.lease.run_generation.get() != reservation.run_generation
        || request.lease.expires_at_unix_ms
            != i64::try_from(reservation.expires_at_unix_ms).unwrap_or(i64::MAX)
        || expected_capability != dispatch.capability
        || request.lease.required_capabilities.iter().any(|required| {
            !supported_capabilities().iter().any(|available| available.name == required.as_str())
        })
    {
        anyhow::bail!(
            "networked worker request does not match the authenticated delivery reservation"
        );
    }
    let device_authority_sha256 = sha256_hex(&device.signing_public_key());
    if request.worker_identity.capability_authority_sha256.as_deref()
        != Some(device_authority_sha256.as_str())
    {
        anyhow::bail!(
            "networked worker attestation is not bound to the authenticated device signing key"
        );
    }
    Ok(())
}

fn node_host_worker_replay_path() -> Result<PathBuf> {
    Ok(node_host_state_dir(true)?.join(NODE_HOST_WORKER_REPLAY_FILE_NAME))
}

fn read_networked_worker_replay_registry_at(path: &Path) -> Result<NodeWorkerReplayRegistry> {
    if !path.exists() {
        return Ok(NodeWorkerReplayRegistry::default());
    }
    let bytes = fs::read(path)
        .with_context(|| format!("failed to read worker replay registry {}", path.display()))?;
    let registry = serde_json::from_slice::<NodeWorkerReplayRegistry>(bytes.as_slice())
        .with_context(|| format!("failed to parse worker replay registry {}", path.display()))?;
    if registry.schema_version != NODE_WORKER_REPLAY_SCHEMA_VERSION {
        anyhow::bail!("worker replay registry schema {} is unsupported", registry.schema_version);
    }
    Ok(registry)
}

fn write_networked_worker_replay_registry_at(
    path: &Path,
    registry: &NodeWorkerReplayRegistry,
) -> Result<()> {
    let payload = serde_json::to_vec(registry)
        .context("failed to encode networked worker replay registry")?;
    write_file_atomically(path, payload.as_slice())
        .with_context(|| format!("failed to write worker replay registry {}", path.display()))
}

fn admit_networked_worker_replay(
    idempotency_key: &str,
    request_id: &str,
    reconcile_after_unix_ms: u64,
) -> Result<()> {
    let path = node_host_worker_replay_path()?;
    admit_networked_worker_replay_at(
        path.as_path(),
        idempotency_key,
        request_id,
        now_unix_ms(),
        reconcile_after_unix_ms,
    )
}

fn admit_networked_worker_replay_at(
    path: &Path,
    idempotency_key: &str,
    request_id: &str,
    observed_at_unix_ms: u64,
    reconcile_after_unix_ms: u64,
) -> Result<()> {
    let mut registry = read_networked_worker_replay_registry_at(path)?;
    let mut reconciled = false;
    for record in registry.records.values_mut() {
        if record.state == NodeWorkerReplayState::InFlight
            && record.reconcile_after_unix_ms <= observed_at_unix_ms
        {
            record.state = NodeWorkerReplayState::OutcomeUnknown;
            record.updated_at_unix_ms = observed_at_unix_ms;
            reconciled = true;
        }
    }
    if reconciled {
        write_networked_worker_replay_registry_at(path, &registry)?;
    }
    if let Some(record) = registry.records.get(idempotency_key) {
        let state = match record.state {
            NodeWorkerReplayState::InFlight => "in_flight",
            NodeWorkerReplayState::OutcomeUnknown => "outcome_unknown",
            NodeWorkerReplayState::Settled => "settled",
        };
        anyhow::bail!(
            "networked worker duplicate task requires reconciliation: prior_request_id={}, state={state}",
            record.request_id,
        );
    }
    if registry.records.len() >= NODE_WORKER_REPLAY_MAX_RECORDS {
        let oldest_settled = registry
            .records
            .iter()
            .filter(|(_, record)| record.state == NodeWorkerReplayState::Settled)
            .min_by_key(|(_, record)| record.updated_at_unix_ms)
            .map(|(key, _)| key.clone())
            .ok_or_else(|| anyhow!("networked worker replay registry has no evictable records"))?;
        registry.records.remove(oldest_settled.as_str());
    }
    registry.records.insert(
        idempotency_key.to_owned(),
        NodeWorkerReplayRecord {
            request_id: request_id.to_owned(),
            state: NodeWorkerReplayState::InFlight,
            updated_at_unix_ms: observed_at_unix_ms,
            reconcile_after_unix_ms,
            result_sha256: None,
        },
    );
    write_networked_worker_replay_registry_at(path, &registry)
}

fn settle_networked_worker_replay(
    idempotency_key: &str,
    request_id: &str,
    result_sha256: String,
) -> Result<()> {
    let path = node_host_worker_replay_path()?;
    settle_networked_worker_replay_at(
        path.as_path(),
        idempotency_key,
        request_id,
        result_sha256,
        now_unix_ms(),
    )
}

fn settle_networked_worker_replay_at(
    path: &Path,
    idempotency_key: &str,
    request_id: &str,
    result_sha256: String,
    observed_at_unix_ms: u64,
) -> Result<()> {
    let mut registry = read_networked_worker_replay_registry_at(path)?;
    let record = registry
        .records
        .get_mut(idempotency_key)
        .filter(|record| {
            record.request_id == request_id && record.state == NodeWorkerReplayState::InFlight
        })
        .ok_or_else(|| anyhow!("networked worker replay fence disappeared before settlement"))?;
    record.state = NodeWorkerReplayState::Settled;
    record.updated_at_unix_ms = observed_at_unix_ms;
    record.result_sha256 = Some(result_sha256);
    write_networked_worker_replay_registry_at(path, &registry)
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn execute_local_capability(
    capability: &str,
    input_json: &Value,
    config: &NodeHostConfig,
    device: &DeviceIdentity,
) -> LocalCapabilityResult {
    match capability.trim() {
        "echo" => LocalCapabilityResult {
            success: true,
            output_json: json!({
                "echo": input_json,
                "device_id": config.device_id,
            }),
            error: String::new(),
        },
        "system.health" => LocalCapabilityResult {
            success: true,
            output_json: json!({
                "status": "ok",
                "device_id": config.device_id,
                "grpc_url": config.grpc_url,
                "poll_interval_ms": config.poll_interval_ms,
                "generated_at_unix_ms": now_unix_ms(),
            }),
            error: String::new(),
        },
        "system.identity" => LocalCapabilityResult {
            success: true,
            output_json: json!({
                "device_id": config.device_id,
                "identity_fingerprint": device.fingerprint(),
                "grpc_url": config.grpc_url,
                "paired_at_unix_ms": config.paired_at_unix_ms,
            }),
            error: String::new(),
        },
        "desktop.open_url" => match open_url_capability(input_json) {
            Ok(url) => LocalCapabilityResult {
                success: true,
                output_json: json!({
                    "opened": true,
                    "url": url,
                }),
                error: String::new(),
            },
            Err(error) => LocalCapabilityResult {
                success: false,
                output_json: Value::Null,
                error: error.to_string(),
            },
        },
        "desktop.open_path" => match open_path_capability(input_json) {
            Ok(path) => LocalCapabilityResult {
                success: true,
                output_json: json!({
                    "opened": true,
                    "path": path.display().to_string(),
                    "kind": if path.is_dir() { "directory" } else { "file" },
                }),
                error: String::new(),
            },
            Err(error) => LocalCapabilityResult {
                success: false,
                output_json: Value::Null,
                error: error.to_string(),
            },
        },
        other => LocalCapabilityResult {
            success: false,
            output_json: Value::Null,
            error: format!("unsupported capability `{other}`"),
        },
    }
}

fn build_node_event_request(
    device_id: &str,
    event_name: &str,
    payload_json: Value,
) -> Result<node_v1::NodeEventRequest> {
    Ok(node_v1::NodeEventRequest {
        v: RUN_STREAM_REQUEST_VERSION,
        device_id: Some(canonical_id(device_id)),
        event_name: event_name.to_owned(),
        payload_json: serde_json::to_vec(&payload_json)
            .context("failed to encode node event payload as JSON")?,
        replay: None,
    })
}

fn pairing_method_to_proto(method: PairingMethodArg, pairing_code: &str) -> node_v1::PairingMethod {
    match method {
        PairingMethodArg::Pin => node_v1::PairingMethod {
            value: Some(node_v1::pairing_method::Value::PinCode(pairing_code.to_owned())),
        },
        PairingMethodArg::Qr => node_v1::PairingMethod {
            value: Some(node_v1::pairing_method::Value::QrToken(pairing_code.to_owned())),
        },
    }
}

fn canonical_id(value: &str) -> common_v1::CanonicalId {
    common_v1::CanonicalId { ulid: value.to_owned() }
}

fn supported_capabilities() -> &'static [NodeCapabilityDescriptor] {
    &NODE_CAPABILITY_DESCRIPTORS
}

fn capability_requires_local_mediation(capability: &str) -> bool {
    supported_capabilities()
        .iter()
        .find(|descriptor| descriptor.name == capability.trim())
        .is_some_and(|descriptor| descriptor.requires_local_mediation)
}

fn build_capability_lifecycle_payload(
    dispatch: &node_v1::NodeCapabilityDispatch,
    state: &str,
) -> Result<Value> {
    let request_id = dispatch
        .request_id
        .as_ref()
        .map(|value| value.ulid.as_str())
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| anyhow!("capability dispatch is missing request_id"))?;
    Ok(json!({
        "request_id": request_id,
        "capability": dispatch.capability,
        "state": state,
    }))
}

fn node_platform_label() -> String {
    format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH)
}

fn open_url_capability(input_json: &Value) -> Result<String> {
    let url = input_json
        .get("url")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("desktop.open_url requires non-empty input_json.url"))?;
    let parsed = reqwest::Url::parse(url)
        .with_context(|| format!("desktop.open_url received invalid URL {url}"))?;
    match parsed.scheme() {
        "http" | "https" => {}
        other => anyhow::bail!("desktop.open_url only allows http/https URLs, got {other}"),
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        anyhow::bail!("desktop.open_url forbids embedded credentials");
    }
    if parsed.fragment().is_some() {
        anyhow::bail!("desktop.open_url forbids URL fragments");
    }
    webbrowser::open(parsed.as_str()).context("desktop.open_url failed to open browser target")?;
    Ok(parsed.to_string())
}

fn open_path_capability(input_json: &Value) -> Result<PathBuf> {
    let raw_path = input_json
        .get("path")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("desktop.open_path requires non-empty input_json.path"))?;
    let path = PathBuf::from(raw_path);
    if !path.is_absolute() {
        anyhow::bail!("desktop.open_path requires an absolute path");
    }
    let canonical = fs::canonicalize(path.as_path())
        .with_context(|| format!("desktop.open_path target {} was not found", path.display()))?;
    if !canonical.is_file() && !canonical.is_dir() {
        anyhow::bail!(
            "desktop.open_path only supports existing files or directories, got {}",
            canonical.display()
        );
    }

    #[cfg(target_os = "windows")]
    let mut command = {
        let mut command = Command::new("explorer");
        command.arg(canonical.as_os_str());
        command
    };
    #[cfg(target_os = "macos")]
    let mut command = {
        let mut command = Command::new("open");
        command.arg(canonical.as_os_str());
        command
    };
    #[cfg(all(unix, not(target_os = "macos")))]
    let mut command = {
        let mut command = Command::new("xdg-open");
        command.arg(canonical.as_os_str());
        command
    };

    let status = command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .context("desktop.open_path failed to start platform opener")?;
    if !status.success() {
        anyhow::bail!(
            "desktop.open_path platform opener exited unsuccessfully for {}",
            canonical.display()
        );
    }
    Ok(canonical)
}

fn load_node_client_material(config: &NodeHostConfig) -> Result<NodeClientMaterial> {
    let store = build_identity_store(Path::new(config.identity_store_dir.as_str()))?;
    let gateway_ca_certificate_pem =
        load_gateway_ca_certificate_pem(Some(config.identity_store_dir.clone()))?;
    let raw = store
        .read_secret(node_client_certificate_secret_key(config.device_id.as_str()).as_str())
        .map_err(anyhow::Error::from)
        .with_context(|| {
            format!(
                "failed to read node mTLS client certificate material for device {}",
                config.device_id
            )
        })?;
    let certificate = serde_json::from_slice::<StoredNodeClientCertificate>(raw.as_slice())
        .context("failed to parse stored node mTLS client certificate material")?;
    Ok(NodeClientMaterial { gateway_ca_certificate_pem, certificate })
}

fn store_node_client_material(
    store: &dyn SecretStore,
    device_id: &str,
    certificate: &StoredNodeClientCertificate,
    gateway_ca_certificate_pem: &str,
) -> Result<()> {
    let gateway_ca_state = serde_json::to_vec(&StoredGatewayCaState {
        certificate_pem: gateway_ca_certificate_pem.to_owned(),
    })
    .context("failed to encode gateway CA state for node host")?;
    store
        .write_secret(GATEWAY_CA_STATE_KEY, gateway_ca_state.as_slice())
        .map_err(anyhow::Error::from)
        .context("failed to persist gateway CA state for node host")?;
    let certificate_payload = serde_json::to_vec_pretty(certificate)
        .context("failed to encode node mTLS client certificate payload")?;
    store
        .write_secret(
            node_client_certificate_secret_key(device_id).as_str(),
            certificate_payload.as_slice(),
        )
        .map_err(anyhow::Error::from)
        .context("failed to persist node mTLS client certificate payload")
}

fn node_client_certificate_secret_key(device_id: &str) -> String {
    format!("device/{device_id}/{NODE_HOST_CERTIFICATE_SECRET_KEY_SUFFIX}")
}

fn node_host_state_dir(create: bool) -> Result<PathBuf> {
    let root_context = app::current_root_context()
        .ok_or_else(|| anyhow!("CLI root context is unavailable for node host commands"))?;
    let path = root_context.state_root().join(NODE_HOST_STATE_DIR);
    if create {
        fs::create_dir_all(path.as_path())
            .with_context(|| format!("failed to create node host state dir {}", path.display()))?;
    }
    Ok(path)
}

fn node_host_identity_store_dir() -> Result<PathBuf> {
    Ok(node_host_state_dir(true)?.join("identity"))
}

fn node_host_config_path() -> Result<PathBuf> {
    Ok(node_host_state_dir(true)?.join(NODE_HOST_CONFIG_FILE_NAME))
}

fn node_host_process_metadata_path() -> Result<PathBuf> {
    Ok(node_host_state_dir(true)?.join(NODE_HOST_PROCESS_FILE_NAME))
}

fn read_node_host_config() -> Result<Option<NodeHostConfig>> {
    let path = node_host_config_path()?;
    if !path.exists() {
        return Ok(None);
    }
    let payload = fs::read(path.as_path())
        .with_context(|| format!("failed to read node host config {}", path.display()))?;
    serde_json::from_slice::<NodeHostConfig>(payload.as_slice())
        .with_context(|| format!("failed to parse node host config {}", path.display()))
        .map(Some)
}

fn load_node_host_config_required() -> Result<NodeHostConfig> {
    read_node_host_config()?
        .ok_or_else(|| anyhow!("node host is not installed; run `palyra node install` first"))
}

fn write_node_host_config(config: &NodeHostConfig) -> Result<()> {
    let path = node_host_config_path()?;
    let payload =
        serde_json::to_vec_pretty(config).context("failed to encode node host config payload")?;
    write_file_atomically(path.as_path(), payload.as_slice())
        .with_context(|| format!("failed to write node host config {}", path.display()))
}

fn read_node_host_process_metadata() -> Result<Option<NodeHostProcessMetadata>> {
    let path = node_host_process_metadata_path()?;
    if !path.exists() {
        return Ok(None);
    }
    let payload = fs::read(path.as_path())
        .with_context(|| format!("failed to read node host process metadata {}", path.display()))?;
    serde_json::from_slice::<NodeHostProcessMetadata>(payload.as_slice())
        .with_context(|| format!("failed to parse node host process metadata {}", path.display()))
        .map(Some)
}

fn write_node_host_process_metadata(metadata: &NodeHostProcessMetadata) -> Result<()> {
    let path = node_host_process_metadata_path()?;
    let payload = serde_json::to_vec_pretty(metadata)
        .context("failed to encode node host process metadata payload")?;
    write_file_atomically(path.as_path(), payload.as_slice())
        .with_context(|| format!("failed to write node host process metadata {}", path.display()))
}

fn remove_node_host_process_metadata() -> Result<()> {
    let path = node_host_process_metadata_path()?;
    if path.exists() {
        fs::remove_file(path.as_path()).with_context(|| {
            format!("failed to remove node host process metadata {}", path.display())
        })?;
    }
    Ok(())
}

fn emit_node_lifecycle_payload(payload: NodeLifecyclePayload, json_output: bool) -> Result<()> {
    if json_output {
        return output::print_json_pretty(
            &payload,
            "failed to encode node host lifecycle payload as JSON",
        );
    }

    for line in render_node_lifecycle_text(&payload) {
        println!("{line}");
    }
    std::io::stdout().flush().context("stdout flush failed")
}

// INTENTIONAL: text mode emits only boolean presence flags. Device ids, URLs,
// filesystem paths, PIDs, and timestamps stay JSON-only (`--json`) so terminal
// logs cannot leak them; a unit test pins this redaction contract.
fn render_node_lifecycle_text(payload: &NodeLifecyclePayload) -> Vec<String> {
    let mut lines = vec![format!(
        "node.{} installed={} paired={} running={} device_configured={} grpc_configured={} identity_store_configured={} cert_present={} pid_present={} logs_present={}",
        payload.action,
        payload.installed,
        payload.paired,
        payload.running,
        payload.device_id.is_some(),
        payload.grpc_url.is_some(),
        payload.identity_store_dir.is_some(),
        payload.cert_expires_at_unix_ms.is_some(),
        payload.pid.is_some(),
        payload.stdout_log_path.is_some() || payload.stderr_log_path.is_some(),
    )];
    if payload.installed
        || payload.paired
        || payload.running
        || payload.device_id.is_some()
        || payload.grpc_url.is_some()
        || payload.identity_store_dir.is_some()
        || payload.cert_expires_at_unix_ms.is_some()
        || payload.pid.is_some()
        || payload.stdout_log_path.is_some()
        || payload.stderr_log_path.is_some()
        || !payload.detail.trim().is_empty()
    {
        lines.push(format!("node.{}.details=available via --json", payload.action));
    }
    lines
}

fn emit_node_run_payload(payload: &NodeRunPayload, json_output: bool) -> Result<()> {
    if json_output {
        return output::print_json_pretty(payload, "failed to encode node run payload as JSON");
    }

    println!(
        "node.{} device_id={} grpc_url={} poll_interval_ms={} paired={} capabilities={}",
        payload.action,
        payload.device_id,
        payload.grpc_url,
        payload.poll_interval_ms,
        payload.paired,
        payload.capability_count,
    );
    std::io::stdout().flush().context("stdout flush failed")
}

fn required_nonempty_text(value: String, label: &str) -> Result<String> {
    let value = value.trim().to_owned();
    if value.is_empty() {
        anyhow::bail!("{label} is missing from pairing response");
    }
    Ok(value)
}

fn process_is_running(pid: u32) -> bool {
    #[cfg(windows)]
    {
        Command::new("tasklist")
            .args(["/FI", &format!("PID eq {pid}"), "/FO", "CSV", "/NH"])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .output()
            .ok()
            .and_then(|output| String::from_utf8(output.stdout).ok())
            .is_some_and(|output| output.contains(&format!("\"{pid}\"")))
    }
    #[cfg(not(windows))]
    {
        Command::new("kill")
            .args(["-0", &pid.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|status| status.success())
    }
}

fn terminate_process(pid: u32) -> Result<()> {
    #[cfg(windows)]
    {
        let status = Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/T", "/F"])
            .status()
            .context("failed to execute taskkill")?;
        if !status.success() {
            anyhow::bail!("taskkill returned non-zero exit status for pid {pid}");
        }
        Ok(())
    }
    #[cfg(not(windows))]
    {
        let status = Command::new("kill")
            .args(["-TERM", &pid.to_string()])
            .status()
            .context("failed to execute kill")?;
        if !status.success() {
            anyhow::bail!("kill returned non-zero exit status for pid {pid}");
        }
        Ok(())
    }
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        admit_networked_worker_replay_at, build_capability_lifecycle_payload,
        capability_requires_local_mediation, next_reconnect_attempt, open_path_capability,
        open_url_capability, read_networked_worker_replay_registry_at, render_node_lifecycle_text,
        resolve_pairing_code_input, resolve_sibling_workerd_binary,
        settle_networked_worker_replay_at, NodeLifecyclePayload, NodeWorkerReplayState,
        NETWORKED_WORKER_DELIVERY_FENCE_CAPABILITY, NODE_HOST_RECONNECT_STABLE_MS,
    };
    use crate::proto::palyra::{common::v1 as common_v1, node::v1 as node_v1};

    #[test]
    fn node_lifecycle_text_redacts_sensitive_values() {
        let payload = NodeLifecyclePayload {
            action: "status".to_owned(),
            installed: true,
            paired: true,
            running: true,
            device_id: Some("device-secret".to_owned()),
            grpc_url: Some("https://gateway.example.test:7443".to_owned()),
            identity_store_dir: Some("/private/state/identity".to_owned()),
            cert_expires_at_unix_ms: Some(123456789),
            pid: Some(4242),
            stdout_log_path: Some("/private/logs/stdout.log".to_owned()),
            stderr_log_path: Some("/private/logs/stderr.log".to_owned()),
            detail: "sensitive internal detail".to_owned(),
        };

        let rendered = render_node_lifecycle_text(&payload).join("\n");

        assert!(
            !rendered.contains("device-secret"),
            "rendered output must not expose device id: {rendered}"
        );
        assert!(
            !rendered.contains("gateway.example.test"),
            "rendered output must not expose grpc url: {rendered}"
        );
        assert!(
            !rendered.contains("/private/"),
            "rendered output must not expose filesystem paths: {rendered}"
        );
        assert!(
            !rendered.contains("4242"),
            "rendered output must not expose process identifiers: {rendered}"
        );
        assert!(
            !rendered.contains("123456789"),
            "rendered output must not expose certificate timestamps: {rendered}"
        );
        assert!(
            !rendered.contains("sensitive internal detail"),
            "rendered output must not expose detail text: {rendered}"
        );
        assert!(
            rendered.contains("node.status.details=available via --json"),
            "rendered output should point operators to explicit detail mode: {rendered}"
        );
    }

    #[test]
    fn node_capability_contract_marks_local_mediation_capabilities() {
        assert!(capability_requires_local_mediation("desktop.open_url"));
        assert!(capability_requires_local_mediation("desktop.open_path"));
        assert!(!capability_requires_local_mediation("system.health"));
        assert!(!capability_requires_local_mediation("echo"));
        assert!(!capability_requires_local_mediation(NETWORKED_WORKER_DELIVERY_FENCE_CAPABILITY));
    }

    #[test]
    fn node_pairing_code_arg_requires_explicit_insecure_acknowledgement() {
        let error = resolve_pairing_code_input(Some("123456".to_owned()), false, false)
            .expect_err("argv pairing code must require explicit acknowledgement");

        assert!(
            error.to_string().contains("--allow-insecure-pairing-code-arg"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn node_pairing_code_arg_with_acknowledgement_is_normalized() {
        let code = resolve_pairing_code_input(Some(" 123456 \n".to_owned()), false, true)
            .expect("acknowledged argv pairing code should resolve");

        assert_eq!(code.as_deref(), Some("123456"));
    }

    #[test]
    fn capability_lifecycle_payload_carries_request_id_and_state() {
        let payload = build_capability_lifecycle_payload(
            &node_v1::NodeCapabilityDispatch {
                request_id: Some(common_v1::CanonicalId {
                    ulid: "01ARZ3NDEKTSV4RRFFQ69G5FAZ".to_owned(),
                }),
                capability: "desktop.open_url".to_owned(),
                input_json: Vec::new(),
                max_payload_bytes: 1024,
                networked_worker_reservation: None,
            },
            "awaiting_local_mediation",
        )
        .expect("payload should build");

        assert_eq!(
            payload.get("request_id").and_then(serde_json::Value::as_str),
            Some("01ARZ3NDEKTSV4RRFFQ69G5FAZ")
        );
        assert_eq!(
            payload.get("state").and_then(serde_json::Value::as_str),
            Some("awaiting_local_mediation")
        );
    }

    #[test]
    fn desktop_open_url_rejects_non_http_urls() {
        let error = open_url_capability(&json!({ "url": "file:///tmp/test.txt" }))
            .expect_err("non-http URL must be rejected");

        assert!(error.to_string().contains("only allows http/https"), "unexpected error: {error}");
    }

    #[test]
    fn desktop_open_path_requires_absolute_existing_paths() {
        let error = open_path_capability(&json!({ "path": "relative/file.txt" }))
            .expect_err("relative path must be rejected");

        assert!(
            error.to_string().contains("requires an absolute path"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn node_reconnect_budget_resets_after_a_stable_connection() {
        assert_eq!(
            next_reconnect_attempt(
                7,
                true,
                std::time::Duration::from_millis(NODE_HOST_RECONNECT_STABLE_MS),
            ),
            1
        );
        assert_eq!(next_reconnect_attempt(7, false, std::time::Duration::from_secs(120)), 8);
        assert_eq!(next_reconnect_attempt(7, true, std::time::Duration::from_secs(1)), 8);
    }

    #[test]
    fn networked_worker_uses_only_fixed_sibling_process() {
        let install = tempfile::tempdir().expect("install root");
        let cli = install.path().join(if cfg!(windows) { "palyra.exe" } else { "palyra" });
        let workerd = install.path().join(if cfg!(windows) {
            "palyra-workerd.exe"
        } else {
            "palyra-workerd"
        });
        std::fs::write(cli.as_path(), b"cli").expect("CLI fixture");
        std::fs::write(workerd.as_path(), b"worker").expect("workerd fixture");

        let resolved =
            resolve_sibling_workerd_binary(cli.as_path()).expect("fixed sibling should resolve");

        assert_eq!(resolved, workerd.canonicalize().expect("fixture should canonicalize"));
        std::fs::remove_file(workerd.as_path()).expect("remove worker fixture");
        assert!(
            resolve_sibling_workerd_binary(cli.as_path()).is_err(),
            "missing isolated worker must fail closed without an in-process fallback"
        );
    }

    #[test]
    fn networked_worker_replay_fence_survives_restart_without_payload_persistence() {
        let state = tempfile::tempdir().expect("state root");
        let path = state.path().join("worker-replay.json");
        let idempotency_key = "a".repeat(64);
        admit_networked_worker_replay_at(
            path.as_path(),
            idempotency_key.as_str(),
            "request-first",
            10,
            100,
        )
        .expect("first request should be admitted");

        let duplicate = admit_networked_worker_replay_at(
            path.as_path(),
            idempotency_key.as_str(),
            "request-retry",
            20,
            100,
        )
        .expect_err("reloaded registry must reject duplicate execution");
        assert!(duplicate.to_string().contains("requires reconciliation"));

        settle_networked_worker_replay_at(
            path.as_path(),
            idempotency_key.as_str(),
            "request-first",
            "b".repeat(64),
            30,
        )
        .expect("settlement should persist");
        let reloaded = read_networked_worker_replay_registry_at(path.as_path())
            .expect("registry should reload");
        let record = reloaded.records.get(idempotency_key.as_str()).expect("replay record");
        assert_eq!(record.state, NodeWorkerReplayState::Settled);
        assert_eq!(
            record.result_sha256.as_deref(),
            Some("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
        );

        let persisted = std::fs::read_to_string(path.as_path()).expect("persisted registry");
        assert!(!persisted.contains("input_json"));
        assert!(!persisted.contains("output_json"));
        assert!(!persisted.contains("fetch_token"));

        let unknown_key = "c".repeat(64);
        admit_networked_worker_replay_at(
            path.as_path(),
            unknown_key.as_str(),
            "request-crashed",
            40,
            50,
        )
        .expect("crash candidate should be admitted");
        let unknown = admit_networked_worker_replay_at(
            path.as_path(),
            unknown_key.as_str(),
            "request-after-restart",
            60,
            70,
        )
        .expect_err("expired in-flight execution must require explicit reconciliation");
        assert!(unknown.to_string().contains("state=outcome_unknown"));
        let reconciled = read_networked_worker_replay_registry_at(path.as_path())
            .expect("reconciled registry should reload");
        assert_eq!(
            reconciled.records.get(unknown_key.as_str()).map(|record| record.state),
            Some(NodeWorkerReplayState::OutcomeUnknown)
        );
    }
}
