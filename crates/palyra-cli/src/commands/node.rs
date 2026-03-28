use std::{
    fs::{self, File},
    io::Write,
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
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
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
const NODE_HOST_CERTIFICATE_SECRET_KEY_SUFFIX: &str = "node-mtls-client.json";
const DEFAULT_NODE_POLL_INTERVAL_MS: u64 = 1_000;
const NODE_HOST_START_POLL_MS: u64 = 750;

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
            poll_interval_ms,
            json,
        } => {
            let mut config = resolve_node_host_config(grpc_url, device_id, poll_interval_ms)?;
            ensure_node_pairing_material(&mut config, method, pairing_code, gateway_ca_file)
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
            start,
            json,
        } => {
            let mut config =
                resolve_node_host_config(grpc_url, device_id, Some(DEFAULT_NODE_POLL_INTERVAL_MS))?;
            ensure_node_pairing_material(&mut config, method, pairing_code, gateway_ca_file)
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
    let client_material = load_node_client_material(config)?;
    let mut client = connect_node_service(
        config.grpc_url.as_str(),
        client_material.gateway_ca_certificate_pem.as_str(),
        Some(&client_material.certificate),
    )
    .await?;

    let capabilities = supported_capabilities()
        .into_iter()
        .map(|name| node_v1::DeviceCapability { name: name.to_owned(), available: true })
        .collect::<Vec<_>>();
    let response = client
        .register_node(Request::new(node_v1::RegisterNodeRequest {
            v: RUN_STREAM_REQUEST_VERSION,
            device_id: Some(canonical_id(config.device_id.as_str())),
            platform: node_platform_label(),
            capabilities: capabilities.clone(),
            replay: None,
        }))
        .await
        .context("failed to register node host")?
        .into_inner();
    if !response.accepted {
        anyhow::bail!("node registration failed: {}", response.reason);
    }

    emit_node_run_payload(
        &NodeRunPayload {
            action: "run",
            device_id: config.device_id.clone(),
            grpc_url: config.grpc_url.clone(),
            poll_interval_ms: config.poll_interval_ms,
            paired: true,
            capability_count: capabilities.len(),
        },
        json_output,
    )?;

    let (sender, receiver) = mpsc::channel::<node_v1::NodeEventRequest>(16);
    sender
        .send(build_node_event_request(
            config.device_id.as_str(),
            "node.started",
            json!({
                "device_id": config.device_id,
                "platform": node_platform_label(),
                "capabilities": supported_capabilities(),
                "started_at_unix_ms": now_unix_ms(),
            }),
        )?)
        .await
        .context("failed to queue node startup event")?;
    let mut responses = client
        .stream_node_events(Request::new(ReceiverStream::new(receiver)))
        .await
        .context("failed to open node event stream")?
        .into_inner();

    loop {
        tokio::select! {
            message = responses.message() => {
                let Some(message) = message.context("failed to receive node event stream message")? else {
                    anyhow::bail!("node event stream closed unexpectedly");
                };
                if let Some(dispatch) = message.dispatch {
                    let result_payload = execute_dispatched_capability(&dispatch, config, &device)?;
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
            _ = sleep(Duration::from_millis(config.poll_interval_ms.max(100))) => {
                sender
                    .send(build_node_event_request(
                        config.device_id.as_str(),
                        "node.heartbeat",
                        json!({
                            "device_id": config.device_id,
                            "heartbeat_at_unix_ms": now_unix_ms(),
                        }),
                    )?)
                    .await
                    .context("failed to send node heartbeat")?;
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
    gateway_ca_file: Option<String>,
) -> Result<()> {
    if load_node_client_material(config).is_ok() {
        return Ok(());
    }

    let method = method.ok_or_else(|| {
        anyhow!("node pairing bootstrap requires --method when local pairing material is absent")
    })?;
    let pairing_code = pairing_code
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            anyhow!("node pairing bootstrap requires --pairing-code when local pairing material is absent")
        })?;
    let gateway_ca_file = gateway_ca_file
        .map(PathBuf::from)
        .ok_or_else(|| anyhow!("node pairing bootstrap requires --gateway-ca-file"))?;
    let gateway_ca_certificate_pem =
        fs::read_to_string(gateway_ca_file.as_path()).with_context(|| {
            format!("failed to read gateway CA certificate file {}", gateway_ca_file.display())
        })?;

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
        device_id: Ulid::new().to_string(),
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

fn execute_dispatched_capability(
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

fn supported_capabilities() -> [&'static str; 3] {
    ["echo", "system.health", "system.identity"]
}

fn node_platform_label() -> String {
    format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH)
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

    println!(
        "node.{} installed={} paired={} running={} device_id={} grpc_url={} pid={} cert_expires_at_unix_ms={} detail={}",
        payload.action,
        payload.installed,
        payload.paired,
        payload.running,
        option_text(payload.device_id.as_deref()),
        option_text(payload.grpc_url.as_deref()),
        payload
            .pid
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".to_owned()),
        payload
            .cert_expires_at_unix_ms
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".to_owned()),
        payload.detail,
    );
    if let Some(stdout_log_path) = payload.stdout_log_path.as_deref() {
        println!("node.{}.stdout_log_path={stdout_log_path}", payload.action);
    }
    if let Some(stderr_log_path) = payload.stderr_log_path.as_deref() {
        println!("node.{}.stderr_log_path={stderr_log_path}", payload.action);
    }
    std::io::stdout().flush().context("stdout flush failed")
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

fn option_text(value: Option<&str>) -> &str {
    value.filter(|inner| !inner.trim().is_empty()).unwrap_or("none")
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
