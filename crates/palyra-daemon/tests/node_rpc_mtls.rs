use std::{
    io::{BufRead, BufReader},
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{Child, ChildStdout, Command, Stdio},
    sync::{mpsc, Arc},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use palyra_identity::{
    DeviceIdentity, FilesystemSecretStore, IdentityManager, PairingClientKind, PairingMethod,
    SecretStore,
};
use reqwest::Client as HttpClient;
use tempfile::TempDir;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
use tonic::Code;

const ADMIN_TOKEN: &str = "test-admin-token";
const DEVICE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
const PAIRING_CODE: &str = "123456";

pub mod proto {
    pub mod palyra {
        pub mod common {
            pub mod v1 {
                tonic::include_proto!("palyra.common.v1");
            }
        }

        pub mod node {
            pub mod v1 {
                tonic::include_proto!("palyra.node.v1");
            }
        }
    }
}

use proto::palyra::{common::v1 as common_v1, node::v1 as node_v1};

#[tokio::test(flavor = "multi_thread")]
async fn node_rpc_mtls_rejects_clients_without_certificate() -> Result<()> {
    let identity = prepare_identity_store(false)?;
    let (child, admin_port, node_rpc_port) =
        spawn_palyrad_with_dynamic_ports(identity.store_dir())?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut()).await?;

    let connect_result = connect_node_client(node_rpc_port, identity.gateway_ca_pem(), None).await;
    let mut client = match connect_result {
        Ok(client) => client,
        Err(_) => return Ok(()),
    };
    let response = client.register_node(tonic::Request::new(sample_register_node_request())).await;
    let status = response.expect_err("request without client certificate must fail");
    assert!(
        status.code() == Code::Unauthenticated
            || status.code() == Code::PermissionDenied
            || status.code() == Code::Unavailable
            || status.code() == Code::Cancelled
            || status.code() == Code::Unknown,
        "unexpected status code for missing certificate: {:?}",
        status.code()
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn node_rpc_mtls_accepts_valid_client_certificate() -> Result<()> {
    let identity = prepare_identity_store(false)?;
    let (child, admin_port, node_rpc_port) =
        spawn_palyrad_with_dynamic_ports(identity.store_dir())?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut()).await?;

    let identity_tls =
        Identity::from_pem(identity.device_certificate_pem(), identity.device_private_key_pem());
    let mut client =
        connect_node_client(node_rpc_port, identity.gateway_ca_pem(), Some(identity_tls)).await?;
    let response = client.register_node(tonic::Request::new(sample_register_node_request())).await;
    let status = response.expect_err("stub node RPC service should currently return unimplemented");
    assert_eq!(
        status.code(),
        Code::Unimplemented,
        "valid mTLS client should reach node RPC service implementation"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn node_rpc_mtls_rejects_revoked_client_certificate() -> Result<()> {
    let identity = prepare_identity_store(true)?;
    let (child, admin_port, node_rpc_port) =
        spawn_palyrad_with_dynamic_ports(identity.store_dir())?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut()).await?;

    let revoked_identity =
        Identity::from_pem(identity.device_certificate_pem(), identity.device_private_key_pem());
    let connect_result =
        connect_node_client(node_rpc_port, identity.gateway_ca_pem(), Some(revoked_identity)).await;
    let mut client = match connect_result {
        Ok(client) => client,
        Err(_) => return Ok(()),
    };
    let response = client.register_node(tonic::Request::new(sample_register_node_request())).await;
    let status = response.expect_err("revoked client certificate must be rejected");
    assert_eq!(
        status.code(),
        Code::PermissionDenied,
        "revoked client certificate should be denied by node RPC verifier"
    );
    Ok(())
}

async fn connect_node_client(
    node_rpc_port: u16,
    gateway_ca_pem: &str,
    identity: Option<Identity>,
) -> Result<node_v1::node_service_client::NodeServiceClient<Channel>> {
    let mut tls_config = ClientTlsConfig::new()
        .domain_name("palyrad-node-rpc")
        .ca_certificate(Certificate::from_pem(gateway_ca_pem));
    if let Some(identity) = identity {
        tls_config = tls_config.identity(identity);
    }
    let endpoint = Endpoint::from_shared(format!("https://127.0.0.1:{node_rpc_port}"))
        .context("failed to construct node RPC endpoint")?
        .tls_config(tls_config)
        .context("failed to configure node RPC TLS client settings")?;
    let channel = endpoint.connect().await.context("failed to connect node RPC endpoint")?;
    Ok(node_v1::node_service_client::NodeServiceClient::new(channel))
}

fn sample_register_node_request() -> node_v1::RegisterNodeRequest {
    node_v1::RegisterNodeRequest {
        v: 1,
        device_id: Some(common_v1::CanonicalId { ulid: DEVICE_ID.to_owned() }),
        platform: "test-platform".to_owned(),
        capabilities: Vec::new(),
        replay: None,
    }
}

struct PreparedIdentityStore {
    root: TempDir,
    gateway_ca_pem: String,
    device_certificate_pem: String,
    device_private_key_pem: String,
}

impl PreparedIdentityStore {
    fn store_dir(&self) -> &Path {
        self.root.path()
    }

    fn gateway_ca_pem(&self) -> &str {
        &self.gateway_ca_pem
    }

    fn device_certificate_pem(&self) -> &str {
        &self.device_certificate_pem
    }

    fn device_private_key_pem(&self) -> &str {
        &self.device_private_key_pem
    }
}

fn prepare_identity_store(revoke_after_pairing: bool) -> Result<PreparedIdentityStore> {
    let root = TempDir::new().context("failed to create identity store root")?;
    let store = FilesystemSecretStore::new(root.path()).with_context(|| {
        format!("failed to initialize filesystem identity store at {}", root.path().display())
    })?;
    let store: Arc<dyn SecretStore> = Arc::new(store);
    let mut manager =
        IdentityManager::with_store(store).context("failed to initialize identity manager")?;
    let device =
        DeviceIdentity::generate(DEVICE_ID).context("failed to generate device identity")?;

    let session = manager
        .start_pairing(
            PairingClientKind::Node,
            PairingMethod::Pin { code: PAIRING_CODE.to_owned() },
            SystemTime::now(),
        )
        .context("failed to start pairing session")?;
    let hello = manager
        .build_device_hello(&session, &device, PAIRING_CODE)
        .context("failed to build device hello")?;
    let pairing = manager
        .complete_pairing(hello, SystemTime::now())
        .context("failed to complete pairing session")?;

    if revoke_after_pairing {
        manager
            .revoke_device(DEVICE_ID, "revoked for node RPC integration test", SystemTime::now())
            .context("failed to revoke paired device")?;
    }

    Ok(PreparedIdentityStore {
        root,
        gateway_ca_pem: pairing.gateway_ca_certificate_pem,
        device_certificate_pem: pairing.device.current_certificate.certificate_pem,
        device_private_key_pem: pairing.device.current_certificate.private_key_pem,
    })
}

fn spawn_palyrad_with_dynamic_ports(identity_store_dir: &Path) -> Result<(Child, u16, u16)> {
    let journal_db_path = unique_temp_journal_db_path();
    let mut child = Command::new(env!("CARGO_BIN_EXE_palyrad"))
        .args([
            "--bind",
            "127.0.0.1",
            "--port",
            "0",
            "--grpc-bind",
            "127.0.0.1",
            "--grpc-port",
            "0",
        ])
        .env("PALYRA_ADMIN_TOKEN", ADMIN_TOKEN)
        .env("PALYRA_JOURNAL_DB_PATH", journal_db_path.to_string_lossy().to_string())
        .env("PALYRA_GATEWAY_IDENTITY_STORE_DIR", identity_store_dir.to_string_lossy().to_string())
        .env("RUST_LOG", "info")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .context("failed to start palyrad")?;
    let stdout = child.stdout.take().context("failed to capture palyrad stdout")?;
    let (admin_port, node_rpc_port) = wait_for_admin_and_node_rpc_ports(stdout, &mut child)?;
    Ok((child, admin_port, node_rpc_port))
}

fn wait_for_admin_and_node_rpc_ports(
    stdout: ChildStdout,
    daemon: &mut Child,
) -> Result<(u16, u16)> {
    let (sender, receiver) = mpsc::channel::<Result<(u16, u16), String>>();
    thread::spawn(move || {
        let mut sender = Some(sender);
        let mut admin_port = None::<u16>;
        let mut node_rpc_port = None::<u16>;
        for line in BufReader::new(stdout).lines() {
            let Ok(line) = line else {
                if let Some(sender) = sender.take() {
                    let _ = sender.send(Err("failed to read palyrad stdout line".to_owned()));
                }
                return;
            };
            if admin_port.is_none() {
                admin_port = parse_port_from_log(&line, "\"listen_addr\":\"");
            }
            if node_rpc_port.is_none() {
                node_rpc_port = parse_port_from_log(&line, "\"node_rpc_listen_addr\":\"");
            }
            if let (Some(admin_port), Some(node_rpc_port)) = (admin_port, node_rpc_port) {
                if let Some(sender) = sender.take() {
                    let _ = sender.send(Ok((admin_port, node_rpc_port)));
                }
                return;
            }
        }

        if let Some(sender) = sender.take() {
            let _ = sender.send(Err(
                "palyrad stdout closed before admin/node RPC listen addresses were published"
                    .to_owned(),
            ));
        }
    });

    let timeout_at = Instant::now() + Duration::from_secs(15);
    loop {
        match receiver.recv_timeout(Duration::from_millis(100)) {
            Ok(Ok(ports)) => return Ok(ports),
            Ok(Err(message)) => anyhow::bail!("{message}"),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                anyhow::bail!("listen-address reader disconnected before publishing ports");
            }
        }
        if Instant::now() > timeout_at {
            anyhow::bail!("timed out waiting for palyrad listen address logs");
        }
        if let Some(status) = daemon.try_wait().context("failed to check palyrad status")? {
            anyhow::bail!(
                "palyrad exited before publishing listen addresses with status: {status}"
            );
        }
    }
}

fn parse_port_from_log(line: &str, prefix: &str) -> Option<u16> {
    let start = line.find(prefix)? + prefix.len();
    let rest = &line[start..];
    let end = rest.find('"')?;
    rest[..end].parse::<SocketAddr>().ok().map(|address| address.port())
}

async fn wait_for_health(port: u16, daemon: &mut Child) -> Result<()> {
    let timeout_at = Instant::now() + Duration::from_secs(15);
    let url = format!("http://127.0.0.1:{port}/healthz");
    let client = HttpClient::builder()
        .timeout(Duration::from_millis(300))
        .build()
        .context("failed to build HTTP client")?;

    loop {
        if Instant::now() > timeout_at {
            anyhow::bail!("timed out waiting for palyrad health endpoint");
        }
        if let Some(status) = daemon.try_wait().context("failed to check palyrad status")? {
            anyhow::bail!("palyrad exited before becoming healthy with status: {status}");
        }
        if client.get(&url).send().await.and_then(|response| response.error_for_status()).is_ok() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn unique_temp_journal_db_path() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    std::env::temp_dir()
        .join(format!("palyra-node-rpc-mtls-{nonce}-{}.sqlite3", std::process::id()))
}

struct ChildGuard {
    child: Child,
}

impl ChildGuard {
    fn new(child: Child) -> Self {
        Self { child }
    }

    fn child_mut(&mut self) -> &mut Child {
        &mut self.child
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}
