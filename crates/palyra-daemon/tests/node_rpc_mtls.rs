//! mTLS posture tests for the node RPC listener of a spawned `palyrad`:
//! clients without or with revoked certificates are rejected, valid
//! certificates are accepted, and the insecure opt-out is honored.

use std::{
    fs,
    io::{BufRead, BufReader},
    net::SocketAddr,
    path::Path,
    process::{Child, ChildStdout, Command, Stdio},
    sync::{mpsc, Arc},
    thread,
    time::{Duration, Instant, SystemTime},
};

use anyhow::{Context, Result};
use palyra_identity::{
    DeviceIdentity, FilesystemSecretStore, IdentityManager, PairingClientKind, PairingMethod,
    SecretStore,
};
use reqwest::Client as HttpClient;
use tempfile::TempDir;
use tokio::sync::mpsc as tokio_mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
use tonic::Code;

const ADMIN_TOKEN: &str = "test-admin-token";
const DEVICE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
const OTHER_DEVICE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAW";
const REQUEST_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAX";
const DELIVERY_ATTEMPT_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAY";
const PAIRING_CODE: &str = "123456";
const FETCH_TOKEN: &str = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ-_";

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
    let (child, admin_port, node_rpc_port, _runtime_root) =
        spawn_palyrad_with_dynamic_ports(identity.store_dir(), false)?;
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
async fn node_rpc_mtls_rejects_worker_payload_fetch_and_ack_without_certificate() -> Result<()> {
    let identity = prepare_identity_store(false)?;
    let (child, admin_port, node_rpc_port, _runtime_root) =
        spawn_palyrad_with_dynamic_ports(identity.store_dir(), false)?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut()).await?;

    assert_missing_certificate_denial(
        connect_node_client(node_rpc_port, identity.gateway_ca_pem(), None).await,
        |mut client| async move {
            client
                .fetch_networked_worker_payload(tonic::Request::new(sample_fetch_payload_request(
                    DEVICE_ID,
                )))
                .await
                .map(|_| ())
        },
        "worker payload fetch",
    )
    .await?;
    assert_missing_certificate_denial(
        connect_node_client(node_rpc_port, identity.gateway_ca_pem(), None).await,
        |mut client| async move {
            client
                .acknowledge_networked_worker_payload(tonic::Request::new(
                    sample_acknowledge_payload_request(DEVICE_ID),
                ))
                .await
                .map(|_| ())
        },
        "worker payload acknowledgement",
    )
    .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn node_rpc_mtls_accepts_valid_client_certificate() -> Result<()> {
    let identity = prepare_identity_store(false)?;
    let (child, admin_port, node_rpc_port, _runtime_root) =
        spawn_palyrad_with_dynamic_ports(identity.store_dir(), false)?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut()).await?;

    let identity_tls =
        Identity::from_pem(identity.device_certificate_pem(), identity.device_private_key_pem());
    let mut client =
        connect_node_client(node_rpc_port, identity.gateway_ca_pem(), Some(identity_tls)).await?;
    let response = client
        .register_node(tonic::Request::new(sample_register_node_request()))
        .await
        .context("valid mTLS client should reach node RPC service implementation")?
        .into_inner();
    assert!(response.accepted, "valid mTLS client should be accepted");
    assert_eq!(response.reason, "registered");
    assert_eq!(response.device_id.as_ref().map(|value| value.ulid.as_str()), Some(DEVICE_ID));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn node_rpc_mtls_rejects_forged_capability_request_ownership() -> Result<()> {
    let first_identity = prepare_identity_store(false)?;
    let second_identity = add_paired_device(first_identity.store_dir(), OTHER_DEVICE_ID)?;
    let (child, admin_port, node_rpc_port, _runtime_root) =
        spawn_palyrad_with_dynamic_ports(first_identity.store_dir(), false)?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut()).await?;

    let first_tls = Identity::from_pem(
        first_identity.device_certificate_pem(),
        first_identity.device_private_key_pem(),
    );
    let second_tls = Identity::from_pem(
        second_identity.certificate_pem.as_str(),
        second_identity.private_key_pem.as_str(),
    );
    let mut first_client = connect_node_client(
        node_rpc_port,
        first_identity.gateway_ca_pem(),
        Some(first_tls.clone()),
    )
    .await?;
    let mut second_client =
        connect_node_client(node_rpc_port, first_identity.gateway_ca_pem(), Some(second_tls))
            .await?;
    first_client
        .register_node(tonic::Request::new(sample_register_node_request_for(DEVICE_ID)))
        .await
        .context("first node should register")?;
    second_client
        .register_node(tonic::Request::new(sample_register_node_request_for(OTHER_DEVICE_ID)))
        .await
        .context("second node should register")?;

    let (first_event_sender, first_event_receiver) = tokio_mpsc::channel(4);
    let mut first_events = first_client
        .stream_node_events(tonic::Request::new(ReceiverStream::new(first_event_receiver)))
        .await
        .context("first node event stream should open")?
        .into_inner();
    let mut execute_client =
        connect_node_client(node_rpc_port, first_identity.gateway_ca_pem(), Some(first_tls))
            .await?;
    let execute_task = tokio::spawn(async move {
        execute_client
            .execute_capability(tonic::Request::new(node_v1::ExecuteCapabilityRequest {
                v: 1,
                device_id: Some(common_v1::CanonicalId { ulid: DEVICE_ID.to_owned() }),
                capability: "system.health".to_owned(),
                input_json: br#"{"ok":true}"#.to_vec(),
                max_payload_bytes: 4096,
            }))
            .await
    });
    first_event_sender
        .send(node_event_request(DEVICE_ID, "heartbeat", serde_json::json!({})))
        .await
        .context("first node heartbeat should send")?;
    let dispatch = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let response =
                first_events.next().await.context("first node stream closed before dispatch")??;
            if let Some(dispatch) = response.dispatch {
                break Ok::<_, anyhow::Error>(dispatch);
            }
            first_event_sender
                .send(node_event_request(DEVICE_ID, "heartbeat", serde_json::json!({})))
                .await
                .context("first node heartbeat should send")?;
        }
    })
    .await
    .context("timed out waiting for capability dispatch")??;
    let request_id = dispatch
        .request_id
        .as_ref()
        .map(|value| value.ulid.clone())
        .context("capability dispatch should include request id")?;

    let (forged_sender, forged_receiver) = tokio_mpsc::channel(1);
    let mut forged_events = second_client
        .stream_node_events(tonic::Request::new(ReceiverStream::new(forged_receiver)))
        .await
        .context("second node event stream should open")?
        .into_inner();
    forged_sender
        .send(node_event_request(
            OTHER_DEVICE_ID,
            "capability.result",
            serde_json::json!({
                "request_id": request_id,
                "success": true,
                "output_json": {"status": "forged"},
                "error": ""
            }),
        ))
        .await
        .context("forged capability result should send")?;
    let forged_status = forged_events
        .next()
        .await
        .context("forged stream should return a denial")?
        .expect_err("forged result must be denied");
    assert_eq!(forged_status.code(), Code::PermissionDenied);

    first_event_sender
        .send(node_event_request(
            DEVICE_ID,
            "capability.result",
            serde_json::json!({
                "request_id": request_id,
                "success": true,
                "output_json": {"status": "ok"},
                "error": ""
            }),
        ))
        .await
        .context("owned capability result should send")?;
    let accepted =
        first_events.next().await.context("owner stream should acknowledge result")??;
    assert!(accepted.accepted);
    let execute_response = tokio::time::timeout(Duration::from_secs(5), execute_task)
        .await
        .context("timed out waiting for execute capability response")?
        .context("execute capability task should join")?
        .context("owner result should complete capability execution")?
        .into_inner();
    assert!(execute_response.success);
    assert_eq!(execute_response.output_json, br#"{"status":"ok"}"#);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn node_rpc_mtls_reports_result_owner_loss_without_accepting_result() -> Result<()> {
    let identity = prepare_identity_store(false)?;
    let runtime_root = TempDir::new().context("failed to create node RPC runtime root")?;
    let (child, admin_port, node_rpc_port) =
        spawn_palyrad_at_runtime_root(identity.store_dir(), false, runtime_root.path())?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut()).await?;

    let identity_tls =
        Identity::from_pem(identity.device_certificate_pem(), identity.device_private_key_pem());
    let mut node_client =
        connect_node_client(node_rpc_port, identity.gateway_ca_pem(), Some(identity_tls.clone()))
            .await?;
    node_client
        .register_node(tonic::Request::new(sample_register_node_request()))
        .await
        .context("node should register")?;
    let (event_sender, event_receiver) = tokio_mpsc::channel(4);
    let mut events = node_client
        .stream_node_events(tonic::Request::new(ReceiverStream::new(event_receiver)))
        .await
        .context("node event stream should open")?
        .into_inner();
    let mut execute_client =
        connect_node_client(node_rpc_port, identity.gateway_ca_pem(), Some(identity_tls.clone()))
            .await?;
    let execute_task = tokio::spawn(async move {
        execute_client
            .execute_capability(tonic::Request::new(node_v1::ExecuteCapabilityRequest {
                v: 1,
                device_id: Some(common_v1::CanonicalId { ulid: DEVICE_ID.to_owned() }),
                capability: "system.health".to_owned(),
                input_json: br#"{"ok":true}"#.to_vec(),
                max_payload_bytes: 4096,
            }))
            .await
    });
    event_sender
        .send(node_event_request(DEVICE_ID, "heartbeat", serde_json::json!({})))
        .await
        .context("node heartbeat should send")?;
    let dispatch = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let response = events.next().await.context("node stream closed before dispatch")??;
            if let Some(dispatch) = response.dispatch {
                break Ok::<_, anyhow::Error>(dispatch);
            }
            event_sender
                .send(node_event_request(DEVICE_ID, "heartbeat", serde_json::json!({})))
                .await
                .context("node heartbeat should send")?;
        }
    })
    .await
    .context("timed out waiting for capability dispatch")??;
    let request_id = dispatch
        .request_id
        .as_ref()
        .map(|value| value.ulid.clone())
        .context("capability dispatch should include request id")?;

    daemon.stop()?;
    let state_path = runtime_root.path().join("state").join("node-runtime.v1.json");
    let state_bytes = fs::read(state_path.as_path())
        .with_context(|| format!("failed to read node runtime state {}", state_path.display()))?;
    let state_json: serde_json::Value = serde_json::from_slice(state_bytes.as_slice())
        .context("node runtime state should parse")?;
    assert_eq!(
        state_json
            .pointer(format!("/capability_requests/{request_id}/state").as_str())
            .and_then(serde_json::Value::as_str),
        Some("dispatched")
    );

    let (child, restarted_admin_port, restarted_node_rpc_port) =
        spawn_palyrad_at_runtime_root(identity.store_dir(), false, runtime_root.path())?;
    daemon = ChildGuard::new(child);
    wait_for_health(restarted_admin_port, daemon.child_mut()).await?;
    let mut restarted_node_client =
        connect_node_client(restarted_node_rpc_port, identity.gateway_ca_pem(), Some(identity_tls))
            .await?;
    restarted_node_client
        .register_node(tonic::Request::new(sample_register_node_request()))
        .await
        .context("node should register after restart")?;
    let (restarted_sender, restarted_receiver) = tokio_mpsc::channel(1);
    let mut restarted_events = restarted_node_client
        .stream_node_events(tonic::Request::new(ReceiverStream::new(restarted_receiver)))
        .await
        .context("restarted node event stream should open")?
        .into_inner();
    restarted_sender
        .send(node_event_request(
            DEVICE_ID,
            "capability.result",
            serde_json::json!({
                "request_id": request_id,
                "success": true,
                "output_json": {"status": "late-after-restart"},
                "error": ""
            }),
        ))
        .await
        .context("late result should send after restart")?;
    let response =
        restarted_events.next().await.context("restarted stream should answer late result")??;
    assert!(!response.accepted);
    assert_eq!(response.reason, "capability_result_owner_unavailable");

    drop(execute_task);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn node_rpc_mtls_rejects_worker_payload_fetch_and_ack_for_wrong_device() -> Result<()> {
    let first_identity = prepare_identity_store(false)?;
    let second_identity = add_paired_device(first_identity.store_dir(), OTHER_DEVICE_ID)?;
    let (child, admin_port, node_rpc_port, _runtime_root) =
        spawn_palyrad_with_dynamic_ports(first_identity.store_dir(), false)?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut()).await?;

    let second_tls = Identity::from_pem(
        second_identity.certificate_pem.as_str(),
        second_identity.private_key_pem.as_str(),
    );
    let mut fetch_client = connect_node_client(
        node_rpc_port,
        first_identity.gateway_ca_pem(),
        Some(second_tls.clone()),
    )
    .await?;
    let fetch_status = fetch_client
        .fetch_networked_worker_payload(tonic::Request::new(sample_fetch_payload_request(
            DEVICE_ID,
        )))
        .await
        .expect_err("certificate for another device must not fetch worker payload");
    assert_eq!(fetch_status.code(), Code::PermissionDenied);

    let mut ack_client =
        connect_node_client(node_rpc_port, first_identity.gateway_ca_pem(), Some(second_tls))
            .await?;
    let ack_status = ack_client
        .acknowledge_networked_worker_payload(tonic::Request::new(
            sample_acknowledge_payload_request(DEVICE_ID),
        ))
        .await
        .expect_err("certificate for another device must not acknowledge worker payload");
    assert_eq!(ack_status.code(), Code::PermissionDenied);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn node_rpc_mtls_rejects_worker_payload_calls_on_existing_channel_after_revocation(
) -> Result<()> {
    let identity = prepare_identity_store(false)?;
    let (child, admin_port, node_rpc_port, _runtime_root) =
        spawn_palyrad_with_dynamic_ports(identity.store_dir(), false)?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut()).await?;

    let identity_tls =
        Identity::from_pem(identity.device_certificate_pem(), identity.device_private_key_pem());
    let mut client =
        connect_node_client(node_rpc_port, identity.gateway_ca_pem(), Some(identity_tls)).await?;
    client
        .register_node(tonic::Request::new(sample_register_node_request()))
        .await
        .context("valid mTLS client should register before revocation")?;
    let revoked = client
        .revoke_device_pairing(tonic::Request::new(node_v1::RevokeDevicePairingRequest {
            v: 1,
            device_id: Some(common_v1::CanonicalId { ulid: DEVICE_ID.to_owned() }),
            reason: "test dynamic revocation".to_owned(),
            replay: None,
        }))
        .await
        .context("paired client should revoke its own device")?
        .into_inner();
    assert!(revoked.revoked);

    let fetch_status = client
        .fetch_networked_worker_payload(tonic::Request::new(sample_fetch_payload_request(
            DEVICE_ID,
        )))
        .await
        .expect_err("revoked certificate on an existing channel must not fetch worker payload");
    assert_eq!(fetch_status.code(), Code::PermissionDenied);
    let ack_status = client
        .acknowledge_networked_worker_payload(tonic::Request::new(
            sample_acknowledge_payload_request(DEVICE_ID),
        ))
        .await
        .expect_err(
            "revoked certificate on an existing channel must not acknowledge worker payload",
        );
    assert_eq!(ack_status.code(), Code::PermissionDenied);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn node_rpc_mtls_rejects_revoked_client_certificate() -> Result<()> {
    let identity = prepare_identity_store(true)?;
    let (child, admin_port, node_rpc_port, _runtime_root) =
        spawn_palyrad_with_dynamic_ports(identity.store_dir(), false)?;
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

#[tokio::test(flavor = "multi_thread")]
async fn node_rpc_mtls_rejects_worker_payload_fetch_and_ack_for_revoked_certificate() -> Result<()>
{
    let identity = prepare_identity_store(true)?;
    let (child, admin_port, node_rpc_port, _runtime_root) =
        spawn_palyrad_with_dynamic_ports(identity.store_dir(), false)?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut()).await?;

    let revoked_identity =
        Identity::from_pem(identity.device_certificate_pem(), identity.device_private_key_pem());
    let fetch_result = connect_node_client(
        node_rpc_port,
        identity.gateway_ca_pem(),
        Some(revoked_identity.clone()),
    )
    .await;
    if let Ok(mut client) = fetch_result {
        let status = client
            .fetch_networked_worker_payload(tonic::Request::new(sample_fetch_payload_request(
                DEVICE_ID,
            )))
            .await
            .expect_err("revoked certificate must not fetch worker payload");
        assert_eq!(status.code(), Code::PermissionDenied);
    }

    let ack_result =
        connect_node_client(node_rpc_port, identity.gateway_ca_pem(), Some(revoked_identity)).await;
    if let Ok(mut client) = ack_result {
        let status = client
            .acknowledge_networked_worker_payload(tonic::Request::new(
                sample_acknowledge_payload_request(DEVICE_ID),
            ))
            .await
            .expect_err("revoked certificate must not acknowledge worker payload");
        assert_eq!(status.code(), Code::PermissionDenied);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn node_rpc_insecure_opt_out_accepts_clients_without_certificate() -> Result<()> {
    let identity = prepare_identity_store(false)?;
    let (child, admin_port, node_rpc_port, _runtime_root) =
        spawn_palyrad_with_dynamic_ports(identity.store_dir(), true)?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut()).await?;

    let mut client = connect_node_client(node_rpc_port, identity.gateway_ca_pem(), None).await?;
    let response = client
        .register_node(tonic::Request::new(sample_register_node_request()))
        .await
        .context("without mTLS enforcement, unauthenticated client should reach node RPC handler")?
        .into_inner();
    assert!(response.accepted, "opt-out mode should accept register_node");
    assert_eq!(response.reason, "registered");
    assert_eq!(response.device_id.as_ref().map(|value| value.ulid.as_str()), Some(DEVICE_ID));
    Ok(())
}

async fn assert_missing_certificate_denial<F, Fut>(
    connect_result: Result<node_v1::node_service_client::NodeServiceClient<Channel>>,
    invoke: F,
    operation: &str,
) -> Result<()>
where
    F: FnOnce(node_v1::node_service_client::NodeServiceClient<Channel>) -> Fut,
    Fut: std::future::Future<Output = Result<(), tonic::Status>>,
{
    let client = match connect_result {
        Ok(client) => client,
        Err(_) => return Ok(()),
    };
    let status = invoke(client).await.expect_err("request without client certificate must fail");
    assert!(
        matches!(
            status.code(),
            Code::Unauthenticated
                | Code::PermissionDenied
                | Code::Unavailable
                | Code::Cancelled
                | Code::Unknown
        ),
        "unexpected status code for missing certificate on {operation}: {:?}",
        status.code()
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

fn sample_fetch_payload_request(device_id: &str) -> node_v1::FetchNetworkedWorkerPayloadRequest {
    node_v1::FetchNetworkedWorkerPayloadRequest {
        v: 1,
        device_id: Some(common_v1::CanonicalId { ulid: device_id.to_owned() }),
        request_id: Some(common_v1::CanonicalId { ulid: REQUEST_ID.to_owned() }),
        delivery_attempt_id: Some(common_v1::CanonicalId { ulid: DELIVERY_ATTEMPT_ID.to_owned() }),
        fetch_token: FETCH_TOKEN.to_owned(),
    }
}

fn sample_acknowledge_payload_request(
    device_id: &str,
) -> node_v1::AcknowledgeNetworkedWorkerPayloadRequest {
    node_v1::AcknowledgeNetworkedWorkerPayloadRequest {
        v: 1,
        device_id: Some(common_v1::CanonicalId { ulid: device_id.to_owned() }),
        request_id: Some(common_v1::CanonicalId { ulid: REQUEST_ID.to_owned() }),
        delivery_attempt_id: Some(common_v1::CanonicalId { ulid: DELIVERY_ATTEMPT_ID.to_owned() }),
        fetch_token: FETCH_TOKEN.to_owned(),
    }
}

fn sample_register_node_request() -> node_v1::RegisterNodeRequest {
    sample_register_node_request_for(DEVICE_ID)
}

fn sample_register_node_request_for(device_id: &str) -> node_v1::RegisterNodeRequest {
    node_v1::RegisterNodeRequest {
        v: 1,
        device_id: Some(common_v1::CanonicalId { ulid: device_id.to_owned() }),
        platform: "test-platform".to_owned(),
        capabilities: Vec::new(),
        replay: None,
    }
}

fn node_event_request(
    device_id: &str,
    event_name: &str,
    payload: serde_json::Value,
) -> node_v1::NodeEventRequest {
    node_v1::NodeEventRequest {
        v: 1,
        device_id: Some(common_v1::CanonicalId { ulid: device_id.to_owned() }),
        event_name: event_name.to_owned(),
        payload_json: serde_json::to_vec(&payload).expect("node event payload should serialize"),
        replay: None,
    }
}

struct PreparedIdentityStore {
    root: TempDir,
    gateway_ca_pem: String,
    device_certificate_pem: String,
    device_private_key_pem: String,
}

struct PairedDeviceMaterial {
    certificate_pem: String,
    private_key_pem: String,
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

fn add_paired_device(identity_store_dir: &Path, device_id: &str) -> Result<PairedDeviceMaterial> {
    let store = FilesystemSecretStore::new(identity_store_dir).with_context(|| {
        format!("failed to reopen filesystem identity store at {}", identity_store_dir.display())
    })?;
    let store: Arc<dyn SecretStore> = Arc::new(store);
    let mut manager =
        IdentityManager::with_store(store).context("failed to reopen identity manager")?;
    let device =
        DeviceIdentity::generate(device_id).context("failed to generate device identity")?;
    let session = manager
        .start_pairing(
            PairingClientKind::Node,
            PairingMethod::Pin { code: PAIRING_CODE.to_owned() },
            SystemTime::now(),
        )
        .context("failed to start second pairing session")?;
    let hello = manager
        .build_device_hello(&session, &device, PAIRING_CODE)
        .context("failed to build second device hello")?;
    let pairing = manager
        .complete_pairing(hello, SystemTime::now())
        .context("failed to complete second pairing session")?;
    Ok(PairedDeviceMaterial {
        certificate_pem: pairing.device.current_certificate.certificate_pem,
        private_key_pem: pairing.device.current_certificate.private_key_pem,
    })
}

fn spawn_palyrad_with_dynamic_ports(
    identity_store_dir: &Path,
    allow_insecure_node_rpc_without_mtls: bool,
) -> Result<(Child, u16, u16, TempDir)> {
    let runtime_root = TempDir::new().context("failed to create node RPC runtime root")?;
    let (child, admin_port, node_rpc_port) = spawn_palyrad_at_runtime_root(
        identity_store_dir,
        allow_insecure_node_rpc_without_mtls,
        runtime_root.path(),
    )?;
    Ok((child, admin_port, node_rpc_port, runtime_root))
}

fn spawn_palyrad_at_runtime_root(
    identity_store_dir: &Path,
    allow_insecure_node_rpc_without_mtls: bool,
    runtime_root: &Path,
) -> Result<(Child, u16, u16)> {
    let state_root = runtime_root.join("state");
    let vault_dir = runtime_root.join("vault");
    let config_path = runtime_root.join("palyra.toml");
    let journal_db_path = runtime_root.join("journal.sqlite3");
    fs::create_dir_all(&state_root)
        .with_context(|| format!("failed to create state root {}", state_root.display()))?;
    fs::create_dir_all(&vault_dir)
        .with_context(|| format!("failed to create vault dir {}", vault_dir.display()))?;
    fs::write(&config_path, "version = 1\n")
        .with_context(|| format!("failed to write config {}", config_path.display()))?;
    let mut command = Command::new(env!("CARGO_BIN_EXE_palyrad"));
    command
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
        .env("PALYRA_CONFIG", config_path.to_string_lossy().to_string())
        .env("PALYRA_STATE_ROOT", state_root.to_string_lossy().to_string())
        .env("PALYRA_ADMIN_TOKEN", ADMIN_TOKEN)
        .env("PALYRA_GATEWAY_QUIC_BIND_ADDR", "127.0.0.1")
        .env("PALYRA_GATEWAY_QUIC_PORT", "0")
        .env("PALYRA_JOURNAL_DB_PATH", journal_db_path.to_string_lossy().to_string())
        .env("PALYRA_GATEWAY_IDENTITY_STORE_DIR", identity_store_dir.to_string_lossy().to_string())
        .env("PALYRA_VAULT_BACKEND", "encrypted-file")
        .env("PALYRA_VAULT_DIR", vault_dir.to_string_lossy().to_string())
        .env("RUST_LOG", "info")
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    if allow_insecure_node_rpc_without_mtls {
        command.env("PALYRA_ALLOW_INSECURE_NODE_RPC_WITHOUT_MTLS", "true");
    }
    let mut child = command.spawn().context("failed to start palyrad")?;
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

    fn stop(&mut self) -> Result<()> {
        if self.child.try_wait().context("failed to check palyrad status")?.is_none() {
            self.child.kill().context("failed to stop palyrad")?;
            self.child.wait().context("failed to reap palyrad")?;
        }
        Ok(())
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
