//! gRPC, node RPC, and QUIC server bootstrap for `palyrad`.
//!
//! The bootstrap wires every tonic service with the same message-size limits
//! and shared shutdown signal, then optionally starts the QUIC node transport
//! from the same identity runtime.

use std::{net::SocketAddr, sync::Arc};

use anyhow::{Context, Result};
use palyra_identity::{build_revocation_aware_client_verifier, MemoryRevocationIndex};
use palyra_transport_quic::QuicTransportLimits;
use tokio::sync::Notify;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tracing::info;

use super::{
    proto::palyra::{auth::v1 as auth_v1, cron::v1 as cron_v1, gateway::v1 as gateway_v1},
    services,
};
use crate::{
    app::shutdown::shutdown_signal,
    channels::ChannelPlatform,
    config::LoadedConfig,
    gateway::{self, GatewayRuntimeState},
    node_rpc,
    node_runtime::NodeRuntimeState,
    quic_runtime,
    transport::grpc::auth::GatewayAuthConfig,
};

/// Starts the gateway gRPC server, node RPC server, and optional QUIC listener.
///
/// # Errors
/// Returns an error when TLS setup, listener serving, QUIC binding, verifier
/// construction, or any joined server task fails.
#[expect(clippy::too_many_arguments, reason = "server bootstrap wires existing daemon runtimes")]
pub(crate) async fn serve(
    loaded: &LoadedConfig,
    identity_runtime: &crate::IdentityRuntime,
    runtime: Arc<GatewayRuntimeState>,
    channels: Arc<ChannelPlatform>,
    auth: GatewayAuthConfig,
    auth_runtime: Arc<gateway::AuthRuntimeState>,
    grpc_url: String,
    scheduler_wake: Arc<Notify>,
    grpc_listener: tokio::net::TcpListener,
    node_rpc_listener: tokio::net::TcpListener,
    quic_address: Option<SocketAddr>,
    node_runtime: Arc<NodeRuntimeState>,
    node_rpc_mtls_required: bool,
) -> Result<()> {
    let gateway_service = services::gateway::GatewayServiceImpl::new_with_delivery_outbox(
        runtime.clone(),
        auth.clone(),
        Arc::clone(&node_runtime),
        channels,
    );
    let grpc_gateway_server =
        gateway_v1::gateway_service_server::GatewayServiceServer::new(gateway_service)
            .max_decoding_message_size(crate::GRPC_MAX_DECODING_MESSAGE_SIZE_BYTES)
            .max_encoding_message_size(crate::GRPC_MAX_ENCODING_MESSAGE_SIZE_BYTES);
    let cron_service = services::cron::CronServiceImpl::new(
        runtime.clone(),
        auth.clone(),
        grpc_url,
        Arc::clone(&scheduler_wake),
        loaded.cron.timezone,
    );
    let grpc_cron_server = cron_v1::cron_service_server::CronServiceServer::new(cron_service)
        .max_decoding_message_size(crate::GRPC_MAX_DECODING_MESSAGE_SIZE_BYTES)
        .max_encoding_message_size(crate::GRPC_MAX_ENCODING_MESSAGE_SIZE_BYTES);
    let approvals_service =
        services::approvals::ApprovalsServiceImpl::new(runtime.clone(), auth.clone());
    let grpc_approvals_server =
        gateway_v1::approvals_service_server::ApprovalsServiceServer::new(approvals_service)
            .max_decoding_message_size(crate::GRPC_MAX_DECODING_MESSAGE_SIZE_BYTES)
            .max_encoding_message_size(crate::GRPC_MAX_ENCODING_MESSAGE_SIZE_BYTES);
    let memory_service = services::memory::MemoryServiceImpl::new(runtime.clone(), auth.clone());
    let grpc_memory_server =
        crate::transport::grpc::proto::palyra::memory::v1::memory_service_server::MemoryServiceServer::new(
            memory_service,
        )
        .max_decoding_message_size(crate::GRPC_MAX_DECODING_MESSAGE_SIZE_BYTES)
        .max_encoding_message_size(crate::GRPC_MAX_ENCODING_MESSAGE_SIZE_BYTES);
    let vault_service = services::vault::VaultServiceImpl::new(runtime.clone(), auth.clone());
    let grpc_vault_server =
        gateway_v1::vault_service_server::VaultServiceServer::new(vault_service)
            .max_decoding_message_size(crate::GRPC_MAX_DECODING_MESSAGE_SIZE_BYTES)
            .max_encoding_message_size(crate::GRPC_MAX_ENCODING_MESSAGE_SIZE_BYTES);
    let auth_service = services::auth::AuthServiceImpl::new(
        runtime.clone(),
        auth.clone(),
        Arc::clone(&auth_runtime),
    );
    let grpc_auth_server = auth_v1::auth_service_server::AuthServiceServer::new(auth_service)
        .max_decoding_message_size(crate::GRPC_MAX_DECODING_MESSAGE_SIZE_BYTES)
        .max_encoding_message_size(crate::GRPC_MAX_ENCODING_MESSAGE_SIZE_BYTES);
    let canvas_service = services::canvas::CanvasServiceImpl::new(runtime.clone(), auth);
    let grpc_canvas_server =
        gateway_v1::canvas_service_server::CanvasServiceServer::new(canvas_service)
            .max_decoding_message_size(crate::GRPC_MAX_DECODING_MESSAGE_SIZE_BYTES)
            .max_encoding_message_size(crate::GRPC_MAX_ENCODING_MESSAGE_SIZE_BYTES);
    let node_rpc_service = node_rpc::NodeRpcServiceImpl::new(
        Arc::clone(&identity_runtime.manager),
        Arc::clone(&node_runtime),
        Arc::clone(&runtime),
        node_rpc_mtls_required,
    );
    let node_rpc_server =
        crate::transport::grpc::proto::palyra::node::v1::node_service_server::NodeServiceServer::new(
            node_rpc_service,
        )
        .max_decoding_message_size(crate::GRPC_MAX_DECODING_MESSAGE_SIZE_BYTES)
        .max_encoding_message_size(crate::GRPC_MAX_ENCODING_MESSAGE_SIZE_BYTES);
    let mut grpc_server_builder = Server::builder();
    if loaded.gateway.tls.enabled {
        grpc_server_builder = grpc_server_builder
            .tls_config(crate::build_gateway_tls_config(&loaded.gateway.tls)?)
            .context("failed to apply gRPC TLS configuration")?;
    }
    let mut node_rpc_server_builder = Server::builder();
    node_rpc_server_builder = node_rpc_server_builder
        .tls_config(crate::build_node_rpc_tls_config(identity_runtime, node_rpc_mtls_required))
        .context("failed to apply node RPC TLS configuration")?;

    let revoked_certificate_fingerprints = identity_runtime
        .manager
        .lock()
        .map_err(|_| {
            anyhow::anyhow!("identity manager lock poisoned while building QUIC verifier")
        })?
        .revoked_certificate_fingerprints();

    let quic_client_cert_verifier = node_rpc_mtls_required
        .then(|| {
            build_revocation_aware_client_verifier(
                &identity_runtime.gateway_ca_certificate_pem,
                Arc::new(MemoryRevocationIndex::from_fingerprints(
                    revoked_certificate_fingerprints.clone(),
                )),
            )
        })
        .transpose()
        .context("failed to build QUIC client certificate verifier")?;

    let quic_endpoint = if let Some(quic_bind_addr) = quic_address {
        let endpoint = quic_runtime::bind_endpoint(
            quic_bind_addr,
            &quic_runtime::QuicRuntimeTlsMaterial {
                ca_cert_pem: identity_runtime.gateway_ca_certificate_pem.clone(),
                cert_pem: identity_runtime.node_server_certificate.certificate_pem.clone(),
                key_pem: identity_runtime.node_server_certificate.private_key_pem.clone(),
                require_client_auth: node_rpc_mtls_required,
                client_cert_verifier: quic_client_cert_verifier.clone(),
            },
            &QuicTransportLimits::default(),
        )
        .context("failed to bind palyrad QUIC listener")?;
        let quic_bound =
            endpoint.local_addr().context("failed to resolve palyrad QUIC listen address")?;
        info!(
            quic_listen_addr = %quic_bound,
            node_rpc_mtls_required,
            "gateway QUIC listener initialized"
        );
        Some(endpoint)
    } else {
        None
    };

    let grpc_server = async move {
        grpc_server_builder
            .add_service(grpc_gateway_server)
            .add_service(grpc_cron_server)
            .add_service(grpc_approvals_server)
            .add_service(grpc_memory_server)
            .add_service(grpc_vault_server)
            .add_service(grpc_auth_server)
            .add_service(grpc_canvas_server)
            .serve_with_incoming_shutdown(TcpListenerStream::new(grpc_listener), shutdown_signal())
            .await
            .context("palyrad gRPC server failed")
    };
    let node_rpc_server = async move {
        node_rpc_server_builder
            .add_service(node_rpc_server)
            .serve_with_incoming_shutdown(
                TcpListenerStream::new(node_rpc_listener),
                shutdown_signal(),
            )
            .await
            .context("palyrad node RPC server failed")
    };

    if let Some(quic_endpoint) = quic_endpoint {
        tokio::try_join!(grpc_server, node_rpc_server, async move {
            quic_runtime::serve(quic_endpoint, node_rpc_mtls_required)
                .await
                .context("palyrad QUIC server failed")
        },)?;
    } else {
        tokio::try_join!(grpc_server, node_rpc_server)?;
    }

    Ok(())
}
