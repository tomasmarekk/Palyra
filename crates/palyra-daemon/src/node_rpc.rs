use std::{collections::HashSet, pin::Pin, sync::Arc};

use sha2::{Digest, Sha256};
use tonic::{
    transport::server::{TcpConnectInfo, TlsConnectInfo},
    Request, Response, Status, Streaming,
};

use crate::gateway::proto::palyra::node::v1 as node_v1;

#[derive(Debug, Clone)]
pub struct NodeRpcServiceImpl {
    revoked_certificate_fingerprints: Arc<HashSet<String>>,
    require_mtls: bool,
}

impl NodeRpcServiceImpl {
    #[must_use]
    pub fn new(revoked_certificate_fingerprints: HashSet<String>, require_mtls: bool) -> Self {
        Self {
            revoked_certificate_fingerprints: Arc::new(revoked_certificate_fingerprints),
            require_mtls,
        }
    }

    #[allow(clippy::result_large_err)]
    fn enforce_peer_certificate<B>(&self, request: &Request<B>) -> Result<(), Status> {
        let connect_info = request.extensions().get::<TlsConnectInfo<TcpConnectInfo>>();
        if !self.require_mtls {
            if let Some(connect_info) = connect_info {
                if let Some(peer_certificates) = connect_info.peer_certs() {
                    if let Some(peer_cert) = peer_certificates.first() {
                        let fingerprint = hex::encode(Sha256::digest(peer_cert.as_ref()));
                        if self.revoked_certificate_fingerprints.contains(&fingerprint) {
                            return Err(Status::permission_denied(
                                "node RPC client certificate fingerprint is revoked",
                            ));
                        }
                    }
                }
            }
            return Ok(());
        }
        let Some(connect_info) = connect_info else {
            return Err(Status::failed_precondition(
                "node RPC endpoint requires mTLS transport metadata",
            ));
        };
        let Some(peer_certs) = connect_info.peer_certs() else {
            return Err(Status::unauthenticated(
                "node RPC request is missing a client certificate",
            ));
        };
        let Some(peer_cert) = peer_certs.first() else {
            return Err(Status::unauthenticated(
                "node RPC request did not provide a usable client certificate",
            ));
        };
        let fingerprint = hex::encode(Sha256::digest(peer_cert.as_ref()));
        if self.revoked_certificate_fingerprints.contains(&fingerprint) {
            return Err(Status::permission_denied(
                "node RPC client certificate fingerprint is revoked",
            ));
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl node_v1::node_service_server::NodeService for NodeRpcServiceImpl {
    async fn begin_pairing_session(
        &self,
        request: Request<node_v1::BeginPairingSessionRequest>,
    ) -> Result<Response<node_v1::BeginPairingSessionResponse>, Status> {
        self.enforce_peer_certificate(&request)?;
        Err(Status::unimplemented("BeginPairingSession is not implemented in daemon runtime yet"))
    }

    async fn complete_pairing_session(
        &self,
        request: Request<node_v1::CompletePairingSessionRequest>,
    ) -> Result<Response<node_v1::CompletePairingSessionResponse>, Status> {
        self.enforce_peer_certificate(&request)?;
        Err(Status::unimplemented(
            "CompletePairingSession is not implemented in daemon runtime yet",
        ))
    }

    async fn rotate_device_certificate(
        &self,
        request: Request<node_v1::RotateDeviceCertificateRequest>,
    ) -> Result<Response<node_v1::RotateDeviceCertificateResponse>, Status> {
        self.enforce_peer_certificate(&request)?;
        Err(Status::unimplemented(
            "RotateDeviceCertificate is not implemented in daemon runtime yet",
        ))
    }

    async fn revoke_device_pairing(
        &self,
        request: Request<node_v1::RevokeDevicePairingRequest>,
    ) -> Result<Response<node_v1::RevokeDevicePairingResponse>, Status> {
        self.enforce_peer_certificate(&request)?;
        Err(Status::unimplemented("RevokeDevicePairing is not implemented in daemon runtime yet"))
    }

    async fn register_node(
        &self,
        request: Request<node_v1::RegisterNodeRequest>,
    ) -> Result<Response<node_v1::RegisterNodeResponse>, Status> {
        self.enforce_peer_certificate(&request)?;
        Err(Status::unimplemented("RegisterNode is not implemented in daemon runtime yet"))
    }

    type StreamNodeEventsStream = Pin<
        Box<dyn tokio_stream::Stream<Item = Result<node_v1::NodeEventResponse, Status>> + Send>,
    >;

    async fn stream_node_events(
        &self,
        request: Request<Streaming<node_v1::NodeEventRequest>>,
    ) -> Result<Response<Self::StreamNodeEventsStream>, Status> {
        self.enforce_peer_certificate(&request)?;
        Err(Status::unimplemented("StreamNodeEvents is not implemented in daemon runtime yet"))
    }

    async fn execute_capability(
        &self,
        request: Request<node_v1::ExecuteCapabilityRequest>,
    ) -> Result<Response<node_v1::ExecuteCapabilityResponse>, Status> {
        self.enforce_peer_certificate(&request)?;
        Err(Status::unimplemented("ExecuteCapability is not implemented in daemon runtime yet"))
    }
}
