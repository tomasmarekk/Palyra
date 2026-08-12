//! gRPC `NodeService` implementation: device pairing, certificate lifecycle,
//! node registration, and capability dispatch over the node event stream.
//!
//! Every device-scoped RPC is certificate-bound: when mTLS is required, the
//! request's client certificate must map to the exact `device_id` it claims,
//! and revoked fingerprints are rejected before any state is touched. Pairing
//! is deny-by-default: completion only records a pending request whose
//! approval is resolved through the journal approval flow, and the device
//! private key is sealed via `palyra-identity` rather than persisted in node
//! runtime state. State and queues live in [`node_runtime::NodeRuntimeState`];
//! transport wiring happens in `transport::grpc::server`.

use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use sha2::{Digest, Sha256};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::{
    transport::server::{TcpConnectInfo, TlsConnectInfo},
    Request, Response, Status, Streaming,
};
use ulid::Ulid;

use palyra_common::validate_canonical_id;
use palyra_identity::{IdentityManager, PairingClientKind};

use crate::gateway::proto::palyra::{common::v1 as common_v1, node::v1 as node_v1};
use crate::{
    gateway::GatewayRuntimeState,
    journal::{
        ApprovalCreateRequest, ApprovalDecisionScope, ApprovalPolicySnapshot, ApprovalPromptOption,
        ApprovalPromptRecord, ApprovalRiskLevel, ApprovalSubjectType,
    },
    node_runtime::{
        self, CapabilityDispatchRecord, CapabilityRequestTimeoutOutcome, DeviceCapabilityView,
        DevicePairingRequestState, NodeRuntimeState, PairingCodeMethod,
    },
};

const NODE_PAIRING_PRINCIPAL: &str = "system:node-pairing";
const NODE_PAIRING_CHANNEL: &str = "node";
const NODE_CAPABILITY_TIMEOUT_MS: u64 = 30 * 1_000;

/// gRPC handler state for the node service.
///
/// Cloning is cheap (all fields are shared handles). `require_mtls` controls
/// whether device-scoped RPCs fail closed when transport metadata or a client
/// certificate is missing; with it disabled (loopback/dev deployments) the
/// certificate checks degrade to no-ops.
#[derive(Clone)]
pub struct NodeRpcServiceImpl {
    identity_manager: Arc<Mutex<IdentityManager>>,
    node_runtime: Arc<NodeRuntimeState>,
    runtime: Arc<GatewayRuntimeState>,
    require_mtls: bool,
}

impl NodeRpcServiceImpl {
    /// Creates the service from shared daemon state handles.
    #[must_use]
    pub fn new(
        identity_manager: Arc<Mutex<IdentityManager>>,
        node_runtime: Arc<NodeRuntimeState>,
        runtime: Arc<GatewayRuntimeState>,
        require_mtls: bool,
    ) -> Self {
        Self { identity_manager, node_runtime, runtime, require_mtls }
    }

    /// Extracts and screens the peer client-certificate fingerprint.
    ///
    /// Returns `Ok(None)` only when mTLS is not required and the transport
    /// carried no usable certificate; with `require_mtls` every missing-cert
    /// path fails closed, and a revoked fingerprint is always rejected.
    fn peer_certificate_fingerprint<B>(
        &self,
        request: &Request<B>,
    ) -> Result<Option<String>, Status> {
        let connect_info = request.extensions().get::<TlsConnectInfo<TcpConnectInfo>>();
        let Some(connect_info) = connect_info else {
            if self.require_mtls {
                return Err(Status::failed_precondition(
                    "node RPC endpoint requires mTLS transport metadata",
                ));
            }
            return Ok(None);
        };
        let Some(peer_certificates) = connect_info.peer_certs() else {
            if self.require_mtls {
                return Err(Status::unauthenticated(
                    "node RPC request is missing a client certificate",
                ));
            }
            return Ok(None);
        };
        let Some(peer_cert) = peer_certificates.first() else {
            if self.require_mtls {
                return Err(Status::unauthenticated(
                    "node RPC request did not provide a usable client certificate",
                ));
            }
            return Ok(None);
        };
        let fingerprint = hex::encode(Sha256::digest(peer_cert.as_ref()));
        let identity = self.identity_manager.lock().map_err(|_| {
            Status::internal(
                "identity manager lock poisoned while checking certificate fingerprint",
            )
        })?;
        if identity.is_revoked_certificate_fingerprint(fingerprint.as_str()) {
            return Err(Status::permission_denied(
                "node RPC client certificate fingerprint is revoked",
            ));
        }
        Ok(Some(fingerprint))
    }

    /// Requires that the request's client certificate is bound to exactly the
    /// claimed `device_id`, so one paired device cannot act for another.
    fn enforce_cert_bound_device<B>(
        &self,
        request: &Request<B>,
        device_id: &str,
    ) -> Result<(), Status> {
        let fingerprint = self.peer_certificate_fingerprint(request)?;
        let Some(fingerprint) = fingerprint else {
            if self.require_mtls {
                return Err(Status::unauthenticated(
                    "node RPC request requires a paired client certificate",
                ));
            }
            return Ok(());
        };
        let identity = self.identity_manager.lock().map_err(|_| {
            Status::internal("identity manager lock poisoned while resolving certificate device")
        })?;
        let Some(bound_device_id) =
            identity.device_id_for_certificate_fingerprint(fingerprint.as_str())
        else {
            return Err(Status::permission_denied(
                "node RPC client certificate does not map to a paired device",
            ));
        };
        if bound_device_id != device_id {
            return Err(Status::permission_denied(
                "node RPC request device_id does not match the authenticated client certificate",
            ));
        }
        Ok(())
    }

    fn resolve_bound_device_id(
        identity_manager: &Arc<Mutex<IdentityManager>>,
        fingerprint: &str,
    ) -> Result<Option<String>, Status> {
        let identity = identity_manager.lock().map_err(|_| {
            Status::internal("identity manager lock poisoned while validating event stream")
        })?;
        Ok(identity.device_id_for_certificate_fingerprint(fingerprint))
    }

    fn parse_client_kind(raw: &str) -> Result<PairingClientKind, Status> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "cli" => Ok(PairingClientKind::Cli),
            "desktop" => Ok(PairingClientKind::Desktop),
            "node" => Ok(PairingClientKind::Node),
            _ => Err(Status::invalid_argument("client_kind must be one of cli|desktop|node")),
        }
    }

    fn parse_pairing_method(
        value: &node_v1::PairingMethod,
    ) -> Result<(PairingCodeMethod, String), Status> {
        match value.value.as_ref() {
            Some(node_v1::pairing_method::Value::PinCode(code)) => {
                let code = code.trim();
                if code.is_empty() {
                    return Err(Status::invalid_argument("pin pairing code must not be empty"));
                }
                Ok((PairingCodeMethod::Pin, code.to_owned()))
            }
            Some(node_v1::pairing_method::Value::QrToken(token)) => {
                let token = token.trim();
                if token.is_empty() {
                    return Err(Status::invalid_argument("QR pairing token must not be empty"));
                }
                Ok((PairingCodeMethod::Qr, token.to_owned()))
            }
            None => Err(Status::invalid_argument("pairing method must be provided")),
        }
    }

    fn canonical_id_text(
        value: Option<&common_v1::CanonicalId>,
        field: &str,
    ) -> Result<String, Status> {
        let ulid = value
            .and_then(|candidate| {
                let trimmed = candidate.ulid.trim();
                (!trimmed.is_empty()).then_some(trimmed.to_owned())
            })
            .ok_or_else(|| Status::invalid_argument(format!("{field} must be a canonical ULID")))?;
        validate_canonical_id(&ulid)
            .map_err(|_| Status::invalid_argument(format!("{field} must be a canonical ULID")))?;
        Ok(ulid)
    }

    async fn create_pairing_approval(
        &self,
        session_id: &str,
        device_id: &str,
        client_kind: PairingClientKind,
        method: PairingCodeMethod,
        identity_fingerprint: &str,
        transcript_hash_hex: &str,
    ) -> Result<String, Status> {
        let approval_id = Ulid::generate().to_string();
        let prompt = ApprovalPromptRecord {
            title: format!("Approve device pairing for {device_id}"),
            risk_level: ApprovalRiskLevel::High,
            subject_id: format!("device_pairing:{session_id}:{device_id}"),
            summary: format!(
                "Device `{device_id}` requested {} pairing with method `{}`",
                client_kind.as_str(),
                method.as_str()
            ),
            options: vec![
                ApprovalPromptOption {
                    option_id: "allow_once".to_owned(),
                    label: "Approve pairing".to_owned(),
                    description: "Issue a device certificate and activate the pairing request.".to_owned(),
                    default_selected: true,
                    decision_scope: ApprovalDecisionScope::Once,
                    timebox_ttl_ms: None,
                },
                ApprovalPromptOption {
                    option_id: "deny_once".to_owned(),
                    label: "Reject pairing".to_owned(),
                    description: "Reject this device pairing request.".to_owned(),
                    default_selected: false,
                    decision_scope: ApprovalDecisionScope::Once,
                    timebox_ttl_ms: None,
                },
            ],
            timeout_seconds: 600,
            details_json: serde_json::json!({
                "session_id": session_id,
                "device_id": device_id,
                "client_kind": client_kind.as_str(),
                "method": method.as_str(),
                "identity_fingerprint": identity_fingerprint,
                "transcript_hash_hex": transcript_hash_hex,
            })
            .to_string(),
            policy_explanation:
                "Node/device pairing is deny-by-default until an operator explicitly approves the request."
                    .to_owned(),
        };
        let policy_snapshot = ApprovalPolicySnapshot {
            policy_id: "node_pairing.approval.v1".to_owned(),
            policy_hash: hex::encode(Sha256::digest(prompt.details_json.as_bytes())),
            evaluation_summary: "action=device.pair approval_required=true deny_by_default=true"
                .to_owned(),
        };
        let record = self
            .runtime
            .create_approval_record(ApprovalCreateRequest {
                approval_id: approval_id.clone(),
                session_id: session_id.to_owned(),
                run_id: Ulid::generate().to_string(),
                principal: NODE_PAIRING_PRINCIPAL.to_owned(),
                device_id: device_id.to_owned(),
                channel: Some(NODE_PAIRING_CHANNEL.to_owned()),
                subject_type: ApprovalSubjectType::DevicePairing,
                subject_id: format!("device_pairing:{session_id}:{device_id}"),
                request_summary: format!(
                    "device_id={device_id} client_kind={} method={} approval_required=true",
                    client_kind.as_str(),
                    method.as_str()
                ),
                policy_snapshot,
                prompt,
            })
            .await?;
        Ok(record.approval_id)
    }

    fn dispatch_to_proto(dispatch: CapabilityDispatchRecord) -> node_v1::NodeCapabilityDispatch {
        let networked_worker_reservation =
            dispatch.networked_worker_reservation.map(|reservation| {
                node_v1::NetworkedWorkerDeliveryReservation {
                    v: 2,
                    request_id: Some(common_v1::CanonicalId { ulid: reservation.request_id }),
                    delivery_attempt_id: Some(common_v1::CanonicalId {
                        ulid: reservation.delivery_attempt_id,
                    }),
                    protocol: node_runtime::NETWORKED_WORKER_DELIVERY_FENCE_PROTOCOL.to_owned(),
                    fetch_token: reservation.fetch_token,
                    request_sha256: reservation.request_sha256,
                    worker_id: reservation.worker_id,
                    lease_id: Some(common_v1::CanonicalId { ulid: reservation.lease_id }),
                    run_id: Some(common_v1::CanonicalId { ulid: reservation.run_id }),
                    fleet_generation: reservation.fleet_generation,
                    expires_at_unix_ms: u64::try_from(reservation.expires_at_unix_ms).unwrap_or(0),
                    run_generation: reservation.run_generation.get(),
                }
            });
        node_v1::NodeCapabilityDispatch {
            request_id: Some(common_v1::CanonicalId { ulid: dispatch.request_id }),
            capability: dispatch.capability,
            input_json: dispatch.input_json,
            max_payload_bytes: dispatch.max_payload_bytes,
            networked_worker_reservation,
        }
    }

    fn persist_pairing_private_key(
        &self,
        request_id: &str,
        private_key_pem: &str,
    ) -> Result<(), Status> {
        let identity = self.identity_manager.lock().map_err(|_| {
            Status::internal("identity manager lock poisoned while sealing pairing private key")
        })?;
        identity.persist_pending_pairing_private_key(request_id, private_key_pem).map_err(|error| {
            Status::internal(format!(
                "failed to seal pending pairing private key for {request_id}: {error}"
            ))
        })
    }

    fn resolve_pairing_private_key(
        &self,
        request_id: &str,
        material: &node_runtime::DevicePairingMaterialRecord,
    ) -> Result<String, Status> {
        let identity = self.identity_manager.lock().map_err(|_| {
            Status::internal("identity manager lock poisoned while loading pairing private key")
        })?;
        let sealed_private_key =
            identity.load_pending_pairing_private_key(request_id).map_err(|error| {
                Status::internal(format!(
                    "failed to load sealed pairing private key for {request_id}: {error}"
                ))
            })?;
        if let Some(private_key_pem) = sealed_private_key {
            return Ok(private_key_pem);
        }
        // Legacy fallback: older node runtime state persisted the private key
        // inline in the pairing material. Migrate it into sealed identity
        // storage on first read; new records never carry the inline key.
        if !material.mtls_client_private_key_pem.is_empty() {
            identity
                .persist_pending_pairing_private_key(
                    request_id,
                    material.mtls_client_private_key_pem.as_str(),
                )
                .map_err(|error| {
                    Status::internal(format!(
                        "failed to migrate legacy pairing private key for {request_id}: {error}"
                    ))
                })?;
            return Ok(material.mtls_client_private_key_pem.clone());
        }
        Err(Status::internal("completed pairing request is missing the sealed private key payload"))
    }
}

#[tonic::async_trait]
impl node_v1::node_service_server::NodeService for NodeRpcServiceImpl {
    async fn begin_pairing_session(
        &self,
        request: Request<node_v1::BeginPairingSessionRequest>,
    ) -> Result<Response<node_v1::BeginPairingSessionResponse>, Status> {
        let payload = request.into_inner();
        let client_kind = Self::parse_client_kind(payload.client_kind.as_str())?;
        let pairing_method = payload
            .method
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("pairing method is required"))?;
        let (code_method, code) = Self::parse_pairing_method(pairing_method)?;
        let reserved_code = self.node_runtime.reserve_pairing_code(code_method, code.as_str())?;
        let mut identity = self.identity_manager.lock().map_err(|_| {
            Status::internal("identity manager lock poisoned while starting pairing session")
        })?;
        let session = match identity.start_pairing(
            client_kind,
            code_method.to_pairing_method(code),
            std::time::SystemTime::now(),
        ) {
            Ok(session) => session,
            Err(error) => {
                drop(identity);
                self.node_runtime.restore_pairing_code(reserved_code)?;
                return Err(Status::failed_precondition(format!(
                    "failed to start pairing session: {error}"
                )));
            }
        };
        drop(identity);
        self.node_runtime.bind_reserved_pairing_code(session.session_id.as_str(), reserved_code)?;
        Ok(Response::new(node_v1::BeginPairingSessionResponse {
            v: payload.v.max(1),
            session_id: session.session_id,
            client_kind: session.client_kind.as_str().to_owned(),
            gateway_ephemeral_public: session.gateway_ephemeral_public.to_vec(),
            challenge: session.challenge.to_vec(),
            expires_at_unix_ms: session.expires_at_unix_ms,
        }))
    }

    async fn complete_pairing_session(
        &self,
        request: Request<node_v1::CompletePairingSessionRequest>,
    ) -> Result<Response<node_v1::CompletePairingSessionResponse>, Status> {
        let payload = request.into_inner();
        let session_id = payload.session_id.trim().to_owned();
        if session_id.is_empty() {
            return Err(Status::invalid_argument("session_id must not be empty"));
        }
        let device_id = Self::canonical_id_text(payload.device_id.as_ref(), "device_id")?;
        let client_kind = Self::parse_client_kind(payload.client_kind.as_str())?;
        let reserved_code =
            self.node_runtime.take_reserved_pairing_code(session_id.as_str())?.ok_or_else(
                || Status::failed_precondition("pairing session does not have a reserved code"),
            )?;
        let verified = {
            let mut identity = self.identity_manager.lock().map_err(|_| {
                Status::internal("identity manager lock poisoned while verifying pairing session")
            })?;
            identity
                .verify_pairing(
                    palyra_identity::DevicePairingHello {
                        session_id: session_id.clone(),
                        protocol_version: payload.v.max(1),
                        device_id: device_id.clone(),
                        client_kind,
                        // Both arms are intentionally identical: the pairing
                        // proof is the raw code for PIN and QR alike. The
                        // exhaustive match stays so a new method variant
                        // forces a decision about its proof material here.
                        proof: match reserved_code.method {
                            PairingCodeMethod::Pin => reserved_code.code.clone(),
                            PairingCodeMethod::Qr => reserved_code.code.clone(),
                        },
                        device_signing_public: payload
                            .device_signing_public
                            .as_slice()
                            .try_into()
                            .map_err(|_| {
                                Status::invalid_argument("device_signing_public must be 32 bytes")
                            })?,
                        device_x25519_public: payload
                            .device_x25519_public
                            .as_slice()
                            .try_into()
                            .map_err(|_| {
                                Status::invalid_argument("device_x25519_public must be 32 bytes")
                            })?,
                        challenge_signature: payload
                            .challenge_signature
                            .as_slice()
                            .try_into()
                            .map_err(|_| {
                                Status::invalid_argument("challenge_signature must be 64 bytes")
                            })?,
                        transcript_mac: payload.transcript_mac.as_slice().try_into().map_err(
                            |_| Status::invalid_argument("transcript_mac must be 32 bytes"),
                        )?,
                    },
                    std::time::SystemTime::now(),
                )
                .map_err(|error| {
                    Status::failed_precondition(format!("pairing verification failed: {error}"))
                })?
        };
        let approval_id = self
            .create_pairing_approval(
                session_id.as_str(),
                device_id.as_str(),
                client_kind,
                reserved_code.method,
                verified.identity_fingerprint.as_str(),
                verified.transcript_hash_hex.as_str(),
            )
            .await?;
        self.node_runtime.create_pairing_request(
            session_id.as_str(),
            verified.clone(),
            reserved_code,
            approval_id.as_str(),
        )?;
        Ok(Response::new(node_v1::CompletePairingSessionResponse {
            v: payload.v.max(1),
            paired: false,
            reason: "pending_approval".to_owned(),
            identity_fingerprint: verified.identity_fingerprint,
            transcript_hash: verified.transcript_hash_hex,
            mtls_client_certificate_pem: String::new(),
            mtls_client_private_key_pem: String::new(),
            gateway_ca_certificate_pem: String::new(),
            cert_expires_at_unix_ms: 0,
        }))
    }

    async fn get_pairing_request_status(
        &self,
        request: Request<node_v1::GetPairingRequestStatusRequest>,
    ) -> Result<Response<node_v1::GetPairingRequestStatusResponse>, Status> {
        let payload = request.into_inner();
        let session_id = payload.session_id.trim().to_owned();
        if session_id.is_empty() {
            return Err(Status::invalid_argument("session_id must not be empty"));
        }
        let device_id = Self::canonical_id_text(payload.device_id.as_ref(), "device_id")?;
        let Some(mut pairing_request) = self.node_runtime.pairing_request(session_id.as_str())?
        else {
            return Err(Status::not_found("pairing request was not found"));
        };
        if pairing_request.device_id != device_id {
            return Err(Status::permission_denied(
                "pairing request does not belong to the requested device_id",
            ));
        }
        if matches!(pairing_request.state, DevicePairingRequestState::Approved)
            && pairing_request.material.is_none()
        {
            let finalized = {
                let mut identity = self.identity_manager.lock().map_err(|_| {
                    Status::internal("identity manager lock poisoned while finalizing pairing")
                })?;
                identity
                    .finalize_verified_pairing(pairing_request.verified_pairing.clone())
                    .map_err(|error| {
                        Status::internal(format!("failed to finalize approved pairing: {error}"))
                    })?
            };
            self.persist_pairing_private_key(
                session_id.as_str(),
                finalized.device.current_certificate.private_key_pem.as_str(),
            )?;
            pairing_request = self
                .node_runtime
                .complete_pairing_request(session_id.as_str(), &finalized)?
                .ok_or_else(|| Status::internal("pairing request disappeared during completion"))?;
        }
        let material = pairing_request.material.clone();
        let private_key_pem = match material.as_ref() {
            Some(value) => self.resolve_pairing_private_key(session_id.as_str(), value)?,
            None => String::new(),
        };
        Ok(Response::new(node_v1::GetPairingRequestStatusResponse {
            v: payload.v.max(1),
            status: pairing_request.state.as_str().to_owned(),
            reason: pairing_request.decision_reason.unwrap_or_default(),
            paired: matches!(pairing_request.state, DevicePairingRequestState::Completed),
            approval_id: pairing_request.approval_id,
            identity_fingerprint: material
                .as_ref()
                .map(|value| value.identity_fingerprint.clone())
                .unwrap_or_else(|| pairing_request.verified_pairing.identity_fingerprint.clone()),
            transcript_hash: material
                .as_ref()
                .map(|value| value.transcript_hash_hex.clone())
                .unwrap_or_else(|| pairing_request.verified_pairing.transcript_hash_hex.clone()),
            mtls_client_certificate_pem: material
                .as_ref()
                .map(|value| value.mtls_client_certificate_pem.clone())
                .unwrap_or_default(),
            mtls_client_private_key_pem: private_key_pem,
            gateway_ca_certificate_pem: material
                .as_ref()
                .map(|value| value.gateway_ca_certificate_pem.clone())
                .unwrap_or_default(),
            cert_expires_at_unix_ms: material
                .as_ref()
                .and_then(|value| u64::try_from(value.cert_expires_at_unix_ms).ok())
                .unwrap_or_default(),
        }))
    }

    async fn rotate_device_certificate(
        &self,
        request: Request<node_v1::RotateDeviceCertificateRequest>,
    ) -> Result<Response<node_v1::RotateDeviceCertificateResponse>, Status> {
        let device_id = Self::canonical_id_text(request.get_ref().device_id.as_ref(), "device_id")?;
        self.enforce_cert_bound_device(&request, device_id.as_str())?;
        let certificate = {
            let mut identity = self.identity_manager.lock().map_err(|_| {
                Status::internal("identity manager lock poisoned while rotating certificate")
            })?;
            identity.force_rotate_device_certificate(device_id.as_str()).map_err(|error| {
                Status::failed_precondition(format!("device certificate rotation failed: {error}"))
            })?
        };
        Ok(Response::new(node_v1::RotateDeviceCertificateResponse {
            v: request.get_ref().v.max(1),
            rotated: true,
            reason: "rotated".to_owned(),
            mtls_client_certificate_pem: certificate.certificate_pem,
            mtls_client_private_key_pem: certificate.private_key_pem,
            cert_expires_at_unix_ms: certificate.expires_at_unix_ms,
        }))
    }

    async fn revoke_device_pairing(
        &self,
        request: Request<node_v1::RevokeDevicePairingRequest>,
    ) -> Result<Response<node_v1::RevokeDevicePairingResponse>, Status> {
        let device_id = Self::canonical_id_text(request.get_ref().device_id.as_ref(), "device_id")?;
        self.enforce_cert_bound_device(&request, device_id.as_str())?;
        {
            let mut identity = self.identity_manager.lock().map_err(|_| {
                Status::internal("identity manager lock poisoned while revoking device pairing")
            })?;
            identity
                .revoke_device(
                    device_id.as_str(),
                    request.get_ref().reason.trim(),
                    std::time::SystemTime::now(),
                )
                .map_err(|error| {
                    Status::failed_precondition(format!(
                        "device pairing revocation failed: {error}"
                    ))
                })?;
        }
        Ok(Response::new(node_v1::RevokeDevicePairingResponse {
            v: request.get_ref().v.max(1),
            revoked: true,
            reason: "revoked".to_owned(),
        }))
    }

    async fn register_node(
        &self,
        request: Request<node_v1::RegisterNodeRequest>,
    ) -> Result<Response<node_v1::RegisterNodeResponse>, Status> {
        let device_id = Self::canonical_id_text(request.get_ref().device_id.as_ref(), "device_id")?;
        self.enforce_cert_bound_device(&request, device_id.as_str())?;
        let capabilities = request
            .get_ref()
            .capabilities
            .iter()
            .map(|value| DeviceCapabilityView {
                name: value.name.trim().to_owned(),
                available: value.available,
            })
            .collect::<Vec<_>>();
        self.node_runtime.register_node(
            device_id.as_str(),
            request.get_ref().platform.trim(),
            capabilities,
        )?;
        Ok(Response::new(node_v1::RegisterNodeResponse {
            v: request.get_ref().v.max(1),
            device_id: request.get_ref().device_id.clone(),
            accepted: true,
            reason: "registered".to_owned(),
        }))
    }

    type StreamNodeEventsStream = Pin<
        Box<dyn tokio_stream::Stream<Item = Result<node_v1::NodeEventResponse, Status>> + Send>,
    >;

    async fn stream_node_events(
        &self,
        request: Request<Streaming<node_v1::NodeEventRequest>>,
    ) -> Result<Response<Self::StreamNodeEventsStream>, Status> {
        let peer_fingerprint = self.peer_certificate_fingerprint(&request)?;
        let node_runtime = Arc::clone(&self.node_runtime);
        let runtime = Arc::clone(&self.runtime);
        let identity_manager = Arc::clone(&self.identity_manager);
        let require_mtls = self.require_mtls;
        let mut inbound = request.into_inner();
        let (sender, receiver) = mpsc::channel::<Result<node_v1::NodeEventResponse, Status>>(32);

        tokio::spawn(async move {
            while let Some(message) = inbound.next().await {
                let message = match message {
                    Ok(value) => value,
                    Err(error) => {
                        let _ = sender.send(Err(Status::invalid_argument(error.to_string()))).await;
                        break;
                    }
                };
                let device_id =
                    match Self::canonical_id_text(message.device_id.as_ref(), "device_id") {
                        Ok(value) => value,
                        Err(error) => {
                            let _ = sender.send(Err(error)).await;
                            break;
                        }
                    };
                // The certificate-to-device binding is re-resolved per message
                // (not just at stream open): revoking a device removes it from
                // the paired set, so a long-lived stream fails closed on its
                // next message instead of staying authorized until reconnect.
                if require_mtls {
                    let Some(fingerprint) = peer_fingerprint.as_ref() else {
                        let _ = sender
                            .send(Err(Status::unauthenticated(
                                "node event stream requires a paired client certificate",
                            )))
                            .await;
                        break;
                    };
                    let bound_device_id = match Self::resolve_bound_device_id(
                        &identity_manager,
                        fingerprint.as_str(),
                    ) {
                        Ok(bound_device_id) => bound_device_id,
                        Err(error) => {
                            let _ = sender.send(Err(error)).await;
                            break;
                        }
                    };
                    let Some(bound_device_id) = bound_device_id else {
                        let _ = sender
                            .send(Err(Status::permission_denied(
                                "node event stream certificate is not mapped to a paired device",
                            )))
                            .await;
                        break;
                    };
                    if bound_device_id != device_id {
                        let _ = sender
                            .send(Err(Status::permission_denied(
                                "node event stream device_id does not match the authenticated client certificate",
                            )))
                            .await;
                        break;
                    }
                }

                if message.event_name == "capability.awaiting_local_mediation" {
                    let request_id = match node_runtime::parse_capability_request_id_payload(
                        &message.payload_json,
                    ) {
                        Ok(request_id) => request_id,
                        Err(error) => {
                            let _ = sender.send(Err(error)).await;
                            break;
                        }
                    };
                    if let Err(error) = node_runtime.mark_capability_awaiting_local_mediation(
                        device_id.as_str(),
                        request_id.as_str(),
                    ) {
                        let _ = sender.send(Err(error)).await;
                        break;
                    }
                } else if message.event_name == "capability.result" {
                    let (request_id, delivery_attempt_id, run_generation, result) =
                        match node_runtime::parse_capability_result_payload(&message.payload_json) {
                            Ok(result) => result,
                            Err(error) => {
                                let _ = sender.send(Err(error)).await;
                                break;
                            }
                        };
                    match node_runtime.complete_capability_request(
                        device_id.as_str(),
                        request_id.as_str(),
                        delivery_attempt_id.as_deref(),
                        run_generation,
                        result,
                        runtime.as_ref(),
                    ) {
                        Ok(true) => {}
                        Ok(false) => {
                            let response = node_v1::NodeEventResponse {
                                v: message.v.max(1),
                                event_id: Some(common_v1::CanonicalId {
                                    ulid: Ulid::generate().to_string(),
                                }),
                                accepted: false,
                                reason: "capability_result_owner_unavailable".to_owned(),
                                dispatch: None,
                            };
                            if sender.send(Ok(response)).await.is_err() {
                                break;
                            }
                            continue;
                        }
                        Err(error) => {
                            let _ = sender.send(Err(error)).await;
                            break;
                        }
                    }
                }

                let _ =
                    node_runtime.touch_node_event(device_id.as_str(), message.event_name.as_str());
                let dispatch = match node_runtime
                    .next_capability_dispatch(device_id.as_str(), runtime.as_ref())
                {
                    Ok(dispatch) => dispatch,
                    Err(error) => {
                        let _ = sender.send(Err(error)).await;
                        break;
                    }
                };
                let response = node_v1::NodeEventResponse {
                    v: message.v.max(1),
                    event_id: Some(common_v1::CanonicalId { ulid: Ulid::generate().to_string() }),
                    accepted: true,
                    reason: "accepted".to_owned(),
                    dispatch: dispatch.clone().map(Self::dispatch_to_proto),
                };
                if sender.send(Ok(response)).await.is_err() {
                    if let Some(dispatch) = dispatch.as_ref() {
                        if let Err(error) = node_runtime
                            .recover_undelivered_capability_dispatch(dispatch, runtime.as_ref())
                        {
                            tracing::warn!(
                                request_id = %dispatch.request_id,
                                error = %error,
                                "failed to recover an undelivered node capability dispatch"
                            );
                        }
                    }
                    break;
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(receiver)) as Self::StreamNodeEventsStream))
    }

    async fn fetch_networked_worker_payload(
        &self,
        request: Request<node_v1::FetchNetworkedWorkerPayloadRequest>,
    ) -> Result<Response<node_v1::FetchNetworkedWorkerPayloadResponse>, Status> {
        let device_id = Self::canonical_id_text(request.get_ref().device_id.as_ref(), "device_id")?;
        self.enforce_cert_bound_device(&request, device_id.as_str())?;
        let request_id =
            Self::canonical_id_text(request.get_ref().request_id.as_ref(), "request_id")?;
        let delivery_attempt_id = Self::canonical_id_text(
            request.get_ref().delivery_attempt_id.as_ref(),
            "delivery_attempt_id",
        )?;
        let payload = self.node_runtime.fetch_networked_worker_payload(
            device_id.as_str(),
            request_id.as_str(),
            delivery_attempt_id.as_str(),
            request.get_ref().fetch_token.as_str(),
            self.runtime.as_ref(),
        )?;
        let authenticated_delivery_hmac_sha256 =
            palyra_workerd::remote_protocol::authenticated_delivery_hmac_sha256(
                request.get_ref().fetch_token.as_str(),
                payload.request_sha256.as_str(),
                payload.input_json.as_slice(),
            );
        Ok(Response::new(node_v1::FetchNetworkedWorkerPayloadResponse {
            v: request.get_ref().v.max(1),
            request_id: Some(common_v1::CanonicalId { ulid: payload.request_id }),
            delivery_attempt_id: Some(common_v1::CanonicalId { ulid: payload.delivery_attempt_id }),
            input_json: payload.input_json,
            max_payload_bytes: payload.max_payload_bytes,
            request_sha256: payload.request_sha256,
            authenticated_delivery_hmac_sha256,
        }))
    }

    async fn acknowledge_networked_worker_payload(
        &self,
        request: Request<node_v1::AcknowledgeNetworkedWorkerPayloadRequest>,
    ) -> Result<Response<node_v1::AcknowledgeNetworkedWorkerPayloadResponse>, Status> {
        let device_id = Self::canonical_id_text(request.get_ref().device_id.as_ref(), "device_id")?;
        self.enforce_cert_bound_device(&request, device_id.as_str())?;
        let request_id =
            Self::canonical_id_text(request.get_ref().request_id.as_ref(), "request_id")?;
        let delivery_attempt_id = Self::canonical_id_text(
            request.get_ref().delivery_attempt_id.as_ref(),
            "delivery_attempt_id",
        )?;
        let outcome = self.node_runtime.acknowledge_networked_worker_payload(
            device_id.as_str(),
            request_id.as_str(),
            delivery_attempt_id.as_str(),
            request.get_ref().fetch_token.as_str(),
            self.runtime.as_ref(),
        )?;
        let reason = match outcome {
            crate::journal::NetworkedWorkerPayloadAcknowledgementOutcome::Acknowledged => {
                "acknowledged"
            }
            crate::journal::NetworkedWorkerPayloadAcknowledgementOutcome::AlreadyAcknowledged => {
                "already_acknowledged"
            }
            crate::journal::NetworkedWorkerPayloadAcknowledgementOutcome::Rejected => {
                unreachable!("node runtime maps rejected acknowledgements to a status error")
            }
        };
        Ok(Response::new(node_v1::AcknowledgeNetworkedWorkerPayloadResponse {
            v: request.get_ref().v.max(1),
            acknowledged: true,
            reason: reason.to_owned(),
        }))
    }

    async fn execute_capability(
        &self,
        request: Request<node_v1::ExecuteCapabilityRequest>,
    ) -> Result<Response<node_v1::ExecuteCapabilityResponse>, Status> {
        let device_id = Self::canonical_id_text(request.get_ref().device_id.as_ref(), "device_id")?;
        self.enforce_cert_bound_device(&request, device_id.as_str())?;
        if self.node_runtime.node(device_id.as_str())?.is_none() {
            return Err(Status::failed_precondition(
                "node must register before executing capabilities",
            ));
        }
        let (request_id, mut receiver) = self.node_runtime.enqueue_capability_request(
            device_id.as_str(),
            request.get_ref().capability.trim(),
            request.get_ref().input_json.clone(),
            request.get_ref().max_payload_bytes,
            Some(NODE_CAPABILITY_TIMEOUT_MS),
        )?;
        // The deadline is enforced here, not inside the queue: the dispatch
        // record stays addressable so a late node result can still be journaled
        // after this RPC has already returned DEADLINE_EXCEEDED.
        let result = match tokio::time::timeout(
            Duration::from_millis(NODE_CAPABILITY_TIMEOUT_MS),
            receiver.recv(),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => match self.node_runtime.mark_capability_timeout(request_id.as_str())? {
                CapabilityRequestTimeoutOutcome::MarkedTimedOut
                | CapabilityRequestTimeoutOutcome::AlreadyTerminal => {
                    return Err(Status::deadline_exceeded(
                        "timed out waiting for node capability result",
                    ));
                }
                CapabilityRequestTimeoutOutcome::ResultCommitted => receiver.recv().await,
                CapabilityRequestTimeoutOutcome::Missing => {
                    return Err(Status::internal(
                        "node capability request disappeared during timeout handling",
                    ));
                }
            },
        };
        Ok(Response::new(node_v1::ExecuteCapabilityResponse {
            v: request.get_ref().v.max(1),
            success: result.result.success,
            output_json: result.result.output_json,
            error: result.result.error,
        }))
    }
}
