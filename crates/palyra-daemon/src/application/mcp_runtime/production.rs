//! Production assembly for durable MCP actors and host-owned transport ports.
//!
//! This module is the only bridge from validated daemon configuration to the
//! persistent actor registry. It deliberately keeps process creation, vault
//! resolution, journal writes, and network policy outside protocol connectors.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    io::{Read, Write},
    net::IpAddr,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc as std_mpsc, Arc, Mutex, RwLock, Weak,
    },
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use palyra_common::{
    runtime_contracts::{
        CleanupOutcome, RuntimeHandleDescriptorV1, RuntimeHandleKind, RuntimeHandleState,
    },
    runtime_preview::RuntimePreviewMode,
};
use palyra_egress_proxy::{EgressPolicyVerdict, EgressProxyPolicyService, EgressProxyRequest};
use palyra_vault::{Vault, VaultRef};
use reqwest::{
    header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE},
    redirect::Policy,
    Client, Method, Response, StatusCode, Url,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot, Mutex as AsyncMutex, OwnedSemaphorePermit, Semaphore};
use ulid::Ulid;

use crate::{
    application::local_resource_governor::{
        LocalResourceGovernor, ResourceLeaseRequestV1, ResourceLeaseV1, ResourcePriority,
        ResourceServiceKind, ResourceUnitsV1,
    },
    application::mcp_broker::{
        build_mcp_tool_catalog_snapshot_with_external_tools, mcp_manifest_from_config, McpBroker,
        McpBrokerPolicy, McpRuntimeSupervisor, McpRuntimeTransport, McpServerLifecycleState,
        McpToolDiscoveryReport, McpToolInvocationOutcome,
    },
    application::tool_registry::{ModelVisibleToolCatalogSnapshot, ToolCatalogBuildRequest},
    config::{
        McpServerConfig, McpServerEgressPolicy, McpServerOAuthGrant, McpServerSamplingMode,
        McpServerTransport, McpServerTrustLevel, McpServersConfig,
    },
    gateway::GatewayRuntimeState,
    journal::{
        ApprovalCreateRequest, ApprovalDecision, ApprovalDecisionScope, ApprovalPolicySnapshot,
        ApprovalPromptOption, ApprovalPromptRecord, ApprovalResolveRequest, ApprovalRiskLevel,
        ApprovalSubjectType,
    },
    model_provider::{ProviderOutputContentPart, ProviderRequest},
    provider_leases::{LeasePriority, ProviderLeaseExecutionContext},
    sandbox_runner::{
        spawn_sandboxed_managed_stdio_process, ManagedStdioProcess, ManagedStdioProcessConfig,
    },
    usage_governance::resolve_provider_binding_for_model,
};

use super::{
    McpActorFactoryError, McpActorLaunchPlan, McpActorRegistry, McpActorRegistryDrainReport,
    McpActorRegistryError, McpActorRuntimeFactory, McpAuthorizedElicitationRequest,
    McpAuthorizedSamplingRequest, McpByteReader, McpByteWriter, McpCallbackBinding,
    McpCatalogAuthority, McpCatalogEpochPin, McpConformanceReportV1, McpConnectRequest,
    McpConnectorCatalogState, McpConnectorLimits, McpConnectorPortError,
    McpDescriptorAdmissionError, McpDescriptorAdmissionPolicy, McpDescriptorAttestation,
    McpDescriptorTrustVerifier, McpElicitationExecutionPort, McpExternalToolDescriptor,
    McpHostCallbackPolicy, McpHostExecutionError, McpHostPolicyCallbackService, McpHttpConnector,
    McpHttpConnectorConfig, McpHttpSessionCloseRequest, McpHttpSessionEventRequest,
    McpHttpSessionExchangeRequest, McpHttpSessionOpenRequest, McpHttpSessionPort,
    McpHttpSessionResponse, McpInitializeRequest, McpLaunchedProcessSession,
    McpOAuthCredentialError, McpOAuthCredentialLease, McpOAuthCredentialPort,
    McpOAuthRefreshCoordinator, McpOAuthRefreshRequest, McpPolicyAuditAppendOutcome,
    McpPolicyAuditEventV1, McpPolicyAuditStore, McpPolicyAuditStoreError, McpProcessCloseEvidence,
    McpProcessControl, McpProcessLaunchRequest, McpProcessLauncher, McpProtocolCapabilities,
    McpReconnectPolicy, McpRuntimeEventV2, McpRuntimeLifecycleState, McpRuntimeRecordStore,
    McpRuntimeStoreError, McpSamplingExecutionPort, McpSamplingUsage, McpSecurityEvidenceStore,
    McpSecurityEvidenceStoreError, McpServerCallbackResponse, McpServerRecordV2,
    McpSessionActorConfig, McpSessionConnector, McpSessionReader, McpSessionRequest,
    McpSessionTransportKind, McpSessionWriter, McpSseConnector, McpSseConnectorConfig,
    McpStdioConnector, McpStdioConnectorConfig, McpTransportError, McpTransportSession,
    McpTrustedToolActivationState, McpTrustedToolApproval, McpTrustedToolRecordV1,
    McpTrustedToolRegistry, McpTrustedToolRegistryError, McpVerifiedDescriptorIdentity,
    TrustedExternalToolRegistrationRequest,
};

const IO_CHANNEL_CAPACITY: usize = 32;
const IO_THREAD_CHUNK_BYTES: usize = 8 * 1024;
const DEFAULT_PROCESS_LEASE: Duration = Duration::from_secs(24 * 60 * 60);
const DEFAULT_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_RESOURCE_LEASE: Duration = Duration::from_secs(60);
const DEFAULT_ELICITATION_WAIT_TIMEOUT: Duration = Duration::from_secs(12);
const MCP_SAMPLING_LEASE_WAIT_MS: u64 = 5_000;
const MCP_BROKER_MAX_CONCURRENT_WORKERS: usize = 1;
const MCP_BROKER_WORKER_STACK_BYTES: usize = 8 * 1024 * 1024;
const MCP_PROTOCOL_VERSION: &str = "2025-06-18";
const REMOTE_CONTENT_TYPE_JSON: &str = "application/json";
const REMOTE_CONTENT_TYPE_SSE: &str = "text/event-stream";
const REMOTE_ACCEPT: &str = "application/json, text/event-stream";
const REMOTE_SESSION_HEADER: &str = "mcp-session-id";

/// Installed production MCP runtime and its model-visible discovery state.
pub(crate) struct McpProductionRuntime {
    mode: RuntimePreviewMode,
    registry: Arc<McpActorRegistry>,
    transport: McpRuntimeTransport,
    broker: Mutex<McpBroker>,
    broker_worker_slots: Arc<Semaphore>,
    supervisor: Arc<Mutex<McpRuntimeSupervisor>>,
    discovery_reports: RwLock<Vec<McpToolDiscoveryReport>>,
    authorities: Arc<RwLock<BTreeMap<String, Arc<McpCatalogAuthority>>>>,
    security_store: Arc<GatewayMcpStore>,
    server_trust: BTreeMap<String, McpServerTrustLevel>,
    security_gate: AsyncMutex<()>,
}

impl std::fmt::Debug for McpProductionRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("McpProductionRuntime")
            .field("mode", &self.mode)
            .field("actor_count", &self.registry.len())
            .finish_non_exhaustive()
    }
}

impl McpProductionRuntime {
    /// Reconciles durable records, restores one actor per active server, and
    /// primes the broker catalog through the persistent registry transport.
    ///
    /// # Errors
    /// Returns a fail-closed startup error for durable-state, factory, or
    /// manifest failures. Individual unavailable servers remain supervised and
    /// are reflected as unhealthy instead of aborting the daemon.
    pub(crate) async fn bootstrap(
        runtime: &Arc<GatewayRuntimeState>,
        config: &McpServersConfig,
        startup_cwd: PathBuf,
    ) -> Result<Arc<Self>, McpProductionRuntimeError> {
        let store = Arc::new(GatewayMcpStore { runtime: Arc::downgrade(runtime) });
        reconcile_records(store.as_ref(), config).await?;
        let active_configs = config
            .servers
            .iter()
            .filter(|server| server.enabled && config.mode != RuntimePreviewMode::Disabled)
            .cloned()
            .map(|server| (server.id.clone(), server))
            .collect::<BTreeMap<_, _>>();
        let launcher = Arc::new(McpStdioProcessLauncher {
            configs: Arc::new(active_configs.clone()),
            startup_cwd,
            vault: Arc::clone(&runtime.vault),
            runtime: Arc::downgrade(runtime),
        });
        let authorities = Arc::new(RwLock::new(BTreeMap::new()));
        let factory = Arc::new(ProductionActorFactory {
            configs: active_configs,
            launcher,
            audit: store.clone(),
            vault: Arc::clone(&runtime.vault),
            runtime: Arc::downgrade(runtime),
            resource_governor: runtime.mcp_resource_governor(),
            authorities: Arc::clone(&authorities),
        });
        let registry = Arc::new(
            McpActorRegistry::restore_and_start(store.clone(), factory, DEFAULT_DRAIN_TIMEOUT)
                .await?,
        );
        let transport = McpRuntimeTransport::new(Arc::clone(&registry));
        let supervisor = Arc::new(Mutex::new(McpRuntimeSupervisor::from_config(config)));
        let policy = McpBrokerPolicy {
            allowed_stdio_commands: config
                .servers
                .iter()
                .filter_map(|server| server.command.as_ref()?.first().cloned())
                .collect(),
            ..McpBrokerPolicy::default()
        };
        let mut broker = McpBroker::new(policy);
        for server in &config.servers {
            broker.register_manifest(mcp_manifest_from_config(server))?;
        }
        let production = Arc::new(Self {
            mode: config.mode,
            registry,
            transport,
            broker: Mutex::new(broker),
            broker_worker_slots: Arc::new(Semaphore::new(MCP_BROKER_MAX_CONCURRENT_WORKERS)),
            supervisor,
            discovery_reports: RwLock::new(Vec::new()),
            authorities,
            security_store: store,
            server_trust: config
                .servers
                .iter()
                .map(|server| (server.id.clone(), server.trust_level))
                .collect(),
            security_gate: AsyncMutex::new(()),
        });
        production.prime_catalog(runtime, config).await;
        production.spawn_catalog_refresh_pumps();
        Ok(production)
    }

    /// Returns the shared compatibility diagnostics projection actively
    /// updated by production actor startup and drain.
    pub(crate) fn supervisor(&self) -> Arc<Mutex<McpRuntimeSupervisor>> {
        Arc::clone(&self.supervisor)
    }

    /// Returns the runtime mode controlling model visibility and invocation.
    #[must_use]
    pub(crate) const fn mode(&self) -> RuntimePreviewMode {
        self.mode
    }

    /// Returns a consistent copy of the current imported-tool reports.
    pub(crate) fn discovery_reports(&self) -> Vec<McpToolDiscoveryReport> {
        self.discovery_reports.read().map(|reports| reports.clone()).unwrap_or_default()
    }

    /// Builds the same catalog while merging additional host-verified entries.
    pub(crate) fn build_tool_catalog_snapshot_with_external_tools(
        &self,
        request: ToolCatalogBuildRequest<'_>,
        external_tools: &[crate::application::tool_registry::ToolRegistryEntry],
    ) -> ModelVisibleToolCatalogSnapshot {
        if self.mode != RuntimePreviewMode::Enabled {
            return crate::application::tool_registry::
                build_model_visible_tool_catalog_snapshot_with_external_records(
                    request,
                    external_tools,
                    &[],
                    &[],
                );
        }
        let supervisor = self
            .supervisor
            .lock()
            .map(|supervisor| supervisor.snapshot(now_unix_ms()))
            .unwrap_or_else(|_| {
                McpRuntimeSupervisor::from_config(&McpServersConfig::default())
                    .snapshot(now_unix_ms())
            });
        let reports = self.discovery_reports();
        build_mcp_tool_catalog_snapshot_with_external_tools(
            request,
            &supervisor,
            reports.as_slice(),
            external_tools,
        )
    }

    /// Invokes one already-admitted external tool through the broker's
    /// independent policy gates and the persistent actor transport.
    pub(crate) async fn invoke_tool(
        self: &Arc<Self>,
        runtime: Arc<GatewayRuntimeState>,
        tool_name: String,
        input_json: Vec<u8>,
        approval_id: String,
        callback_binding: McpCallbackBinding,
    ) -> Result<McpToolInvocationOutcome, crate::application::mcp_broker::McpBrokerError> {
        let input: Value = serde_json::from_slice(&input_json).map_err(|error| {
            crate::application::mcp_broker::McpBrokerError::new(
                "mcp.tool_input_invalid",
                format!("MCP tool input is not valid JSON: {error}"),
            )
        })?;
        let refresh_server_id = self.discovery_reports().into_iter().find_map(|report| {
            report
                .registry_entries
                .iter()
                .any(|entry| entry.name == tool_name)
                .then_some(report.server_name)
        });
        let server_id = refresh_server_id.clone().ok_or_else(|| {
            crate::application::mcp_broker::McpBrokerError::new(
                "mcp.tool_unknown",
                format!("MCP tool '{tool_name}' is not in the active catalog"),
            )
        })?;
        let worker_permit = acquire_broker_worker_slot(&self.broker_worker_slots)?;
        self.registry
            .wait_until_ready(server_id.as_str(), DEFAULT_HANDSHAKE_TIMEOUT)
            .await
            .map_err(|error| {
                crate::application::mcp_broker::McpBrokerError::new(
                    "mcp.runtime.actor_not_ready",
                    format!("persistent MCP actor did not become ready: {error}"),
                )
            })?;
        let production = Arc::clone(self);
        let (result_sender, result_receiver) = oneshot::channel();
        // The broker mutex already serializes this path. Admit only one worker
        // before reserving its large native stack so contenders cannot consume
        // process-wide thread or memory capacity while waiting for that lock.
        thread::Builder::new()
            .name("palyra-mcp-broker".to_owned())
            .stack_size(MCP_BROKER_WORKER_STACK_BYTES)
            .spawn(move || {
                let _worker_permit = worker_permit;
                let result = production
                    .broker
                    .lock()
                    .map_err(|_| {
                        crate::application::mcp_broker::McpBrokerError::new(
                            "mcp.broker_unavailable",
                            "MCP broker lock is unavailable",
                        )
                    })
                    .and_then(|mut broker| {
                        if broker.state(server_id.as_str())? != McpServerLifecycleState::Healthy {
                            broker.start_server_with_managed_health(
                                runtime.as_ref(),
                                server_id.as_str(),
                                &production.transport,
                            )?;
                            let report = broker.discover_tools_with_managed_health(
                                runtime.as_ref(),
                                server_id.as_str(),
                                &production.transport,
                            )?;
                            production.replace_discovery_report(report).map_err(|error| {
                                crate::application::mcp_broker::McpBrokerError::new(
                                    "mcp.catalog_state_unavailable",
                                    format!("MCP catalog repair could not be committed: {error}"),
                                )
                            })?;
                        }
                        let initial = broker.invoke_namespaced_tool_with_managed_health(
                            runtime.as_ref(),
                            tool_name.as_str(),
                            input.clone(),
                            approval_id.as_str(),
                            callback_binding.clone(),
                            &production.transport,
                        );
                        if initial
                            .as_ref()
                            .is_err_and(|error| error.reason_code == "mcp.tool_unknown")
                        {
                            if let Some(server_id) = refresh_server_id {
                                // A model-visible discovery report proves this tool was admitted.
                                // Repair one missing broker projection before any effect crossed
                                // the transport boundary, then retry exactly once.
                                let report = broker.discover_tools_with_managed_health(
                                    runtime.as_ref(),
                                    server_id.as_str(),
                                    &production.transport,
                                )?;
                                production.replace_discovery_report(report).map_err(|error| {
                                    crate::application::mcp_broker::McpBrokerError::new(
                                        "mcp.catalog_state_unavailable",
                                        format!(
                                            "MCP catalog refresh could not be committed: {error}"
                                        ),
                                    )
                                })?;
                                return broker.invoke_namespaced_tool_with_managed_health(
                                    runtime.as_ref(),
                                    tool_name.as_str(),
                                    input,
                                    approval_id.as_str(),
                                    callback_binding,
                                    &production.transport,
                                );
                            }
                        }
                        initial
                    });
                let _ = result_sender.send(result);
            })
            .map_err(|error| {
                crate::application::mcp_broker::McpBrokerError::new(
                    "mcp.broker_worker_spawn_failed",
                    format!("MCP broker worker could not start: {error}"),
                )
            })?;
        result_receiver.await.map_err(|error| {
            crate::application::mcp_broker::McpBrokerError::new(
                "mcp.broker_worker_failed",
                format!("MCP broker worker failed before reporting an outcome: {error}"),
            )
        })?
    }

    /// Registers a console-authorized host-trusted descriptor and withdraws
    /// any older active digest from future catalog snapshots.
    pub(crate) async fn register_trusted_tool(
        &self,
        server_id: &str,
        descriptor: McpExternalToolDescriptor,
    ) -> Result<(McpTrustedToolRecordV1, McpCatalogEpochPin), McpProductionRuntimeError> {
        let _gate = self.security_gate.lock().await;
        let pin = self.registry.catalog_pin(server_id).await?;
        let registry = self.trusted_tool_registry(server_id)?;
        let descriptor_json = serde_json::to_vec(&descriptor)
            .map_err(|_| McpProductionRuntimeError::TrustedDescriptorInvalid)?;
        let descriptor_sha256 = hex::encode(Sha256::digest(&descriptor_json));
        let request = TrustedExternalToolRegistrationRequest {
            server_id: server_id.to_owned(),
            runtime_generation: pin.runtime_generation,
            catalog_epoch: pin.catalog_epoch,
            descriptor,
            attestation: McpDescriptorAttestation {
                issuer_id: "palyra-host".to_owned(),
                key_id: "console-authority".to_owned(),
                descriptor_sha256: descriptor_sha256.clone(),
                signature: format!("host-trusted:{descriptor_sha256}"),
            },
        };
        let record = registry.register(&pin, request, now_unix_ms()).await?;
        // A repeated identical descriptor may still be pending, but only a
        // newly admitted digest withdraws the current catalog publication.
        if record.reason_code != "mcp.runtime.trusted_tool.pending_approval" {
            return Ok((record, pin));
        }
        let next_pin = self.apply_trusted_tool_catalog_state(&pin, &record).await?;
        let record = registry.rebind_catalog_epoch(&record, &next_pin, now_unix_ms()).await?;
        Ok((record, next_pin))
    }

    /// Persists current generation- and epoch-bound conformance evidence.
    pub(crate) async fn record_conformance(
        &self,
        report: &McpConformanceReportV1,
    ) -> Result<(), McpProductionRuntimeError> {
        let _gate = self.security_gate.lock().await;
        let registry = self.trusted_tool_registry(report.server_id.as_str())?;
        registry.record_conformance(report).await?;
        Ok(())
    }

    /// Applies an explicit operator decision, requiring a complete current
    /// conformance report before an approval can activate the descriptor.
    pub(crate) async fn decide_trusted_tool(
        &self,
        decision: &McpTrustedToolApproval,
    ) -> Result<(McpTrustedToolRecordV1, McpCatalogEpochPin), McpProductionRuntimeError> {
        let _gate = self.security_gate.lock().await;
        let registry = self.trusted_tool_registry(decision.server_id.as_str())?;
        let pin = self.registry.catalog_pin(decision.server_id.as_str()).await?;
        let record = registry.decide(decision).await?;
        let next_pin = self.apply_trusted_tool_catalog_state(&pin, &record).await?;
        let record = registry.rebind_catalog_epoch(&record, &next_pin, now_unix_ms()).await?;
        Ok((record, next_pin))
    }

    fn trusted_tool_registry(
        &self,
        server_id: &str,
    ) -> Result<McpTrustedToolRegistry, McpProductionRuntimeError> {
        if !matches!(
            self.server_trust.get(server_id),
            Some(McpServerTrustLevel::Local | McpServerTrustLevel::Workspace)
        ) {
            return Err(McpProductionRuntimeError::TrustedRegistrationDenied);
        }
        let authority = self
            .authorities
            .read()
            .map_err(|_| McpProductionRuntimeError::CatalogAuthorityUnavailable)?
            .get(server_id)
            .cloned()
            .ok_or(McpProductionRuntimeError::CatalogAuthorityUnavailable)?;
        let policy = McpDescriptorAdmissionPolicy {
            trusted_issuer_ids: BTreeSet::from(["palyra-host".to_owned()]),
            allow_mutating_tools: true,
            ..McpDescriptorAdmissionPolicy::default()
        };
        let store: Arc<dyn McpSecurityEvidenceStore> = self.security_store.clone();
        Ok(McpTrustedToolRegistry::new(
            authority,
            policy,
            Arc::new(McpHostTrustedDescriptorVerifier),
            store,
        ))
    }

    async fn apply_trusted_tool_catalog_state(
        &self,
        pin: &McpCatalogEpochPin,
        record: &McpTrustedToolRecordV1,
    ) -> Result<McpCatalogEpochPin, McpProductionRuntimeError> {
        {
            let broker =
                self.broker.lock().map_err(|_| McpProductionRuntimeError::BrokerUnavailable)?;
            let mut candidate = broker.clone();
            match record.activation {
                McpTrustedToolActivationState::Active => candidate
                    .activate_trusted_tool_descriptor(&record.server_id, &record.descriptor)?,
                McpTrustedToolActivationState::PendingApproval
                | McpTrustedToolActivationState::Disabled => candidate
                    .remove_trusted_tool_descriptor(&record.server_id, &record.tool_name)?,
            };
        }
        let digest = hex::encode(Sha256::digest(
            serde_json::to_vec(&json!({
                "previous_catalog_digest": pin.catalog_digest,
                "server_id": record.server_id,
                "tool_name": record.tool_name,
                "descriptor_sha256": record.descriptor_sha256,
                "activation": record.activation,
                "record_revision": record.revision,
            }))
            .map_err(|_| McpProductionRuntimeError::TrustedDescriptorInvalid)?,
        ));
        let next_pin = self.registry.advance_host_catalog(pin, digest).await?;
        let report = {
            let mut broker =
                self.broker.lock().map_err(|_| McpProductionRuntimeError::BrokerUnavailable)?;
            match record.activation {
                McpTrustedToolActivationState::Active => broker
                    .activate_trusted_tool_descriptor(&record.server_id, &record.descriptor)?,
                McpTrustedToolActivationState::PendingApproval
                | McpTrustedToolActivationState::Disabled => {
                    broker.remove_trusted_tool_descriptor(&record.server_id, &record.tool_name)?
                }
            }
        };
        self.replace_discovery_report(report)?;
        Ok(next_pin)
    }

    fn replace_discovery_report(
        &self,
        report: McpToolDiscoveryReport,
    ) -> Result<(), McpProductionRuntimeError> {
        let mut reports = self
            .discovery_reports
            .write()
            .map_err(|_| McpProductionRuntimeError::CatalogStateUnavailable)?;
        if let Some(existing) =
            reports.iter_mut().find(|candidate| candidate.server_name == report.server_name)
        {
            *existing = report;
        } else {
            reports.push(report);
            reports.sort_by(|left, right| left.server_name.cmp(&right.server_name));
        }
        Ok(())
    }

    /// Drains all actor owners within one daemon-owned deadline.
    pub(crate) async fn drain(&self, timeout: Duration) -> McpActorRegistryDrainReport {
        let server_ids = self.registry.server_ids();
        let report = self.registry.drain(timeout).await;
        if let Ok(mut supervisor) = self.supervisor.lock() {
            let now = now_unix_ms();
            for server_id in server_ids {
                let _ = supervisor.stop_server(server_id.as_str(), now);
            }
        }
        report
    }

    async fn prime_catalog(&self, runtime: &GatewayRuntimeState, config: &McpServersConfig) {
        let mut reports = Vec::new();
        for server in config
            .servers
            .iter()
            .filter(|server| server.enabled && config.mode != RuntimePreviewMode::Disabled)
        {
            let start_attempt = self.supervisor.lock().ok().and_then(|mut supervisor| {
                supervisor.start_server(server.id.as_str(), now_unix_ms()).ok()
            });
            let readiness = self
                .registry
                .wait_until_ready(server.id.as_str(), DEFAULT_HANDSHAKE_TIMEOUT)
                .await
                .map_err(|error| {
                    crate::application::mcp_broker::McpBrokerError::new(
                        "mcp.runtime.actor_not_ready",
                        format!("persistent MCP actor did not become ready: {error}"),
                    )
                });
            let result = match readiness {
                Ok(_) => self
                    .broker
                    .lock()
                    .map_err(|_| {
                        crate::application::mcp_broker::McpBrokerError::new(
                            "mcp.broker_unavailable",
                            "MCP broker lock is unavailable during catalog startup",
                        )
                    })
                    .and_then(|mut broker| {
                        broker.start_server_with_managed_health(
                            runtime,
                            server.id.as_str(),
                            &self.transport,
                        )?;
                        broker.discover_tools_with_managed_health(
                            runtime,
                            server.id.as_str(),
                            &self.transport,
                        )
                    }),
                Err(error) => Err(error),
            };
            if let Some(attempt) = start_attempt {
                if let Ok(mut supervisor) = self.supervisor.lock() {
                    match &result {
                        Ok(_) => {
                            let _ = supervisor.record_start_success(
                                server.id.as_str(),
                                attempt.expected_generation,
                                now_unix_ms(),
                            );
                        }
                        Err(error) => {
                            let _ = supervisor.record_failure(
                                server.id.as_str(),
                                attempt.expected_generation,
                                error.reason_code.as_str(),
                                error.message.as_str(),
                                None,
                                now_unix_ms(),
                            );
                            tracing::warn!(
                                server_id = %server.id,
                                reason_code = %error.reason_code,
                                error = %error.message,
                                "persistent MCP catalog startup failed"
                            );
                        }
                    }
                }
            }
            if let Ok(report) = result {
                reports.push(report);
            }
        }
        if let Ok(mut discovery_reports) = self.discovery_reports.write() {
            *discovery_reports = reports;
        }
    }

    fn spawn_catalog_refresh_pumps(self: &Arc<Self>) {
        for server_id in self.registry.server_ids() {
            let Some(mut notifications) = self.registry.take_notifications(server_id.as_str())
            else {
                continue;
            };
            let runtime = Arc::downgrade(self);
            tokio::spawn(async move {
                while let Some(notification) = notifications.recv().await {
                    if !matches!(
                        notification,
                        super::McpActorNotification::CatalogEpochAdvanced { .. }
                    ) {
                        continue;
                    }
                    let Some(runtime) = runtime.upgrade() else {
                        break;
                    };
                    // Refresh is scheduled outside the actor callback so the
                    // notification cannot wedge the in-flight RPC reader.
                    tokio::task::yield_now().await;
                    runtime.refresh_server_catalog(server_id.as_str()).await;
                }
            });
        }
    }

    async fn refresh_server_catalog(&self, server_id: &str) {
        let report = self
            .broker
            .lock()
            .ok()
            .and_then(|mut broker| broker.discover_tools(server_id, &self.transport).ok());
        let Some(report) = report else {
            return;
        };
        let Ok(mut reports) = self.discovery_reports.write() else {
            return;
        };
        if let Some(existing) =
            reports.iter_mut().find(|candidate| candidate.server_name == report.server_name)
        {
            *existing = report;
        } else {
            reports.push(report);
            reports.sort_by(|left, right| left.server_name.cmp(&right.server_name));
        }
    }
}

fn acquire_broker_worker_slot(
    worker_slots: &Arc<Semaphore>,
) -> Result<OwnedSemaphorePermit, crate::application::mcp_broker::McpBrokerError> {
    Arc::clone(worker_slots).try_acquire_owned().map_err(|_| {
        crate::application::mcp_broker::McpBrokerError::new(
            "mcp.broker_backpressure",
            "MCP broker worker capacity is exhausted; retry after the active invocation completes",
        )
    })
}

/// Production MCP assembly failure.
#[derive(Debug, Error)]
pub(crate) enum McpProductionRuntimeError {
    /// Durable runtime or actor ownership could not be restored.
    #[error(transparent)]
    Registry(#[from] McpActorRegistryError),
    /// Durable runtime state could not be reconciled.
    #[error(transparent)]
    Store(#[from] McpRuntimeStoreError),
    /// Durable record transition could not be planned.
    #[error(transparent)]
    Supervisor(#[from] super::McpRuntimeSupervisorError),
    /// Broker manifest registration failed closed.
    #[error(transparent)]
    Broker(#[from] crate::application::mcp_broker::McpBrokerError),
    /// Trusted descriptor security validation or persistence failed.
    #[error(transparent)]
    TrustedRegistry(#[from] McpTrustedToolRegistryError),
    /// Only explicitly configured local or workspace servers may use the
    /// console-authorized host-trusted descriptor path.
    #[error("mcp trusted descriptor registration is denied for this server")]
    TrustedRegistrationDenied,
    /// The actor's shared catalog authority cannot be loaded.
    #[error("mcp catalog authority is unavailable")]
    CatalogAuthorityUnavailable,
    /// The broker catalog lock cannot be acquired.
    #[error("mcp broker is unavailable")]
    BrokerUnavailable,
    /// The model-visible discovery projection cannot be updated.
    #[error("mcp catalog state is unavailable")]
    CatalogStateUnavailable,
    /// A trusted descriptor could not be canonically encoded.
    #[error("invalid mcp trusted descriptor")]
    TrustedDescriptorInvalid,
}

struct McpHostTrustedDescriptorVerifier;

impl McpDescriptorTrustVerifier for McpHostTrustedDescriptorVerifier {
    fn verify(
        &self,
        request: &TrustedExternalToolRegistrationRequest,
        canonical_descriptor_sha256: &str,
    ) -> Result<McpVerifiedDescriptorIdentity, McpDescriptorAdmissionError> {
        if request.attestation.issuer_id != "palyra-host"
            || request.attestation.key_id != "console-authority"
            || request.attestation.descriptor_sha256 != canonical_descriptor_sha256
            || request.attestation.signature
                != format!("host-trusted:{canonical_descriptor_sha256}")
        {
            return Err(McpDescriptorAdmissionError::TrustVerificationFailed);
        }
        Ok(McpVerifiedDescriptorIdentity {
            issuer_id: request.attestation.issuer_id.clone(),
            key_id: request.attestation.key_id.clone(),
        })
    }
}

struct GatewayMcpStore {
    runtime: Weak<GatewayRuntimeState>,
}

impl GatewayMcpStore {
    fn runtime(&self) -> Result<Arc<GatewayRuntimeState>, McpRuntimeStoreError> {
        self.runtime.upgrade().ok_or_else(|| McpRuntimeStoreError::Unavailable {
            reason_code: "mcp.runtime.gateway_dropped".to_owned(),
        })
    }
}

#[async_trait]
impl McpRuntimeRecordStore for GatewayMcpStore {
    async fn load_all(&self) -> Result<Vec<McpServerRecordV2>, McpRuntimeStoreError> {
        McpRuntimeRecordStore::load_all(&self.runtime()?.journal_store).await
    }

    async fn insert_configured(
        &self,
        record: &McpServerRecordV2,
    ) -> Result<(), McpRuntimeStoreError> {
        McpRuntimeRecordStore::insert_configured(&self.runtime()?.journal_store, record).await
    }

    async fn persist_transition(
        &self,
        expected_revision: u64,
        record: &McpServerRecordV2,
        event: &McpRuntimeEventV2,
    ) -> Result<(), McpRuntimeStoreError> {
        McpRuntimeRecordStore::persist_transition(
            &self.runtime()?.journal_store,
            expected_revision,
            record,
            event,
        )
        .await
    }
}

#[async_trait]
impl McpSecurityEvidenceStore for GatewayMcpStore {
    async fn load_trusted_tool(
        &self,
        server_id: &str,
        tool_name: &str,
    ) -> Result<Option<McpTrustedToolRecordV1>, McpSecurityEvidenceStoreError> {
        let runtime = self.runtime.upgrade().ok_or_else(security_gateway_unavailable)?;
        McpSecurityEvidenceStore::load_trusted_tool(&runtime.journal_store, server_id, tool_name)
            .await
    }

    async fn persist_trusted_tool(
        &self,
        expected_revision: Option<u64>,
        record: &McpTrustedToolRecordV1,
    ) -> Result<(), McpSecurityEvidenceStoreError> {
        let runtime = self.runtime.upgrade().ok_or_else(security_gateway_unavailable)?;
        McpSecurityEvidenceStore::persist_trusted_tool(
            &runtime.journal_store,
            expected_revision,
            record,
        )
        .await
    }

    async fn persist_conformance_report(
        &self,
        report: &McpConformanceReportV1,
    ) -> Result<(), McpSecurityEvidenceStoreError> {
        let runtime = self.runtime.upgrade().ok_or_else(security_gateway_unavailable)?;
        McpSecurityEvidenceStore::persist_conformance_report(&runtime.journal_store, report).await
    }

    async fn latest_conformance_report(
        &self,
        server_id: &str,
    ) -> Result<Option<McpConformanceReportV1>, McpSecurityEvidenceStoreError> {
        let runtime = self.runtime.upgrade().ok_or_else(security_gateway_unavailable)?;
        McpSecurityEvidenceStore::latest_conformance_report(&runtime.journal_store, server_id).await
    }
}

fn security_gateway_unavailable() -> McpSecurityEvidenceStoreError {
    McpSecurityEvidenceStoreError::Unavailable {
        reason_code: "mcp.runtime.gateway_dropped".to_owned(),
    }
}

#[async_trait]
impl McpPolicyAuditStore for GatewayMcpStore {
    async fn append_policy_event(
        &self,
        event: &McpPolicyAuditEventV1,
    ) -> Result<McpPolicyAuditAppendOutcome, McpPolicyAuditStoreError> {
        let runtime =
            self.runtime.upgrade().ok_or_else(|| McpPolicyAuditStoreError::Unavailable {
                reason_code: "mcp.runtime.gateway_dropped".to_owned(),
            })?;
        McpPolicyAuditStore::append_policy_event(&runtime.journal_store, event).await
    }

    async fn sampling_usage(
        &self,
        server_id: &str,
        binding_sha256: &str,
        since_unix_ms: i64,
    ) -> Result<McpSamplingUsage, McpPolicyAuditStoreError> {
        let runtime =
            self.runtime.upgrade().ok_or_else(|| McpPolicyAuditStoreError::Unavailable {
                reason_code: "mcp.runtime.gateway_dropped".to_owned(),
            })?;
        McpPolicyAuditStore::sampling_usage(
            &runtime.journal_store,
            server_id,
            binding_sha256,
            since_unix_ms,
        )
        .await
    }
}

async fn reconcile_records(
    store: &dyn McpRuntimeRecordStore,
    config: &McpServersConfig,
) -> Result<(), McpProductionRuntimeError> {
    let configured = config
        .servers
        .iter()
        .map(|server| (server.id.as_str(), server))
        .collect::<BTreeMap<_, _>>();
    let mut existing = store
        .load_all()
        .await?
        .into_iter()
        .map(|record| (record.server_id.clone(), record))
        .collect::<BTreeMap<_, _>>();
    for server in &config.servers {
        let enabled = server.enabled && config.mode != RuntimePreviewMode::Disabled;
        let desired_transport = runtime_transport(server.transport);
        let credential_scope_id = server.oauth_required.then(|| format!("oauth-{}", server.id));
        let trust_profile_id = trust_profile(server.trust_level).to_owned();
        let Some(current) = existing.remove(server.id.as_str()) else {
            if enabled {
                let record = McpServerRecordV2::configured(
                    server.id.clone(),
                    desired_transport,
                    credential_scope_id,
                    trust_profile_id,
                    now_unix_ms(),
                )?;
                store.insert_configured(&record).await?;
            }
            continue;
        };
        if !enabled {
            disable_record(store, current).await?;
            continue;
        }
        let current = stop_for_reconfigure(store, current).await?;
        if current.transport != desired_transport
            || current.credential_scope_id != credential_scope_id
            || current.trust_profile_id != trust_profile_id
            || current.lifecycle != McpRuntimeLifecycleState::Configured
        {
            let next = current.reconfigure(
                desired_transport,
                credential_scope_id,
                trust_profile_id,
                next_timestamp(current.updated_at_unix_ms),
            )?;
            persist_record(store, &current, &next, "mcp.runtime.config.reconciled").await?;
        }
    }
    for (_, stale) in existing {
        if !configured.contains_key(stale.server_id.as_str()) {
            disable_record(store, stale).await?;
        }
    }
    Ok(())
}

async fn stop_for_reconfigure(
    store: &dyn McpRuntimeRecordStore,
    mut record: McpServerRecordV2,
) -> Result<McpServerRecordV2, McpProductionRuntimeError> {
    if matches!(
        record.lifecycle,
        McpRuntimeLifecycleState::Starting
            | McpRuntimeLifecycleState::Handshaking
            | McpRuntimeLifecycleState::Ready
            | McpRuntimeLifecycleState::Degraded
            | McpRuntimeLifecycleState::Reconnecting
    ) {
        let next = record.transition(
            McpRuntimeLifecycleState::Stopping,
            next_timestamp(record.updated_at_unix_ms),
            "mcp.runtime.config.stop",
        )?;
        persist_record(store, &record, &next, "mcp.runtime.config.stop").await?;
        record = next;
    }
    if record.lifecycle == McpRuntimeLifecycleState::Stopping {
        let next = record.transition(
            McpRuntimeLifecycleState::Stopped,
            next_timestamp(record.updated_at_unix_ms),
            "mcp.runtime.config.stopped",
        )?;
        persist_record(store, &record, &next, "mcp.runtime.config.stopped").await?;
        record = next;
    }
    Ok(record)
}

async fn disable_record(
    store: &dyn McpRuntimeRecordStore,
    record: McpServerRecordV2,
) -> Result<(), McpProductionRuntimeError> {
    let record = stop_for_reconfigure(store, record).await?;
    if record.lifecycle == McpRuntimeLifecycleState::Disabled {
        return Ok(());
    }
    let next = record.transition(
        McpRuntimeLifecycleState::Disabled,
        next_timestamp(record.updated_at_unix_ms),
        "mcp.runtime.config.disabled",
    )?;
    persist_record(store, &record, &next, "mcp.runtime.config.disabled").await
}

async fn persist_record(
    store: &dyn McpRuntimeRecordStore,
    previous: &McpServerRecordV2,
    next: &McpServerRecordV2,
    reason_code: &str,
) -> Result<(), McpProductionRuntimeError> {
    let event = McpRuntimeEventV2::from_transition(previous, next, reason_code)?;
    store.persist_transition(previous.revision, next, &event).await?;
    Ok(())
}

fn next_timestamp(previous: i64) -> i64 {
    now_unix_ms().max(previous.saturating_add(1))
}

fn runtime_transport(transport: McpServerTransport) -> McpSessionTransportKind {
    match transport {
        McpServerTransport::Stdio => McpSessionTransportKind::Stdio,
        McpServerTransport::Http => McpSessionTransportKind::StreamableHttp,
        McpServerTransport::Sse => McpSessionTransportKind::ServerSentEvents,
    }
}

fn trust_profile(trust: McpServerTrustLevel) -> &'static str {
    match trust {
        McpServerTrustLevel::Local => "local",
        McpServerTrustLevel::Workspace => "workspace",
        McpServerTrustLevel::External => "external",
    }
}

struct GovernedMcpConnector {
    inner: Arc<dyn McpSessionConnector>,
    governor: LocalResourceGovernor,
    owner_id: String,
    transport: McpSessionTransportKind,
}

#[async_trait]
impl McpSessionConnector for GovernedMcpConnector {
    async fn connect(
        &self,
        request: &McpConnectRequest,
    ) -> Result<Box<dyn McpTransportSession>, McpTransportError> {
        let governor = self.governor.clone();
        let lease_request = ResourceLeaseRequestV1 {
            owner_id: self.owner_id.clone(),
            generation: request.runtime_generation,
            service: ResourceServiceKind::Mcp,
            priority: ResourcePriority::IdleService,
            requested: mcp_transport_resources(self.transport),
            duration: DEFAULT_RESOURCE_LEASE,
        };
        let lease = tokio::task::spawn_blocking(move || governor.acquire(lease_request))
            .await
            .map_err(|_| resource_transport_error("mcp.runtime.resource.acquire_worker_failed"))?
            .map_err(|_| resource_transport_error("mcp.runtime.resource.capacity_denied"))?;

        match self.inner.connect(request).await {
            Ok(session) => Ok(Box::new(GovernedMcpTransportSession {
                inner: Some(session),
                governor: self.governor.clone(),
                lease: Some(lease),
            })),
            Err(error) => {
                release_resource_lease(self.governor.clone(), lease).await;
                Err(error)
            }
        }
    }
}

struct GovernedMcpTransportSession {
    inner: Option<Box<dyn McpTransportSession>>,
    governor: LocalResourceGovernor,
    lease: Option<ResourceLeaseV1>,
}

impl McpTransportSession for GovernedMcpTransportSession {
    fn into_parts(
        mut self: Box<Self>,
    ) -> (super::McpInitializeResult, Box<dyn McpSessionWriter>, Box<dyn McpSessionReader>) {
        let inner = self.inner.take().expect("governed MCP session must own its transport");
        let lease = self.lease.take().expect("governed MCP session must own its resource lease");
        let (initialize, writer, reader) = inner.into_parts();
        (
            initialize,
            Box::new(GovernedMcpSessionWriter {
                inner: writer,
                governor: self.governor.clone(),
                lease: Some(lease),
            }),
            reader,
        )
    }
}

impl Drop for GovernedMcpTransportSession {
    fn drop(&mut self) {
        if let Some(lease) = self.lease.take() {
            let _ = self.governor.release(lease.lease_id.as_str(), lease.generation);
        }
    }
}

struct GovernedMcpSessionWriter {
    inner: Box<dyn McpSessionWriter>,
    governor: LocalResourceGovernor,
    lease: Option<ResourceLeaseV1>,
}

impl GovernedMcpSessionWriter {
    async fn renew(&mut self) -> Result<(), McpTransportError> {
        let lease = self
            .lease
            .as_ref()
            .ok_or_else(|| resource_transport_error("mcp.runtime.resource.lease_released"))?;
        let governor = self.governor.clone();
        let lease_id = lease.lease_id.clone();
        let generation = lease.generation;
        let renewed = tokio::task::spawn_blocking(move || {
            governor.renew(lease_id.as_str(), generation, DEFAULT_RESOURCE_LEASE)
        })
        .await
        .map_err(|_| resource_transport_error("mcp.runtime.resource.renew_worker_failed"))?
        .map_err(|_| resource_transport_error("mcp.runtime.resource.renew_denied"))?;
        self.lease = Some(renewed);
        Ok(())
    }

    async fn release(&mut self) -> Result<(), McpTransportError> {
        let Some(lease) = self.lease.take() else {
            return Ok(());
        };
        let governor = self.governor.clone();
        tokio::task::spawn_blocking(move || {
            governor.release(lease.lease_id.as_str(), lease.generation)
        })
        .await
        .map_err(|_| resource_transport_error("mcp.runtime.resource.release_worker_failed"))?
        .map(|_| ())
        .map_err(|_| resource_transport_error("mcp.runtime.resource.release_failed"))
    }
}

#[async_trait]
impl McpSessionWriter for GovernedMcpSessionWriter {
    async fn send_request(&mut self, request: McpSessionRequest) -> Result<(), McpTransportError> {
        self.renew().await?;
        self.inner.send_request(request).await
    }

    async fn send_callback_response(
        &mut self,
        response: McpServerCallbackResponse,
    ) -> Result<(), McpTransportError> {
        self.renew().await?;
        self.inner.send_callback_response(response).await
    }

    async fn close(&mut self) -> Result<(), McpTransportError> {
        let close = self.inner.close().await;
        let release = self.release().await;
        close.and(release)
    }
}

impl Drop for GovernedMcpSessionWriter {
    fn drop(&mut self) {
        if let Some(lease) = self.lease.take() {
            let _ = self.governor.release(lease.lease_id.as_str(), lease.generation);
        }
    }
}

async fn release_resource_lease(governor: LocalResourceGovernor, lease: ResourceLeaseV1) {
    let _ = tokio::task::spawn_blocking(move || {
        governor.release(lease.lease_id.as_str(), lease.generation)
    })
    .await;
}

const fn mcp_transport_resources(transport: McpSessionTransportKind) -> ResourceUnitsV1 {
    match transport {
        McpSessionTransportKind::Stdio => ResourceUnitsV1 {
            processes: 1,
            memory_bytes: 256 * 1024 * 1024,
            file_descriptors: 32,
            sockets: 0,
            spool_bytes: 2 * 1024 * 1024,
            concurrency: 1,
        },
        McpSessionTransportKind::StreamableHttp | McpSessionTransportKind::ServerSentEvents => {
            ResourceUnitsV1 {
                processes: 0,
                memory_bytes: 32 * 1024 * 1024,
                file_descriptors: 8,
                sockets: 2,
                spool_bytes: 2 * 1024 * 1024,
                concurrency: 1,
            }
        }
    }
}

fn resource_transport_error(reason_code: &str) -> McpTransportError {
    McpTransportError::Unavailable { reason_code: reason_code.to_owned() }
}

struct ProductionActorFactory {
    configs: BTreeMap<String, McpServerConfig>,
    launcher: Arc<McpStdioProcessLauncher>,
    audit: Arc<GatewayMcpStore>,
    vault: Arc<Vault>,
    runtime: Weak<GatewayRuntimeState>,
    resource_governor: LocalResourceGovernor,
    authorities: Arc<RwLock<BTreeMap<String, Arc<McpCatalogAuthority>>>>,
}

#[async_trait]
impl McpActorRuntimeFactory for ProductionActorFactory {
    async fn prepare(
        &self,
        record: &McpServerRecordV2,
    ) -> Result<McpActorLaunchPlan, McpActorFactoryError> {
        let config = self
            .configs
            .get(record.server_id.as_str())
            .ok_or_else(|| factory_error("mcp.runtime.factory.config_missing"))?;
        let existing_authority = self
            .authorities
            .read()
            .ok()
            .and_then(|authorities| authorities.get(record.server_id.as_str()).cloned());
        let authority = match existing_authority {
            Some(authority) => authority,
            None => Arc::new(
                McpCatalogAuthority::new(record.server_id.clone())
                    .map_err(|_| factory_error("mcp.runtime.factory.authority_invalid"))?,
            ),
        };
        authority
            .apply_committed(record)
            .map_err(|_| factory_error("mcp.runtime.factory.authority_invalid"))?;
        self.authorities
            .write()
            .map_err(|_| factory_error("mcp.runtime.factory.authority_unavailable"))?
            .insert(record.server_id.clone(), Arc::clone(&authority));
        let connector: Arc<dyn McpSessionConnector> = match record.transport {
            McpSessionTransportKind::Stdio => Arc::new(
                McpStdioConnector::new(
                    self.launcher.clone(),
                    McpStdioConnectorConfig {
                        launch_profile_id: record.server_id.clone(),
                        catalog_state: McpConnectorCatalogState {
                            catalog_epoch: record.catalog_epoch,
                            catalog_digest: record.catalog_digest.clone(),
                        },
                        limits: connector_limits(),
                    },
                )
                .map_err(|_| factory_error("mcp.runtime.factory.stdio_invalid"))?,
            ),
            McpSessionTransportKind::StreamableHttp => {
                let port = McpRemoteSessionPort::new(
                    config.clone(),
                    record.runtime_generation,
                    Arc::clone(&authority),
                    Arc::clone(&self.audit),
                    Arc::clone(&self.vault),
                    self.runtime.clone(),
                )
                .await?;
                Arc::new(
                    McpHttpConnector::new(
                        Arc::new(port),
                        McpHttpConnectorConfig {
                            endpoint_id: record.server_id.clone(),
                            catalog_state: McpConnectorCatalogState {
                                catalog_epoch: record.catalog_epoch,
                                catalog_digest: record.catalog_digest.clone(),
                            },
                            limits: connector_limits(),
                        },
                    )
                    .map_err(|_| factory_error("mcp.runtime.factory.http_invalid"))?,
                )
            }
            McpSessionTransportKind::ServerSentEvents => {
                let port = McpRemoteSessionPort::new(
                    config.clone(),
                    record.runtime_generation,
                    Arc::clone(&authority),
                    Arc::clone(&self.audit),
                    Arc::clone(&self.vault),
                    self.runtime.clone(),
                )
                .await?;
                Arc::new(
                    McpSseConnector::new(
                        Arc::new(port),
                        McpSseConnectorConfig {
                            event_endpoint_id: record.server_id.clone(),
                            request_endpoint_id: record.server_id.clone(),
                            catalog_state: McpConnectorCatalogState {
                                catalog_epoch: record.catalog_epoch,
                                catalog_digest: record.catalog_digest.clone(),
                            },
                            limits: connector_limits(),
                        },
                    )
                    .map_err(|_| factory_error("mcp.runtime.factory.sse_invalid"))?,
                )
            }
        };
        let connector: Arc<dyn McpSessionConnector> = Arc::new(GovernedMcpConnector {
            inner: connector,
            governor: self.resource_governor.clone(),
            owner_id: format!("mcp:{}", record.server_id),
            transport: record.transport,
        });
        let elicitation: Option<Arc<dyn McpElicitationExecutionPort>> =
            config.elicitation_enabled.then(|| {
                Arc::new(McpProductionElicitationPort {
                    server_id: record.server_id.clone(),
                    runtime: self.runtime.clone(),
                }) as Arc<dyn McpElicitationExecutionPort>
            });
        let sampling: Option<Arc<dyn McpSamplingExecutionPort>> =
            (config.sampling_policy.mode == McpServerSamplingMode::Allowlist).then(|| {
                Arc::new(McpProductionSamplingPort {
                    runtime: self.runtime.clone(),
                    allowed_model_ids: config
                        .sampling_policy
                        .allowed_model_capabilities
                        .iter()
                        .cloned()
                        .collect(),
                }) as Arc<dyn McpSamplingExecutionPort>
            });
        let policy = callback_policy(config);
        let callbacks = Arc::new(
            McpHostPolicyCallbackService::new_session_bound(
                record.server_id.clone(),
                authority,
                policy,
                self.audit.clone(),
                elicitation,
                sampling,
            )
            .map_err(|_| factory_error("mcp.runtime.factory.callback_policy_invalid"))?,
        );
        Ok(McpActorLaunchPlan::new(actor_config(record.clone(), config), connector, callbacks))
    }
}

fn actor_config(record: McpServerRecordV2, config: &McpServerConfig) -> McpSessionActorConfig {
    let timeout = DEFAULT_REQUEST_TIMEOUT;
    let sampling_enabled = config.sampling_policy.mode == McpServerSamplingMode::Allowlist;
    let callbacks_enabled = sampling_enabled || config.elicitation_enabled;
    McpSessionActorConfig {
        record,
        initialize: McpInitializeRequest {
            client_name: "palyra".to_owned(),
            client_version: env!("CARGO_PKG_VERSION").to_owned(),
            supported_protocol_versions: vec![MCP_PROTOCOL_VERSION.to_owned()],
            capabilities: McpProtocolCapabilities {
                sampling: sampling_enabled,
                elicitation: config.elicitation_enabled,
                roots: false,
                catalog_notifications: true,
            },
        },
        callback_binding: callback_binding(config.id.as_str()),
        command_queue_capacity: 128,
        notification_queue_capacity: 128,
        // A callback is authorized from the binding of the sole pending host
        // request, so callback-capable actors deliberately serialize requests.
        max_in_flight_requests: if callbacks_enabled { 1 } else { 64 },
        request_timeout: timeout,
        handshake_timeout: DEFAULT_HANDSHAKE_TIMEOUT,
        callback_timeout: Duration::from_secs(15),
        transport_operation_timeout: Duration::from_secs(10),
        keepalive_interval: DEFAULT_KEEPALIVE_INTERVAL,
        keepalive_timeout: DEFAULT_KEEPALIVE_TIMEOUT,
        default_drain_timeout: DEFAULT_DRAIN_TIMEOUT,
        reconnect_policy: McpReconnectPolicy::default(),
    }
}

fn callback_policy(config: &McpServerConfig) -> McpHostCallbackPolicy {
    let sampling_enabled = config.sampling_policy.mode == McpServerSamplingMode::Allowlist;
    McpHostCallbackPolicy {
        elicitation_enabled: config.elicitation_enabled,
        // Session-bound services obtain the real origin from the sole pending
        // host request; the marker keeps policy construction explicit.
        allowed_elicitation_origins: config
            .elicitation_enabled
            .then(|| "request-bound".to_owned())
            .into_iter()
            .collect(),
        sampling_model_id: sampling_enabled
            .then(|| config.sampling_policy.host_model_id.clone())
            .flatten(),
        allowed_sampling_origins: sampling_enabled
            .then(|| "request-bound".to_owned())
            .into_iter()
            .collect(),
        // MCP sampling is intentionally text-only and cannot inherit tools.
        allowed_sampling_tools: Default::default(),
        max_sampling_output_tokens_per_request: config
            .sampling_policy
            .max_output_tokens_per_request,
        sampling_window: Duration::from_secs(config.sampling_policy.window_seconds.max(1)),
        max_sampling_requests_per_window: config.sampling_policy.max_requests_per_window,
        max_sampling_output_tokens_per_window: config.sampling_policy.max_output_tokens_per_window,
    }
}

fn callback_binding(server_id: &str) -> McpCallbackBinding {
    McpCallbackBinding {
        principal_id: "system-mcp".to_owned(),
        session_id: format!("server-{server_id}"),
        origin: format!("mcp-{server_id}"),
    }
}

struct McpProductionSamplingPort {
    runtime: Weak<GatewayRuntimeState>,
    allowed_model_ids: BTreeSet<String>,
}

#[async_trait]
impl McpSamplingExecutionPort for McpProductionSamplingPort {
    async fn sample(
        &self,
        request: &McpAuthorizedSamplingRequest,
    ) -> Result<Value, McpHostExecutionError> {
        if !request.allowed_tools.is_empty()
            || !self.allowed_model_ids.contains(request.model_id.as_str())
        {
            return Err(host_execution_error("mcp.runtime.sampling.policy_denied"));
        }
        let runtime = self
            .runtime
            .upgrade()
            .ok_or_else(|| host_execution_error("mcp.runtime.sampling.runtime_unavailable"))?;
        let provider_snapshot = runtime.model_provider_status_snapshot();
        if !provider_model_is_enabled(&provider_snapshot, request.model_id.as_str()) {
            return Err(host_execution_error("mcp.runtime.sampling.model_unavailable"));
        }
        let (provider_id, _, credential_id) =
            resolve_provider_binding_for_model(&provider_snapshot, request.model_id.as_str());
        let input_text = serde_json::to_string(&request.input_json)
            .map_err(|_| host_execution_error("mcp.runtime.sampling.input_invalid"))?;
        let mut provider_request = ProviderRequest::from_input_text(
            input_text,
            false,
            Vec::new(),
            Some(request.model_id.clone()),
        );
        provider_request.max_output_tokens = Some(request.max_output_tokens);
        provider_request.tool_catalog_snapshot = None;
        provider_request.context_trace_id = Some(request.idempotency_key.clone());
        let response = runtime
            .execute_model_provider_with_lease(
                provider_request,
                ProviderLeaseExecutionContext {
                    provider_id,
                    credential_id,
                    priority: LeasePriority::Background,
                    task_label: "mcp_sampling".to_owned(),
                    max_wait_ms: MCP_SAMPLING_LEASE_WAIT_MS,
                    session_id: Some(request.binding.session_id.clone()),
                    run_id: None,
                    runtime_authority: None,
                    diagnostic_scope_id: Some(request.idempotency_key.clone()),
                },
            )
            .await
            .map_err(|_| host_execution_error("mcp.runtime.sampling.provider_failed"))?;
        if !self.allowed_model_ids.contains(response.model_id.to_ascii_lowercase().as_str()) {
            return Err(host_execution_error("mcp.runtime.sampling.model_not_allowlisted"));
        }
        if response
            .output
            .content_parts
            .iter()
            .any(|part| matches!(part, ProviderOutputContentPart::ToolCall { .. }))
        {
            return Err(host_execution_error("mcp.runtime.sampling.tool_output_rejected"));
        }
        Ok(json!({
            "model": response.model_id,
            "content": [{
                "type": "text",
                "text": response.output.full_text,
            }],
            "usage": {
                "inputTokens": response.prompt_tokens,
                "outputTokens": response.completion_tokens,
            },
        }))
    }
}

fn provider_model_is_enabled(
    snapshot: &crate::model_provider::ProviderStatusSnapshot,
    model_id: &str,
) -> bool {
    if snapshot.registry.models.is_empty() {
        return snapshot
            .model_id
            .as_deref()
            .is_some_and(|configured| configured.eq_ignore_ascii_case(model_id));
    }
    snapshot.registry.models.iter().any(|model| {
        model.enabled
            && model.model_id.eq_ignore_ascii_case(model_id)
            && snapshot
                .registry
                .providers
                .iter()
                .any(|provider| provider.provider_id == model.provider_id && provider.enabled)
    })
}

struct McpProductionElicitationPort {
    server_id: String,
    runtime: Weak<GatewayRuntimeState>,
}

#[async_trait]
impl McpElicitationExecutionPort for McpProductionElicitationPort {
    async fn elicit(
        &self,
        request: &McpAuthorizedElicitationRequest,
    ) -> Result<Value, McpHostExecutionError> {
        let runtime = self
            .runtime
            .upgrade()
            .ok_or_else(|| host_execution_error("mcp.runtime.elicitation.runtime_unavailable"))?;
        let approval_id = Ulid::new().to_string();
        let subject_digest = hex::encode(Sha256::digest(
            serde_json::to_vec(&json!({
                "server_id": self.server_id,
                "prompt": request.request.prompt,
                "response_schema": request.request.response_schema_json,
            }))
            .map_err(|_| host_execution_error("mcp.runtime.elicitation.request_invalid"))?,
        ));
        let subject_id = format!("mcp.elicitation.{}:{}", self.server_id, &subject_digest[..16]);
        let timeout_seconds =
            u32::try_from(DEFAULT_ELICITATION_WAIT_TIMEOUT.as_secs()).unwrap_or(u32::MAX);
        let record = runtime
            .create_approval_record(ApprovalCreateRequest {
                approval_id: approval_id.clone(),
                session_id: request.binding.session_id.clone(),
                run_id: format!("mcp-elicitation-{}", &subject_digest[..16]),
                principal: request.binding.principal_id.clone(),
                device_id: request.binding.origin.clone(),
                channel: Some(request.binding.origin.clone()),
                subject_type: ApprovalSubjectType::Tool,
                subject_id: subject_id.clone(),
                request_summary: format!(
                    "MCP server `{}` requested structured user confirmation",
                    self.server_id
                ),
                policy_snapshot: ApprovalPolicySnapshot {
                    policy_id: "mcp.elicitation.host_owned".to_owned(),
                    policy_hash: hex::encode(Sha256::digest(
                        format!(
                            "{}:{}:{}",
                            self.server_id, request.binding.session_id, subject_id
                        )
                        .as_bytes(),
                    )),
                    evaluation_summary:
                        "explicit user presence is required for a session-bound MCP elicitation"
                            .to_owned(),
                },
                prompt: ApprovalPromptRecord {
                    title: format!("MCP confirmation for {}", self.server_id),
                    risk_level: ApprovalRiskLevel::High,
                    subject_id,
                    summary: request.request.prompt.clone(),
                    options: vec![
                        ApprovalPromptOption {
                            option_id: "allow_once".to_owned(),
                            label: "Confirm once".to_owned(),
                            description:
                                "Return the schema-compatible confirmation to this MCP request"
                                    .to_owned(),
                            default_selected: false,
                            decision_scope: ApprovalDecisionScope::Once,
                            timebox_ttl_ms: None,
                        },
                        ApprovalPromptOption {
                            option_id: "deny".to_owned(),
                            label: "Decline".to_owned(),
                            description: "Do not return confirmation to the MCP server".to_owned(),
                            default_selected: true,
                            decision_scope: ApprovalDecisionScope::Once,
                            timebox_ttl_ms: None,
                        },
                    ],
                    timeout_seconds,
                    details_json: json!({
                        "kind": "mcp_elicitation",
                        "server_id": self.server_id,
                        "response_schema": request.request.response_schema_json,
                    })
                    .to_string(),
                    policy_explanation:
                        "MCP elicitation is host-owned and bound to the initiating principal and session"
                            .to_owned(),
                },
            })
            .await
            .map_err(|_| host_execution_error("mcp.runtime.elicitation.approval_create_failed"))?;
        verify_elicitation_approval_binding(&record, &request.binding)?;

        let deadline = tokio::time::Instant::now() + DEFAULT_ELICITATION_WAIT_TIMEOUT;
        loop {
            let current = runtime
                .approval_record(approval_id.clone())
                .await
                .map_err(|_| host_execution_error("mcp.runtime.elicitation.approval_read_failed"))?
                .ok_or_else(|| host_execution_error("mcp.runtime.elicitation.approval_missing"))?;
            verify_elicitation_approval_binding(&current, &request.binding)?;
            if let Some(decision) = current.decision {
                return elicitation_decision_payload(
                    decision,
                    &request.request.response_schema_json,
                );
            }
            if tokio::time::Instant::now() >= deadline {
                let resolved = runtime
                    .resolve_approval_record(ApprovalResolveRequest {
                        approval_id: approval_id.clone(),
                        decision: ApprovalDecision::Timeout,
                        decision_scope: ApprovalDecisionScope::Once,
                        decision_reason: "bounded MCP elicitation wait expired".to_owned(),
                        decision_scope_ttl_ms: None,
                    })
                    .await
                    .map_err(|_| {
                        host_execution_error("mcp.runtime.elicitation.approval_timeout_failed")
                    })?;
                verify_elicitation_approval_binding(&resolved, &request.binding)?;
                return elicitation_decision_payload(
                    resolved.decision.unwrap_or(ApprovalDecision::Timeout),
                    &request.request.response_schema_json,
                );
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

fn verify_elicitation_approval_binding(
    record: &crate::journal::ApprovalRecord,
    binding: &McpCallbackBinding,
) -> Result<(), McpHostExecutionError> {
    if record.principal != binding.principal_id
        || record.session_id != binding.session_id
        || record.device_id != binding.origin
        || record.channel.as_deref() != Some(binding.origin.as_str())
    {
        return Err(host_execution_error("mcp.runtime.elicitation.approval_binding_mismatch"));
    }
    Ok(())
}

fn elicitation_decision_payload(
    decision: ApprovalDecision,
    schema: &Value,
) -> Result<Value, McpHostExecutionError> {
    match decision {
        ApprovalDecision::Allow => approved_elicitation_payload(schema)
            .ok_or_else(|| host_execution_error("mcp.runtime.elicitation.response_unavailable")),
        ApprovalDecision::Deny => Err(host_execution_error("mcp.runtime.elicitation.declined")),
        ApprovalDecision::Timeout => Err(host_execution_error("mcp.runtime.elicitation.cancelled")),
        ApprovalDecision::Error => {
            Err(host_execution_error("mcp.runtime.elicitation.approval_failed"))
        }
    }
}

fn approved_elicitation_payload(schema: &Value) -> Option<Value> {
    if schema == &Value::Bool(true) || schema_explicitly_accepts_true(schema) {
        return Some(Value::Bool(true));
    }
    schema_explicitly_accepts_confirmation_object(schema).then(|| json!({"confirmed": true}))
}

fn schema_explicitly_accepts_true(schema: &Value) -> bool {
    let Some(object) = schema.as_object() else {
        return false;
    };
    const ALLOWED_KEYS: &[&str] =
        &["$id", "$schema", "title", "description", "default", "examples", "type", "const", "enum"];
    if object.keys().any(|key| !ALLOWED_KEYS.contains(&key.as_str())) {
        return false;
    }
    let type_accepts = match object.get("type") {
        Some(Value::String(kind)) => kind == "boolean",
        Some(Value::Array(kinds)) => kinds.iter().any(|kind| kind.as_str() == Some("boolean")),
        Some(_) => false,
        None => object.contains_key("const") || object.contains_key("enum"),
    };
    type_accepts
        && object.get("const").is_none_or(|value| value == &Value::Bool(true))
        && object.get("enum").is_none_or(|values| {
            values.as_array().is_some_and(|values| values.contains(&Value::Bool(true)))
        })
}

fn schema_explicitly_accepts_confirmation_object(schema: &Value) -> bool {
    let Some(object) = schema.as_object() else {
        return false;
    };
    const ALLOWED_KEYS: &[&str] = &[
        "$id",
        "$schema",
        "title",
        "description",
        "default",
        "examples",
        "type",
        "properties",
        "required",
        "additionalProperties",
        "minProperties",
        "maxProperties",
    ];
    if object.keys().any(|key| !ALLOWED_KEYS.contains(&key.as_str()))
        || object.get("type").and_then(Value::as_str) != Some("object")
        || object
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|properties| properties.get("confirmed"))
            .is_none_or(|confirmed| !schema_explicitly_accepts_true(confirmed))
        || object.get("required").is_some_and(|required| {
            required.as_array().is_none_or(|required| {
                required.iter().any(|property| property.as_str() != Some("confirmed"))
            })
        })
        || object.get("minProperties").and_then(Value::as_u64).is_some_and(|minimum| minimum > 1)
        || object.get("maxProperties").and_then(Value::as_u64).is_some_and(|maximum| maximum < 1)
    {
        return false;
    }
    !matches!(object.get("additionalProperties"), Some(Value::Bool(false)))
        || object
            .get("properties")
            .and_then(Value::as_object)
            .is_some_and(|properties| properties.contains_key("confirmed"))
}

fn host_execution_error(reason_code: &str) -> McpHostExecutionError {
    McpHostExecutionError { reason_code: reason_code.to_owned() }
}

fn connector_limits() -> McpConnectorLimits {
    McpConnectorLimits {
        max_frame_bytes: 4 * 1024 * 1024,
        max_http_body_bytes: 4 * 1024 * 1024,
        max_sse_event_bytes: 1024 * 1024,
        max_stderr_tail_bytes: 16 * 1024,
        response_queue_capacity: 128,
        session_idle_timeout_ms: 5 * 60 * 1_000,
    }
}

fn factory_error(reason_code: &str) -> McpActorFactoryError {
    McpActorFactoryError { reason_code: reason_code.to_owned() }
}

struct McpRemoteSessionPort {
    config: McpServerConfig,
    runtime_generation: u64,
    base_url: Url,
    client: Client,
    vault: Arc<Vault>,
    credentials: Option<Arc<McpConfigCredentialPort>>,
    oauth: Option<Arc<McpOAuthRefreshCoordinator>>,
    oauth_request_sequence: AtomicU64,
    sessions: AsyncMutex<BTreeMap<String, RemoteSessionState>>,
}

struct RemoteSessionState {
    upstream_session_id: Option<String>,
    request_url: Url,
    event_stream: Option<Response>,
    pending_stream_bytes: Vec<u8>,
    queued_events: VecDeque<RemoteSseEvent>,
    handshake_requests_remaining: u8,
}

struct RemoteSseEvent {
    raw: Vec<u8>,
    data: Vec<u8>,
    id: Option<String>,
    event_type: Option<String>,
}

impl McpRemoteSessionPort {
    async fn new(
        config: McpServerConfig,
        runtime_generation: u64,
        authority: Arc<McpCatalogAuthority>,
        audit: Arc<GatewayMcpStore>,
        vault: Arc<Vault>,
        runtime: Weak<GatewayRuntimeState>,
    ) -> Result<Self, McpActorFactoryError> {
        if config.egress_policy != McpServerEgressPolicy::Allowlist
            || config.egress_allowlist.is_empty()
        {
            return Err(factory_error("mcp.runtime.remote.egress_denied"));
        }
        let base_url = Url::parse(
            config.url.as_deref().ok_or_else(|| factory_error("mcp.runtime.remote.url_missing"))?,
        )
        .map_err(|_| factory_error("mcp.runtime.remote.url_invalid"))?;
        let verdict = evaluate_remote_target(
            base_url.clone(),
            config.egress_allowlist.clone(),
            connector_limits().max_http_body_bytes,
        )
        .await?;
        let client = build_pinned_remote_client(&base_url, &verdict, DEFAULT_REQUEST_TIMEOUT)?;
        let (credentials, oauth) = match config.oauth_grant.clone() {
            Some(grant) => {
                let credentials = Arc::new(McpConfigCredentialPort {
                    active_access_token_vault_ref: RwLock::new(
                        grant.access_token_vault_ref.clone(),
                    ),
                    grant,
                    vault: Arc::clone(&vault),
                    runtime,
                });
                let coordinator = if credentials.grant.auth_profile_id.is_some() {
                    let credential_port: Arc<dyn McpOAuthCredentialPort> = credentials.clone();
                    Some(Arc::new(
                        McpOAuthRefreshCoordinator::new(
                            config.id.clone(),
                            authority,
                            credential_port,
                            audit,
                        )
                        .map_err(|_| factory_error("mcp.runtime.remote.oauth_invalid"))?,
                    ))
                } else {
                    None
                };
                (Some(credentials), coordinator)
            }
            None if config.oauth_required => {
                return Err(factory_error("mcp.runtime.remote.oauth_grant_missing"));
            }
            None => (None, None),
        };
        Ok(Self {
            config,
            runtime_generation,
            base_url,
            client,
            vault,
            credentials,
            oauth,
            oauth_request_sequence: AtomicU64::new(0),
            sessions: AsyncMutex::new(BTreeMap::new()),
        })
    }

    async fn request(
        &self,
        method: Method,
        url: Url,
        body: Option<Vec<u8>>,
        upstream_session_id: Option<&str>,
        last_event_id: Option<&str>,
    ) -> Result<Response, McpConnectorPortError> {
        self.validate_target(&url).await?;
        let mut request = self
            .client
            .request(method, url)
            .header(ACCEPT, REMOTE_ACCEPT)
            .timeout(DEFAULT_REQUEST_TIMEOUT);
        if let Some(body) = body {
            request = request.header(CONTENT_TYPE, REMOTE_CONTENT_TYPE_JSON).body(body);
        }
        if let Some(session_id) = upstream_session_id {
            request = request.header(REMOTE_SESSION_HEADER, session_id);
        }
        if let Some(last_event_id) = last_event_id {
            request = request.header("last-event-id", last_event_id);
        }
        if let Some(token) = self.oauth_access_token().await? {
            request = request.header(AUTHORIZATION, format!("Bearer {token}"));
        }
        request.send().await.map_err(|_| port_error("mcp.runtime.remote.request_failed"))
    }

    async fn validate_target(&self, url: &Url) -> Result<(), McpConnectorPortError> {
        if !same_origin(&self.base_url, url) {
            return Err(port_error("mcp.runtime.remote.origin_changed"));
        }
        evaluate_remote_target(
            url.clone(),
            self.config.egress_allowlist.clone(),
            connector_limits().max_http_body_bytes,
        )
        .await
        .map(|_| ())
        .map_err(|_| port_error("mcp.runtime.remote.egress_denied"))
    }

    async fn oauth_access_token(&self) -> Result<Option<String>, McpConnectorPortError> {
        let Some(grant) = self.config.oauth_grant.as_ref() else {
            return Ok(None);
        };
        let credentials = self
            .credentials
            .as_ref()
            .ok_or_else(|| port_error("mcp.runtime.remote.oauth_unavailable"))?;
        let now = now_unix_ms();
        let sequence = self.oauth_request_sequence.fetch_add(1, Ordering::Relaxed);
        let refresh_request = McpOAuthRefreshRequest {
            request_id: format!("remote-{}-{}-{}", self.runtime_generation, now.max(1), sequence),
            server_id: self.config.id.clone(),
            credential_scope_id: format!("oauth-{}", self.config.id),
            expected_runtime_generation: self.runtime_generation,
            minimum_valid_until_unix_ms: now.saturating_add(30_000),
            requested_at_unix_ms: now.max(1),
        };
        if let Some(coordinator) = self.oauth.as_ref() {
            coordinator
                .refresh(&refresh_request)
                .await
                .map_err(|_| port_error("mcp.runtime.remote.oauth_unavailable"))?;
        } else {
            credentials
                .refresh(&refresh_request)
                .await
                .map_err(|_| port_error("mcp.runtime.remote.oauth_unavailable"))?;
        }
        let access_token_vault_ref = credentials.access_token_vault_ref()?;
        debug_assert!(
            grant.auth_profile_id.is_some()
                || access_token_vault_ref == grant.access_token_vault_ref
        );
        load_vault_text(&self.vault, access_token_vault_ref.as_str())
            .map(Some)
            .map_err(|_| port_error("mcp.runtime.remote.oauth_secret_unavailable"))
    }

    async fn open_http(
        &self,
        request: &McpHttpSessionOpenRequest,
    ) -> Result<McpHttpSessionResponse, McpConnectorPortError> {
        let response = self
            .request(Method::POST, self.base_url.clone(), Some(request.body.clone()), None, None)
            .await?;
        let status = response.status();
        let content_type = response_content_type(&response);
        let upstream_session_id = response
            .headers()
            .get(REMOTE_SESSION_HEADER)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let body = read_bounded_response(response, request.max_response_bytes).await?;
        let session_id = host_session_id(
            request.server_id.as_str(),
            request.runtime_generation,
            upstream_session_id.as_deref(),
        );
        self.sessions.lock().await.insert(
            session_id.clone(),
            RemoteSessionState {
                upstream_session_id,
                request_url: self.base_url.clone(),
                event_stream: None,
                pending_stream_bytes: Vec::new(),
                queued_events: VecDeque::new(),
                handshake_requests_remaining: 0,
            },
        );
        Ok(remote_response(status, Some(session_id), content_type, body, None, false))
    }

    async fn open_sse(
        &self,
        request: &McpHttpSessionOpenRequest,
    ) -> Result<McpHttpSessionResponse, McpConnectorPortError> {
        let mut event_stream =
            self.request(Method::GET, self.base_url.clone(), None, None, None).await?;
        if !event_stream.status().is_success() {
            return Ok(remote_response(
                event_stream.status(),
                None,
                response_content_type(&event_stream),
                Vec::new(),
                None,
                true,
            ));
        }
        let mut pending_stream_bytes = Vec::new();
        let endpoint_event = read_next_sse_event(
            &mut event_stream,
            &mut pending_stream_bytes,
            request.max_response_bytes,
        )
        .await?;
        if endpoint_event.event_type.as_deref() != Some("endpoint") {
            return Err(port_error("mcp.runtime.sse.endpoint_event_missing"));
        }
        let endpoint_text = std::str::from_utf8(&endpoint_event.data)
            .map_err(|_| port_error("mcp.runtime.sse.endpoint_invalid"))?;
        let request_url = self
            .base_url
            .join(endpoint_text.trim())
            .map_err(|_| port_error("mcp.runtime.sse.endpoint_invalid"))?;
        self.validate_target(&request_url).await?;
        let initialize_response = self
            .request(Method::POST, request_url.clone(), Some(request.body.clone()), None, None)
            .await?;
        if !initialize_response.status().is_success() {
            return Ok(remote_response(
                initialize_response.status(),
                None,
                response_content_type(&initialize_response),
                read_bounded_response(initialize_response, request.max_response_bytes).await?,
                None,
                false,
            ));
        }
        let initialize_id = jsonrpc_id(request.body.as_slice())?
            .ok_or_else(|| port_error("mcp.runtime.sse.initialize_id_missing"))?;
        let mut queued_events = VecDeque::new();
        let initialize_event = read_matching_sse_event(
            &mut event_stream,
            &mut pending_stream_bytes,
            &initialize_id,
            request.max_response_bytes,
            &mut queued_events,
        )
        .await?;
        let session_id =
            host_session_id(request.server_id.as_str(), request.runtime_generation, None);
        let last_event_id = initialize_event.id.clone();
        self.sessions.lock().await.insert(
            session_id.clone(),
            RemoteSessionState {
                upstream_session_id: None,
                request_url,
                event_stream: Some(event_stream),
                pending_stream_bytes,
                queued_events,
                handshake_requests_remaining: 3,
            },
        );
        Ok(remote_response(
            StatusCode::OK,
            Some(session_id),
            REMOTE_CONTENT_TYPE_JSON.to_owned(),
            initialize_event.data,
            last_event_id,
            false,
        ))
    }
}

#[async_trait]
impl McpHttpSessionPort for McpRemoteSessionPort {
    async fn open(
        &self,
        request: &McpHttpSessionOpenRequest,
    ) -> Result<McpHttpSessionResponse, McpConnectorPortError> {
        self.validate_request_identity(
            request.server_id.as_str(),
            request.runtime_generation,
            request.max_response_bytes,
        )?;
        match self.config.transport {
            McpServerTransport::Http => self.open_http(request).await,
            McpServerTransport::Sse => self.open_sse(request).await,
            McpServerTransport::Stdio => Err(port_error("mcp.runtime.remote.transport_mismatch")),
        }
    }

    async fn exchange(
        &self,
        request: &McpHttpSessionExchangeRequest,
    ) -> Result<McpHttpSessionResponse, McpConnectorPortError> {
        self.validate_request_identity(
            request.server_id.as_str(),
            request.runtime_generation,
            request.max_response_bytes,
        )?;
        let (request_url, upstream_session_id) = {
            let sessions = self.sessions.lock().await;
            let state = sessions
                .get(request.session_id.as_str())
                .ok_or_else(|| port_error("mcp.runtime.remote.session_missing"))?;
            (state.request_url.clone(), state.upstream_session_id.clone())
        };
        let response = self
            .request(
                Method::POST,
                request_url,
                Some(request.body.clone()),
                upstream_session_id.as_deref(),
                None,
            )
            .await?;
        let status = response.status();
        let content_type = response_content_type(&response);
        let body = read_bounded_response(response, request.max_response_bytes).await?;
        if self.config.transport == McpServerTransport::Http {
            return Ok(remote_response(
                status,
                Some(request.session_id.clone()),
                content_type,
                body,
                None,
                false,
            ));
        }
        if !status.is_success() {
            return Ok(remote_response(
                status,
                Some(request.session_id.clone()),
                content_type,
                body,
                None,
                false,
            ));
        }
        let Some(request_id) = jsonrpc_id(request.body.as_slice())? else {
            return Ok(remote_response(
                status,
                Some(request.session_id.clone()),
                REMOTE_CONTENT_TYPE_JSON.to_owned(),
                Vec::new(),
                None,
                false,
            ));
        };
        let mut sessions = self.sessions.lock().await;
        let state = sessions
            .get_mut(request.session_id.as_str())
            .ok_or_else(|| port_error("mcp.runtime.remote.session_missing"))?;
        if state.handshake_requests_remaining == 0 {
            return Ok(remote_response(
                status,
                Some(request.session_id.clone()),
                REMOTE_CONTENT_TYPE_JSON.to_owned(),
                Vec::new(),
                None,
                false,
            ));
        }
        let event_stream = state
            .event_stream
            .as_mut()
            .ok_or_else(|| port_error("mcp.runtime.sse.stream_missing"))?;
        let event = read_matching_sse_event(
            event_stream,
            &mut state.pending_stream_bytes,
            &request_id,
            request.max_response_bytes,
            &mut state.queued_events,
        )
        .await?;
        state.handshake_requests_remaining = state.handshake_requests_remaining.saturating_sub(1);
        Ok(remote_response(
            StatusCode::OK,
            Some(request.session_id.clone()),
            REMOTE_CONTENT_TYPE_JSON.to_owned(),
            event.data,
            event.id,
            false,
        ))
    }

    async fn next_event(
        &self,
        request: &McpHttpSessionEventRequest,
    ) -> Result<McpHttpSessionResponse, McpConnectorPortError> {
        self.validate_request_identity(
            request.server_id.as_str(),
            request.runtime_generation,
            request.max_response_bytes,
        )?;
        let mut sessions = self.sessions.lock().await;
        let state = sessions
            .get_mut(request.session_id.as_str())
            .ok_or_else(|| port_error("mcp.runtime.remote.session_missing"))?;
        if let Some(event) = state.queued_events.pop_front() {
            return Ok(remote_response(
                StatusCode::OK,
                Some(request.session_id.clone()),
                REMOTE_CONTENT_TYPE_SSE.to_owned(),
                event.raw,
                event.id,
                false,
            ));
        }
        if self.config.transport == McpServerTransport::Sse {
            let event_stream = state
                .event_stream
                .as_mut()
                .ok_or_else(|| port_error("mcp.runtime.sse.stream_missing"))?;
            let event = read_next_sse_event(
                event_stream,
                &mut state.pending_stream_bytes,
                request.max_response_bytes,
            )
            .await?;
            return Ok(remote_response(
                StatusCode::OK,
                Some(request.session_id.clone()),
                REMOTE_CONTENT_TYPE_SSE.to_owned(),
                event.raw,
                event.id,
                false,
            ));
        }
        if state.event_stream.is_none() {
            let mut event_request = self
                .client
                .get(state.request_url.clone())
                .header(ACCEPT, REMOTE_CONTENT_TYPE_SSE)
                .timeout(DEFAULT_REQUEST_TIMEOUT);
            if let Some(session_id) = state.upstream_session_id.as_deref() {
                event_request = event_request.header(REMOTE_SESSION_HEADER, session_id);
            }
            if let Some(last_event_id) = request.last_event_id.as_deref() {
                event_request = event_request.header("last-event-id", last_event_id);
            }
            if let Some(token) = self.oauth_access_token().await? {
                event_request = event_request.header(AUTHORIZATION, format!("Bearer {token}"));
            }
            self.validate_target(&state.request_url).await?;
            state.event_stream = Some(
                event_request
                    .send()
                    .await
                    .map_err(|_| port_error("mcp.runtime.remote.event_request_failed"))?,
            );
        }
        let event_stream = state
            .event_stream
            .as_mut()
            .ok_or_else(|| port_error("mcp.runtime.remote.event_stream_missing"))?;
        let status = event_stream.status();
        let content_type = response_content_type(event_stream);
        let chunk = event_stream
            .chunk()
            .await
            .map_err(|_| port_error("mcp.runtime.remote.event_read_failed"))?;
        let stream_closed = chunk.is_none();
        let body = chunk.map(|bytes| bytes.to_vec()).unwrap_or_default();
        if body.len() > request.max_response_bytes {
            return Err(port_error("mcp.runtime.remote.response_too_large"));
        }
        Ok(remote_response(
            status,
            Some(request.session_id.clone()),
            content_type,
            body,
            request.last_event_id.clone(),
            stream_closed,
        ))
    }

    async fn close(
        &self,
        request: &McpHttpSessionCloseRequest,
    ) -> Result<McpHttpSessionResponse, McpConnectorPortError> {
        self.validate_request_identity(
            request.server_id.as_str(),
            request.runtime_generation,
            request.max_response_bytes,
        )?;
        let Some(state) = self.sessions.lock().await.remove(request.session_id.as_str()) else {
            return Ok(remote_response(
                StatusCode::NO_CONTENT,
                Some(request.session_id.clone()),
                REMOTE_CONTENT_TYPE_JSON.to_owned(),
                Vec::new(),
                None,
                true,
            ));
        };
        if self.config.transport == McpServerTransport::Sse {
            return Ok(remote_response(
                StatusCode::NO_CONTENT,
                Some(request.session_id.clone()),
                REMOTE_CONTENT_TYPE_JSON.to_owned(),
                Vec::new(),
                None,
                true,
            ));
        }
        let response = self
            .request(
                Method::DELETE,
                state.request_url,
                None,
                state.upstream_session_id.as_deref(),
                None,
            )
            .await?;
        let status = response.status();
        let content_type = response_content_type(&response);
        let body = read_bounded_response(response, request.max_response_bytes).await?;
        Ok(remote_response(
            status,
            Some(request.session_id.clone()),
            content_type,
            body,
            None,
            true,
        ))
    }
}

impl McpRemoteSessionPort {
    fn validate_request_identity(
        &self,
        server_id: &str,
        runtime_generation: u64,
        max_response_bytes: usize,
    ) -> Result<(), McpConnectorPortError> {
        if server_id != self.config.id || runtime_generation != self.runtime_generation {
            return Err(port_error("mcp.runtime.remote.stale_identity"));
        }
        if max_response_bytes == 0 || max_response_bytes > connector_limits().max_http_body_bytes {
            return Err(port_error("mcp.runtime.remote.response_budget_invalid"));
        }
        Ok(())
    }
}

struct McpConfigCredentialPort {
    grant: McpServerOAuthGrant,
    vault: Arc<Vault>,
    runtime: Weak<GatewayRuntimeState>,
    active_access_token_vault_ref: RwLock<String>,
}

impl McpConfigCredentialPort {
    fn access_token_vault_ref(&self) -> Result<String, McpConnectorPortError> {
        self.active_access_token_vault_ref
            .read()
            .map(|value| value.clone())
            .map_err(|_| port_error("mcp.runtime.oauth.credential_state_poisoned"))
    }
}

#[async_trait]
impl McpOAuthCredentialPort for McpConfigCredentialPort {
    async fn refresh(
        &self,
        request: &McpOAuthRefreshRequest,
    ) -> Result<McpOAuthCredentialLease, McpOAuthCredentialError> {
        if self.grant.revoked_at_unix_ms.is_some() {
            return Err(oauth_error("mcp.runtime.oauth.grant_revoked"));
        }
        if let Some(profile_id) = self.grant.auth_profile_id.as_deref() {
            let runtime = self
                .runtime
                .upgrade()
                .ok_or_else(|| oauth_error("mcp.runtime.oauth.auth_runtime_unavailable"))?;
            let lease = runtime
                .ensure_mcp_oauth_profile(profile_id, request.minimum_valid_until_unix_ms)
                .await
                .map_err(|_| oauth_error("mcp.runtime.oauth.auth_profile_refresh_failed"))?;
            load_vault_text(&self.vault, lease.access_token_vault_ref.as_str())
                .map_err(|_| oauth_error("mcp.runtime.oauth.auth_profile_secret_unavailable"))?;
            *self
                .active_access_token_vault_ref
                .write()
                .map_err(|_| oauth_error("mcp.runtime.oauth.credential_state_poisoned"))? =
                lease.access_token_vault_ref;
            return Ok(McpOAuthCredentialLease {
                credential_handle_id: format!(
                    "auth-profile-{}",
                    &hex::encode(Sha256::digest(profile_id.as_bytes()))[..16]
                ),
                expires_at_unix_ms: lease.expires_at_unix_ms,
                evidence_sha256: lease.evidence_sha256,
            });
        }
        let expires_at_unix_ms = self
            .grant
            .expires_at_unix_ms
            .unwrap_or_else(|| request.minimum_valid_until_unix_ms.saturating_add(86_400_000));
        if expires_at_unix_ms < request.minimum_valid_until_unix_ms
            || load_vault_text(&self.vault, self.grant.access_token_vault_ref.as_str()).is_err()
        {
            return Err(oauth_error("mcp.runtime.oauth.refresh_required"));
        }
        let evidence_sha256 = hex::encode(Sha256::digest(
            format!(
                "{}\0{}\0{}\0{}",
                self.grant.grant_id,
                self.grant.metadata_vault_ref,
                self.grant.updated_at_unix_ms,
                self.grant.rotation_id.as_deref().unwrap_or_default()
            )
            .as_bytes(),
        ));
        Ok(McpOAuthCredentialLease {
            credential_handle_id: format!("vault-{}", self.grant.grant_id),
            expires_at_unix_ms,
            evidence_sha256,
        })
    }
}

async fn evaluate_remote_target(
    url: Url,
    allowed_hosts: Vec<String>,
    max_response_bytes: usize,
) -> Result<EgressPolicyVerdict, McpActorFactoryError> {
    tokio::task::spawn_blocking(move || {
        EgressProxyPolicyService.evaluate_request(&EgressProxyRequest {
            method: "POST",
            url: url.as_str(),
            allow_private_targets: remote_url_targets_loopback(&url),
            allowed_hosts: allowed_hosts.as_slice(),
            allowed_dns_suffixes: &[],
            max_response_bytes,
            credential_bindings: &[],
        })
    })
    .await
    .map_err(|_| factory_error("mcp.runtime.remote.egress_worker_failed"))?
    .map_err(|_| factory_error("mcp.runtime.remote.egress_denied"))
}

fn build_pinned_remote_client(
    url: &Url,
    verdict: &EgressPolicyVerdict,
    timeout: Duration,
) -> Result<Client, McpActorFactoryError> {
    let host = url.host_str().ok_or_else(|| factory_error("mcp.runtime.remote.host_missing"))?;
    let mut builder =
        Client::builder().redirect(Policy::none()).connect_timeout(timeout).timeout(timeout);
    if host.parse::<IpAddr>().is_err() {
        for address in &verdict.resolved_addresses {
            builder = builder.resolve(host, *address);
        }
    }
    builder.build().map_err(|_| factory_error("mcp.runtime.remote.client_build_failed"))
}

async fn read_bounded_response(
    mut response: Response,
    max_response_bytes: usize,
) -> Result<Vec<u8>, McpConnectorPortError> {
    let mut body = Vec::new();
    while let Some(chunk) =
        response.chunk().await.map_err(|_| port_error("mcp.runtime.remote.read_failed"))?
    {
        if body.len().saturating_add(chunk.len()) > max_response_bytes {
            return Err(port_error("mcp.runtime.remote.response_too_large"));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

async fn read_next_sse_event(
    response: &mut Response,
    pending: &mut Vec<u8>,
    max_event_bytes: usize,
) -> Result<RemoteSseEvent, McpConnectorPortError> {
    loop {
        if let Some(end) = sse_event_end(pending.as_slice()) {
            let tail = pending.split_off(end);
            let raw = std::mem::replace(pending, tail);
            return parse_remote_sse_event(raw);
        }
        let chunk = response
            .chunk()
            .await
            .map_err(|_| port_error("mcp.runtime.sse.read_failed"))?
            .ok_or_else(|| port_error("mcp.runtime.sse.stream_closed"))?;
        if pending.len().saturating_add(chunk.len()) > max_event_bytes {
            return Err(port_error("mcp.runtime.sse.event_too_large"));
        }
        pending.extend_from_slice(&chunk);
    }
}

async fn read_matching_sse_event(
    response: &mut Response,
    pending: &mut Vec<u8>,
    expected_id: &serde_json::Value,
    max_event_bytes: usize,
    queued_events: &mut VecDeque<RemoteSseEvent>,
) -> Result<RemoteSseEvent, McpConnectorPortError> {
    for _ in 0..128 {
        let event = read_next_sse_event(response, pending, max_event_bytes).await?;
        if serde_json::from_slice::<serde_json::Value>(&event.data)
            .ok()
            .and_then(|value| value.get("id").cloned())
            .as_ref()
            == Some(expected_id)
        {
            return Ok(event);
        }
        queued_events.push_back(event);
    }
    Err(port_error("mcp.runtime.sse.response_not_found"))
}

fn parse_remote_sse_event(raw: Vec<u8>) -> Result<RemoteSseEvent, McpConnectorPortError> {
    let text = std::str::from_utf8(&raw).map_err(|_| port_error("mcp.runtime.sse.invalid_utf8"))?;
    let mut event_type = None;
    let mut id = None;
    let mut data = Vec::new();
    for line in text.lines() {
        if let Some(value) = line.strip_prefix("event:") {
            event_type = Some(value.trim_start().to_owned());
        } else if let Some(value) = line.strip_prefix("id:") {
            id = Some(value.trim_start().to_owned());
        } else if let Some(value) = line.strip_prefix("data:") {
            if !data.is_empty() {
                data.push(b'\n');
            }
            data.extend_from_slice(value.trim_start().as_bytes());
        }
    }
    if data.is_empty() {
        return Err(port_error("mcp.runtime.sse.data_missing"));
    }
    Ok(RemoteSseEvent { raw, data, id, event_type })
}

fn sse_event_end(bytes: &[u8]) -> Option<usize> {
    bytes.windows(2).position(|window| window == b"\n\n").map(|index| index + 2).or_else(|| {
        bytes.windows(4).position(|window| window == b"\r\n\r\n").map(|index| index + 4)
    })
}

fn jsonrpc_id(body: &[u8]) -> Result<Option<serde_json::Value>, McpConnectorPortError> {
    serde_json::from_slice::<serde_json::Value>(body)
        .map(|value| value.get("id").cloned())
        .map_err(|_| port_error("mcp.runtime.remote.request_invalid"))
}

fn remote_response(
    status: StatusCode,
    session_id: Option<String>,
    content_type: String,
    body: Vec<u8>,
    last_event_id: Option<String>,
    stream_closed: bool,
) -> McpHttpSessionResponse {
    McpHttpSessionResponse {
        status: status.as_u16(),
        session_id,
        content_type,
        body,
        expires_at_unix_ms: None,
        last_event_id,
        stream_closed,
    }
}

fn response_content_type(response: &Response) -> String {
    response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or(REMOTE_CONTENT_TYPE_JSON)
        .to_owned()
}

fn host_session_id(server_id: &str, generation: u64, upstream: Option<&str>) -> String {
    upstream
        .filter(|value| {
            !value.is_empty() && value.len() <= 512 && !value.chars().any(char::is_control)
        })
        .map(str::to_owned)
        .unwrap_or_else(|| format!("host-{server_id}-{generation}"))
}

fn same_origin(left: &Url, right: &Url) -> bool {
    left.scheme() == right.scheme()
        && left.host_str().map(str::to_ascii_lowercase)
            == right.host_str().map(str::to_ascii_lowercase)
        && left.port_or_known_default() == right.port_or_known_default()
}

fn remote_url_targets_loopback(url: &Url) -> bool {
    url.host_str().is_some_and(|host| {
        host.eq_ignore_ascii_case("localhost")
            || host.parse::<IpAddr>().is_ok_and(|address| address.is_loopback())
    })
}

fn load_vault_text(vault: &Vault, raw_ref: &str) -> Result<String, ()> {
    let reference = VaultRef::parse(raw_ref.trim_start_matches("vault://")).map_err(|_| ())?;
    let bytes = vault.get_secret(&reference.scope, reference.key.as_str()).map_err(|_| ())?;
    String::from_utf8(bytes).map_err(|_| ())
}

fn oauth_error(reason_code: &str) -> McpOAuthCredentialError {
    McpOAuthCredentialError { reason_code: reason_code.to_owned() }
}

struct McpStdioProcessLauncher {
    configs: Arc<BTreeMap<String, McpServerConfig>>,
    startup_cwd: PathBuf,
    vault: Arc<Vault>,
    runtime: Weak<GatewayRuntimeState>,
}

#[async_trait]
impl McpProcessLauncher for McpStdioProcessLauncher {
    async fn launch(
        &self,
        request: &McpProcessLaunchRequest,
    ) -> Result<McpLaunchedProcessSession, McpConnectorPortError> {
        let config = self
            .configs
            .get(request.launch_profile_id.as_str())
            .ok_or_else(|| port_error("mcp.runtime.stdio.profile_missing"))?;
        let command = config
            .command
            .as_ref()
            .filter(|command| !command.is_empty())
            .ok_or_else(|| port_error("mcp.runtime.stdio.command_missing"))?;
        let executable = resolve_executable(command[0].as_str(), &self.startup_cwd)
            .ok_or_else(|| port_error("mcp.runtime.stdio.executable_unresolved"))?;
        let env = resolve_stdio_environment(config, &self.vault)?;
        let runtime =
            self.runtime.upgrade().ok_or_else(|| port_error("mcp.runtime.gateway_dropped"))?;
        let process_config = ManagedStdioProcessConfig {
            executable,
            args: command.iter().skip(1).cloned().collect(),
            cwd: self.startup_cwd.clone(),
            env,
            generation: request.runtime_generation,
            lease_duration: DEFAULT_PROCESS_LEASE,
        };
        let process_runner = runtime.config.tool_call.process_runner.clone();
        let configured_command = command[0].clone();
        let process = tokio::task::spawn_blocking(move || {
            spawn_sandboxed_managed_stdio_process(
                &process_runner,
                configured_command.as_str(),
                &process_config,
            )
        })
        .await
        .map_err(|_| port_error("mcp.runtime.stdio.launch_worker_failed"))?
        .map_err(|_| port_error("mcp.runtime.stdio.launch_failed"))?;
        let descriptor = mcp_process_descriptor(
            process.lease(),
            request.launch_profile_id.as_str(),
            now_unix_ms(),
        )?;
        if runtime
            .journal_store
            .register_process_handle_and_lease(&descriptor, process.lease())
            .is_err()
        {
            let _ = tokio::task::spawn_blocking(move || process.cleanup(true)).await;
            return Err(port_error("mcp.runtime.stdio.process_registration_failed"));
        }
        launched_stdio_session(
            process,
            request.max_chunk_bytes,
            Arc::downgrade(&runtime),
            descriptor,
        )
    }
}

fn mcp_process_descriptor(
    lease: &palyra_common::runtime_contracts::ProcessLeaseV1,
    server_id: &str,
    now: i64,
) -> Result<RuntimeHandleDescriptorV1, McpConnectorPortError> {
    let descriptor = RuntimeHandleDescriptorV1 {
        schema_version: lease.schema_version,
        instance_id: lease.instance_id.clone(),
        kind: RuntimeHandleKind::Process,
        session_id: None,
        run_id: None,
        generation: lease.generation,
        owner: format!("mcp:{server_id}"),
        state: RuntimeHandleState::Running,
        resume_metadata_json: Some(
            json!({
                "schema_version": 1,
                "pid": lease.pid,
                "server_id_sha256": hex::encode(Sha256::digest(server_id.as_bytes())),
                "transport": "stdio",
            })
            .to_string(),
        ),
        created_at_unix_ms: now,
        updated_at_unix_ms: now,
    };
    descriptor
        .validate()
        .map_err(|_| port_error("mcp.runtime.stdio.process_descriptor_invalid"))?;
    Ok(descriptor)
}

fn resolve_stdio_environment(
    config: &McpServerConfig,
    vault: &Vault,
) -> Result<BTreeMap<String, String>, McpConnectorPortError> {
    // The sandbox runner supplies its own minimal PATH, locale, and temporary-directory
    // environment. Inheriting host process variables here would either be rejected by its
    // reserved-key guard or let a configured server influence the trusted launch baseline.
    let mut env = BTreeMap::new();
    for binding in &config.env_vault_refs {
        let reference = VaultRef::parse(binding.vault_ref.trim_start_matches("vault://"))
            .map_err(|_| port_error("mcp.runtime.stdio.vault_ref_invalid"))?;
        let value = vault
            .get_secret(&reference.scope, reference.key.as_str())
            .map_err(|_| port_error("mcp.runtime.stdio.vault_secret_unavailable"))?;
        let value = String::from_utf8(value)
            .map_err(|_| port_error("mcp.runtime.stdio.vault_secret_not_utf8"))?;
        env.insert(binding.name.clone(), value);
    }
    Ok(env)
}

fn resolve_executable(command: &str, startup_cwd: &Path) -> Option<PathBuf> {
    let candidate = PathBuf::from(command);
    if candidate.is_absolute() {
        return candidate.is_file().then_some(candidate);
    }
    if candidate.components().count() > 1 {
        let resolved = startup_cwd.join(candidate).canonicalize().ok()?;
        return (resolved.is_file() && resolved.starts_with(startup_cwd)).then_some(resolved);
    }
    crate::application::managed_coding_services::resolve_trusted_executable(command)
}

fn launched_stdio_session(
    mut process: ManagedStdioProcess,
    max_chunk_bytes: usize,
    runtime: Weak<GatewayRuntimeState>,
    descriptor: RuntimeHandleDescriptorV1,
) -> Result<McpLaunchedProcessSession, McpConnectorPortError> {
    let io = (|| {
        let stdin =
            process.take_stdin().map_err(|_| port_error("mcp.runtime.stdio.stdin_unavailable"))?;
        let stdout = process
            .take_stdout()
            .map_err(|_| port_error("mcp.runtime.stdio.stdout_unavailable"))?;
        let stderr = process
            .take_stderr()
            .map_err(|_| port_error("mcp.runtime.stdio.stderr_unavailable"))?;
        let writer = spawn_byte_writer(stdin)?;
        let stdout = spawn_byte_reader(stdout, max_chunk_bytes, "stdout")?;
        let stderr = spawn_byte_reader(stderr, max_chunk_bytes, "stderr")?;
        Ok::<_, McpConnectorPortError>((writer, stdout, stderr))
    })();
    let (writer, stdout, stderr) = match io {
        Ok(io) => io,
        Err(error) => {
            let _ = cleanup_registered_mcp_process(process, &runtime, &descriptor);
            return Err(error);
        }
    };
    let process = Arc::new(Mutex::new(Some(process)));
    Ok(McpLaunchedProcessSession::new(
        Box::new(writer),
        Box::new(stdout),
        Some(Box::new(stderr)),
        Box::new(OwnedProcessControl { process, runtime, descriptor }),
    ))
}

enum WriterCommand {
    Frame { bytes: Vec<u8>, reply: oneshot::Sender<Result<(), McpConnectorPortError>> },
    Close { reply: oneshot::Sender<Result<(), McpConnectorPortError>> },
}

struct ThreadByteWriter {
    commands: Option<std_mpsc::SyncSender<WriterCommand>>,
}

fn spawn_byte_writer(
    mut stdin: std::process::ChildStdin,
) -> Result<ThreadByteWriter, McpConnectorPortError> {
    let (commands, receiver) = std_mpsc::sync_channel(IO_CHANNEL_CAPACITY);
    thread::Builder::new()
        .name("palyra-mcp-stdio-writer".to_owned())
        .spawn(move || {
            while let Ok(command) = receiver.recv() {
                match command {
                    WriterCommand::Frame { bytes, reply } => {
                        let result = stdin
                            .write_all(&bytes)
                            .and_then(|_| stdin.flush())
                            .map_err(|_| port_error("mcp.runtime.stdio.write_failed"));
                        let failed = result.is_err();
                        let _ = reply.send(result);
                        if failed {
                            break;
                        }
                    }
                    WriterCommand::Close { reply } => {
                        drop(stdin);
                        let _ = reply.send(Ok(()));
                        break;
                    }
                }
            }
        })
        .map_err(|_| port_error("mcp.runtime.stdio.writer_thread_failed"))?;
    Ok(ThreadByteWriter { commands: Some(commands) })
}

#[async_trait]
impl McpByteWriter for ThreadByteWriter {
    async fn write_frame(&mut self, frame: &[u8]) -> Result<(), McpConnectorPortError> {
        let sender =
            self.commands.as_ref().ok_or_else(|| port_error("mcp.runtime.stdio.writer_closed"))?;
        let (reply, response) = oneshot::channel();
        sender.try_send(WriterCommand::Frame { bytes: frame.to_vec(), reply }).map_err(
            |error| match error {
                std_mpsc::TrySendError::Full(_) => {
                    port_error("mcp.runtime.stdio.writer_backpressure")
                }
                std_mpsc::TrySendError::Disconnected(_) => {
                    port_error("mcp.runtime.stdio.writer_closed")
                }
            },
        )?;
        response.await.map_err(|_| port_error("mcp.runtime.stdio.writer_closed"))?
    }

    async fn close(&mut self) -> Result<(), McpConnectorPortError> {
        let Some(sender) = self.commands.take() else {
            return Ok(());
        };
        let (reply, response) = oneshot::channel();
        sender
            .try_send(WriterCommand::Close { reply })
            .map_err(|_| port_error("mcp.runtime.stdio.writer_close_failed"))?;
        response.await.map_err(|_| port_error("mcp.runtime.stdio.writer_closed"))?
    }
}

struct ThreadByteReader {
    chunks: mpsc::Receiver<Result<Vec<u8>, McpConnectorPortError>>,
    pending: VecDeque<u8>,
    max_chunk_bytes: usize,
}

fn spawn_byte_reader<R>(
    mut reader: R,
    max_chunk_bytes: usize,
    stream: &'static str,
) -> Result<ThreadByteReader, McpConnectorPortError>
where
    R: Read + Send + 'static,
{
    let capacity = IO_CHANNEL_CAPACITY;
    let (sender, chunks) = mpsc::channel(capacity);
    thread::Builder::new()
        .name(format!("palyra-mcp-{stream}-reader"))
        .spawn(move || {
            let mut buffer = vec![0_u8; IO_THREAD_CHUNK_BYTES];
            loop {
                match reader.read(buffer.as_mut_slice()) {
                    Ok(0) => break,
                    Ok(read) => {
                        if sender.blocking_send(Ok(buffer[..read].to_vec())).is_err() {
                            break;
                        }
                    }
                    Err(_) => {
                        let _ =
                            sender.blocking_send(Err(port_error("mcp.runtime.stdio.read_failed")));
                        break;
                    }
                }
            }
        })
        .map_err(|_| port_error("mcp.runtime.stdio.reader_thread_failed"))?;
    Ok(ThreadByteReader {
        chunks,
        pending: VecDeque::new(),
        max_chunk_bytes: max_chunk_bytes.max(1),
    })
}

#[async_trait]
impl McpByteReader for ThreadByteReader {
    async fn read_chunk(
        &mut self,
        max_bytes: usize,
    ) -> Result<Option<Vec<u8>>, McpConnectorPortError> {
        let requested = max_bytes.min(self.max_chunk_bytes).max(1);
        if self.pending.is_empty() {
            match self.chunks.recv().await {
                Some(Ok(chunk)) => self.pending.extend(chunk),
                Some(Err(error)) => return Err(error),
                None => return Ok(None),
            }
        }
        let length = requested.min(self.pending.len());
        Ok(Some(self.pending.drain(..length).collect()))
    }
}

struct OwnedProcessControl {
    process: Arc<Mutex<Option<ManagedStdioProcess>>>,
    runtime: Weak<GatewayRuntimeState>,
    descriptor: RuntimeHandleDescriptorV1,
}

#[async_trait]
impl McpProcessControl for OwnedProcessControl {
    async fn close(&mut self) -> Result<McpProcessCloseEvidence, McpConnectorPortError> {
        let process = self
            .process
            .lock()
            .map_err(|_| port_error("mcp.runtime.stdio.process_lock_poisoned"))?
            .take();
        let Some(process) = process else {
            return Ok(McpProcessCloseEvidence {
                process_exited: true,
                descendants_remaining: 0,
                reason_code: "mcp.runtime.stdio.already_closed".to_owned(),
            });
        };
        let runtime = self.runtime.clone();
        let descriptor = self.descriptor.clone();
        tokio::task::spawn_blocking(move || {
            cleanup_registered_mcp_process(process, &runtime, &descriptor)
        })
        .await
        .map_err(|_| port_error("mcp.runtime.stdio.cleanup_worker_failed"))?
    }
}

impl Drop for OwnedProcessControl {
    fn drop(&mut self) {
        let process = self.process.lock().ok().and_then(|mut process| process.take());
        if let Some(process) = process {
            let _ = cleanup_registered_mcp_process(process, &self.runtime, &self.descriptor);
        }
    }
}

fn cleanup_registered_mcp_process(
    process: ManagedStdioProcess,
    runtime: &Weak<GatewayRuntimeState>,
    descriptor: &RuntimeHandleDescriptorV1,
) -> Result<McpProcessCloseEvidence, McpConnectorPortError> {
    let report = process.cleanup(true);
    let process_exited = report.outcome == CleanupOutcome::Completed;
    let mut terminal = descriptor.clone();
    terminal.state =
        if process_exited { RuntimeHandleState::Closed } else { RuntimeHandleState::Orphaned };
    terminal.updated_at_unix_ms = report.completed_at_unix_ms.max(terminal.created_at_unix_ms);
    let runtime = runtime.upgrade().ok_or_else(|| port_error("mcp.runtime.gateway_dropped"))?;
    runtime
        .journal_store
        .finalize_process_cleanup(&terminal, &report)
        .map_err(|_| port_error("mcp.runtime.stdio.cleanup_persistence_failed"))?;
    Ok(McpProcessCloseEvidence {
        process_exited,
        descendants_remaining: u32::from(!process_exited),
        reason_code: report.reason_code,
    })
}

fn port_error(reason_code: &str) -> McpConnectorPortError {
    McpConnectorPortError { reason_code: reason_code.to_owned() }
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(1)
        .max(1)
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read as _, Write as _},
        net::{TcpListener, TcpStream},
        thread,
        time::Duration,
    };

    use super::*;
    use crate::application::local_resource_governor::LocalResourceGovernorConfig;

    #[test]
    fn broker_worker_admission_rejects_over_capacity_and_recovers() {
        let worker_slots = Arc::new(Semaphore::new(1));
        let active = acquire_broker_worker_slot(&worker_slots).expect("first worker is admitted");

        let error = acquire_broker_worker_slot(&worker_slots)
            .expect_err("a concurrent worker must receive backpressure");
        assert_eq!(error.reason_code, "mcp.broker_backpressure");

        drop(active);
        assert!(acquire_broker_worker_slot(&worker_slots).is_ok());
    }

    struct UnavailableConnector;

    #[async_trait]
    impl McpSessionConnector for UnavailableConnector {
        async fn connect(
            &self,
            _request: &McpConnectRequest,
        ) -> Result<Box<dyn McpTransportSession>, McpTransportError> {
            Err(resource_transport_error("mcp.runtime.test.connector_unavailable"))
        }
    }

    fn remote_config(transport: McpServerTransport, url: String) -> McpServerConfig {
        McpServerConfig {
            id: "remote-a".to_owned(),
            enabled: true,
            namespace: "remote_a".to_owned(),
            transport,
            command: None,
            url: Some(url),
            env_vault_refs: Vec::new(),
            trust_level: McpServerTrustLevel::External,
            approval_profile: crate::config::McpServerApprovalProfile::RequireApproval,
            egress_policy: McpServerEgressPolicy::Allowlist,
            egress_allowlist: vec!["127.0.0.1".to_owned()],
            oauth_required: false,
            oauth_grant: None,
            elicitation_enabled: false,
            sampling_policy: crate::config::McpServerSamplingPolicy::default(),
            tool_allowlist: Vec::new(),
            tool_denylist: Vec::new(),
        }
    }

    fn test_remote_port(config: McpServerConfig) -> McpRemoteSessionPort {
        McpRemoteSessionPort {
            base_url: Url::parse(config.url.as_deref().expect("test URL exists"))
                .expect("test URL parses"),
            config,
            runtime_generation: 1,
            client: Client::builder()
                .redirect(Policy::none())
                .connect_timeout(Duration::from_secs(2))
                .timeout(Duration::from_secs(2))
                .build()
                .expect("test client builds"),
            vault: crate::gateway::build_test_vault(),
            credentials: None,
            oauth: None,
            oauth_request_sequence: AtomicU64::new(0),
            sessions: AsyncMutex::new(BTreeMap::new()),
        }
    }

    #[test]
    fn stdio_environment_contains_only_vault_bindings() {
        let vault = crate::gateway::build_test_vault();
        let secret_ref = VaultRef::parse("global/mcp_stdio_token").expect("vault ref parses");
        vault
            .put_secret(&secret_ref.scope, secret_ref.key.as_str(), b"test-stdio-token")
            .expect("test token is stored");
        let mut config =
            remote_config(McpServerTransport::Stdio, "http://127.0.0.1/unused".to_owned());
        config.command = Some(vec!["node".to_owned(), "mcp-server.mjs".to_owned()]);
        config.env_vault_refs = vec![crate::config::McpServerEnvVaultRef {
            name: "MCP_STDIO_TOKEN".to_owned(),
            vault_ref: "global/mcp_stdio_token".to_owned(),
        }];

        let environment =
            resolve_stdio_environment(&config, vault.as_ref()).expect("vault binding resolves");

        assert_eq!(environment.len(), 1);
        assert_eq!(
            environment.get("MCP_STDIO_TOKEN").map(String::as_str),
            Some("test-stdio-token")
        );
        assert!(!environment.contains_key("PATH"));
    }

    fn open_request() -> McpHttpSessionOpenRequest {
        McpHttpSessionOpenRequest {
            endpoint_id: "remote-a".to_owned(),
            paired_request_endpoint_id: None,
            server_id: "remote-a".to_owned(),
            runtime_generation: 1,
            body: br#"{"jsonrpc":"2.0","id":"initialize-1","method":"initialize"}"#.to_vec(),
            max_response_bytes: 8 * 1024,
        }
    }

    fn read_http_request(stream: &mut TcpStream) {
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("test timeout config succeeds");
        let mut request = Vec::new();
        let mut buffer = [0_u8; 2048];
        loop {
            let read = stream.read(&mut buffer).expect("test request is readable");
            if read == 0 {
                break;
            }
            request.extend_from_slice(&buffer[..read]);
            let header_end =
                request.windows(4).position(|window| window == b"\r\n\r\n").map(|index| index + 4);
            let Some(header_end) = header_end else {
                continue;
            };
            let headers = String::from_utf8_lossy(&request[..header_end]).to_ascii_lowercase();
            let content_length = headers
                .lines()
                .find_map(|line| line.strip_prefix("content-length:"))
                .and_then(|value| value.trim().parse::<usize>().ok())
                .unwrap_or(0);
            if request.len() >= header_end.saturating_add(content_length) {
                break;
            }
        }
    }

    #[tokio::test]
    async fn streamable_http_open_pins_the_server_session() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("test listener binds");
        let url = format!("http://{}/mcp", listener.local_addr().expect("listener address"));
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("HTTP request arrives");
            read_http_request(&mut stream);
            let body = br#"{"jsonrpc":"2.0","id":"initialize-1","result":{}}"#;
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nMcp-Session-Id: session-a\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )
            .expect("test response headers write");
            stream.write_all(body).expect("test response body writes");
        });
        let port = test_remote_port(remote_config(McpServerTransport::Http, url));

        let response = port.open(&open_request()).await.expect("HTTP session opens");

        assert_eq!(response.status, 200);
        assert_eq!(response.session_id.as_deref(), Some("session-a"));
        assert!(port.sessions.lock().await.contains_key("session-a"));
        server.join().expect("test server exits");
    }

    #[tokio::test]
    async fn streamable_http_disconnect_before_headers_fails_closed() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("test listener binds");
        let url = format!("http://{}/mcp", listener.local_addr().expect("listener address"));
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("HTTP request arrives");
            read_http_request(&mut stream);
        });
        let port = test_remote_port(remote_config(McpServerTransport::Http, url));

        let error =
            port.open(&open_request()).await.expect_err("truncated HTTP response is denied");

        assert!(matches!(
            error.reason_code.as_str(),
            "mcp.runtime.remote.request_failed" | "mcp.runtime.remote.read_failed"
        ));
        server.join().expect("test server exits");
    }

    #[tokio::test]
    async fn legacy_sse_disconnect_during_initialize_fails_closed() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("test listener binds");
        let url = format!("http://{}/events", listener.local_addr().expect("listener address"));
        let server = thread::spawn(move || {
            let (mut event_stream, _) = listener.accept().expect("SSE GET arrives");
            read_http_request(&mut event_stream);
            event_stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\nevent: endpoint\ndata: /messages\n\n",
                )
                .expect("endpoint event writes");
            event_stream.flush().expect("endpoint event flushes");

            let (mut request_stream, _) = listener.accept().expect("SSE POST arrives");
            read_http_request(&mut request_stream);
            request_stream
                .write_all(
                    b"HTTP/1.1 202 Accepted\r\nContent-Type: application/json\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .expect("POST acknowledgement writes");
        });
        let port = test_remote_port(remote_config(McpServerTransport::Sse, url));

        let error = port.open(&open_request()).await.expect_err("closed SSE stream is rejected");

        assert_eq!(error.reason_code, "mcp.runtime.sse.stream_closed");
        server.join().expect("test server exits");
    }

    #[tokio::test]
    async fn remote_target_outside_allowlist_is_denied_before_io() {
        let url = Url::parse("http://127.0.0.1:9/mcp").expect("test URL parses");
        let error = evaluate_remote_target(url, vec!["allowed.example".to_owned()], 1024)
            .await
            .expect_err("non-allowlisted target is denied");

        assert_eq!(error.reason_code, "mcp.runtime.remote.egress_denied");
    }

    #[tokio::test]
    async fn configured_oauth_lease_requires_live_vault_material_and_expiry() {
        let vault = crate::gateway::build_test_vault();
        let token_ref = VaultRef::parse("global/mcp_remote_token").expect("vault ref parses");
        vault
            .put_secret(&token_ref.scope, token_ref.key.as_str(), b"test-oauth-token")
            .expect("test token is stored");
        let now = now_unix_ms();
        let grant = McpServerOAuthGrant {
            grant_id: "grant.remote-a".to_owned(),
            auth_profile_id: None,
            access_token_vault_ref: "global/mcp_remote_token".to_owned(),
            refresh_token_vault_ref: Some("global/mcp_remote_refresh".to_owned()),
            metadata_vault_ref: "global/mcp_remote_metadata".to_owned(),
            scopes: vec!["tools.read".to_owned()],
            expires_at_unix_ms: Some(now.saturating_add(60_000)),
            rotation_id: Some("rotation-a".to_owned()),
            issued_at_unix_ms: now.saturating_sub(1_000),
            updated_at_unix_ms: now,
            revoked_at_unix_ms: None,
        };
        let request = McpOAuthRefreshRequest {
            request_id: "request-a".to_owned(),
            server_id: "remote-a".to_owned(),
            credential_scope_id: "oauth-remote-a".to_owned(),
            expected_runtime_generation: 1,
            minimum_valid_until_unix_ms: now.saturating_add(30_000),
            requested_at_unix_ms: now,
        };
        let credentials = McpConfigCredentialPort {
            active_access_token_vault_ref: RwLock::new(grant.access_token_vault_ref.clone()),
            grant: grant.clone(),
            vault: Arc::clone(&vault),
            runtime: Weak::new(),
        };

        let lease = credentials.refresh(&request).await.expect("live grant is available");
        assert_eq!(lease.credential_handle_id, "vault-grant.remote-a");
        assert_eq!(lease.evidence_sha256.len(), 64);

        let expired = McpConfigCredentialPort {
            active_access_token_vault_ref: RwLock::new(grant.access_token_vault_ref.clone()),
            grant: McpServerOAuthGrant { expires_at_unix_ms: Some(now.saturating_sub(1)), ..grant },
            vault,
            runtime: Weak::new(),
        };
        let error = expired.refresh(&request).await.expect_err("expired grant fails closed");
        assert_eq!(error.reason_code, "mcp.runtime.oauth.refresh_required");
    }

    #[test]
    fn approved_elicitation_payload_is_conservative_and_schema_compatible() {
        assert_eq!(approved_elicitation_payload(&Value::Bool(true)), Some(Value::Bool(true)));
        assert_eq!(
            approved_elicitation_payload(&json!({
                "type": "boolean",
                "const": true,
            })),
            Some(Value::Bool(true))
        );
        assert_eq!(
            approved_elicitation_payload(&json!({
                "type": "object",
                "properties": {
                    "confirmed": {
                        "type": "boolean",
                        "const": true,
                    },
                },
                "required": ["confirmed"],
                "additionalProperties": false,
            })),
            Some(json!({"confirmed": true}))
        );
        assert_eq!(
            approved_elicitation_payload(&json!({"type": "string"})),
            None,
            "host must not invent arbitrary elicitation content"
        );
        assert_eq!(
            approved_elicitation_payload(&json!({
                "type": "object",
                "required": ["secret"],
                "properties": {"secret": {"type": "string"}},
            })),
            None,
            "approval must not synthesize caller-requested structured data"
        );
    }

    #[tokio::test]
    async fn failed_connection_releases_exact_resource_lease() {
        let temp = tempfile::tempdir().expect("temp dir");
        let registry_path = temp.path().join("resource-leases.json");
        let limit = ResourceUnitsV1 {
            processes: 2,
            memory_bytes: 512 * 1024 * 1024,
            file_descriptors: 64,
            sockets: 4,
            spool_bytes: 8 * 1024 * 1024,
            concurrency: 2,
        };
        let governor = LocalResourceGovernor::open(LocalResourceGovernorConfig {
            registry_path: registry_path.clone(),
            global_limit: limit,
            per_owner_limit: limit,
            max_records: 16,
        })
        .expect("resource governor opens");
        let connector = GovernedMcpConnector {
            inner: Arc::new(UnavailableConnector),
            governor: governor.clone(),
            owner_id: "mcp:test-server".to_owned(),
            transport: McpSessionTransportKind::Stdio,
        };
        let request = McpConnectRequest {
            server_id: "test-server".to_owned(),
            transport: McpSessionTransportKind::Stdio,
            runtime_generation: 1,
            handshake_timeout_ms: 1_000,
            initialize: McpInitializeRequest {
                client_name: "palyra".to_owned(),
                client_version: "0.1.0".to_owned(),
                supported_protocol_versions: vec![MCP_PROTOCOL_VERSION.to_owned()],
                capabilities: McpProtocolCapabilities::default(),
            },
        };

        let error = match connector.connect(&request).await {
            Ok(_) => panic!("connection should fail"),
            Err(error) => error,
        };
        assert_eq!(error.reason_code(), "mcp.runtime.test.connector_unavailable");
        let snapshot = governor.snapshot();
        assert_eq!(snapshot.active_leases, 0);
        assert_eq!(snapshot.used, ResourceUnitsV1::default());
    }
}
