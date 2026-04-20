use std::{
    collections::HashSet,
    env, fs,
    path::{Component, PathBuf},
};

use anyhow::{Context, Result};
use palyra_common::{
    config_system::{
        parse_document_with_migration, serialize_document_pretty, ConfigMigrationInfo,
    },
    daemon_config_schema::{
        FileMemoryRetrievalConfig, FileRetrievalSourceScoringProfile, RootFileConfig,
    },
    default_config_search_paths,
    feature_rollouts::{
        parse_boolish_feature_rollout, FeatureRolloutSetting, AUXILIARY_EXECUTOR_ROLLOUT_ENV,
        CONTEXT_ENGINE_ROLLOUT_ENV, DELIVERY_ARBITRATION_ROLLOUT_ENV,
        DYNAMIC_TOOL_BUILDER_ROLLOUT_ENV, EXECUTION_BACKEND_NETWORKED_WORKER_ROLLOUT_ENV,
        EXECUTION_BACKEND_REMOTE_NODE_ROLLOUT_ENV, EXECUTION_BACKEND_SSH_TUNNEL_ROLLOUT_ENV,
        EXECUTION_GATE_PIPELINE_V2_ROLLOUT_ENV, FLOW_ORCHESTRATION_ROLLOUT_ENV,
        NETWORKED_WORKERS_ROLLOUT_ENV, PRUNING_POLICY_MATRIX_ROLLOUT_ENV,
        REPLAY_CAPTURE_ROLLOUT_ENV, RETRIEVAL_DUAL_PATH_ROLLOUT_ENV, SAFETY_BOUNDARY_ROLLOUT_ENV,
        SESSION_QUEUE_POLICY_ROLLOUT_ENV,
    },
    parse_config_path,
    runtime_preview::{parse_runtime_preview_mode, RuntimePreviewMode},
    secret_refs::{SecretRef, SecretSource},
};
use palyra_vault::VaultRef;

use super::schema::*;
use crate::channel_router::{
    BroadcastStrategy, ChannelRouterConfig, ChannelRoutingRule, DirectMessagePolicy,
};
use crate::cron::CronTimezoneMode;
use crate::media::MediaRuntimeConfig;
use crate::model_provider::{
    validate_openai_base_url_network_policy, ModelProviderAuthProviderKind, ModelProviderConfig,
    ModelProviderCredentialSource, ModelProviderKind, ProviderCapabilitiesSnapshot,
    ProviderCostTier, ProviderLatencyTier, ProviderMetadataSource, ProviderModelEntryConfig,
    ProviderModelRole, ProviderRegistryEntryConfig,
};
use crate::retrieval::{
    RetrievalBackendKind, RetrievalRuntimeConfig, RetrievalSourceScoringProfile,
};
use crate::sandbox_runner::{EgressEnforcementMode, SandboxProcessRunnerTier};

pub fn load_config() -> Result<LoadedConfig> {
    let mut deployment = DeploymentConfig::default();
    let mut daemon = DaemonConfig::default();
    let mut gateway = GatewayConfig::default();
    let mut feature_rollouts = FeatureRolloutsConfig::default();
    let mut session_queue_policy = SessionQueuePolicyConfig::default();
    let mut pruning_policy_matrix = PruningPolicyMatrixConfig::default();
    let mut retrieval_dual_path = RetrievalDualPathConfig::default();
    let mut auxiliary_executor = AuxiliaryExecutorConfig::default();
    let mut flow_orchestration = FlowOrchestrationConfig::default();
    let mut delivery_arbitration = DeliveryArbitrationConfig::default();
    let mut replay_capture = ReplayCaptureConfig::default();
    let mut networked_workers = NetworkedWorkersConfig::default();
    let mut cron = CronConfig::default();
    let mut orchestrator = OrchestratorConfig::default();
    let mut memory = MemoryConfig::default();
    let mut media = MediaRuntimeConfig::default();
    let mut model_provider = ModelProviderConfig::default();
    let mut tool_call = ToolCallConfig::default();
    let mut channel_router = ChannelRouterConfig::default();
    let mut canvas_host = CanvasHostConfig::default();
    let mut admin = AdminConfig::default();
    let mut identity = IdentityConfig::default();
    let mut storage = StorageConfig::default();
    let mut source = "defaults".to_owned();
    let mut config_version = 1_u32;
    let mut migrated_from_version = None;

    if let Some(path) = find_config_path()? {
        let content = fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        let (parsed, migration) = parse_root_file_config(&content)
            .with_context(|| format!("failed to parse config {}", path.display()))?;
        config_version = migration.target_version;
        if migration.migrated {
            migrated_from_version = Some(migration.source_version);
        }
        if let Some(file_deployment) = parsed.deployment {
            if let Some(mode) = file_deployment.mode {
                deployment.mode = DeploymentMode::parse(mode.as_str(), "deployment.mode")?;
            }
            if let Some(dangerous_remote_bind_ack) = file_deployment.dangerous_remote_bind_ack {
                deployment.dangerous_remote_bind_ack = dangerous_remote_bind_ack;
            }
        }
        if let Some(file_daemon) = parsed.daemon {
            if let Some(bind_addr) = file_daemon.bind_addr {
                daemon.bind_addr = bind_addr;
            }
            if let Some(port) = file_daemon.port {
                daemon.port = port;
            }
        }
        if let Some(file_gateway) = parsed.gateway {
            if let Some(grpc_bind_addr) = file_gateway.grpc_bind_addr {
                gateway.grpc_bind_addr = grpc_bind_addr;
            }
            if let Some(grpc_port) = file_gateway.grpc_port {
                gateway.grpc_port = grpc_port;
            }
            if let Some(quic_bind_addr) = file_gateway.quic_bind_addr {
                gateway.quic_bind_addr = quic_bind_addr;
            }
            if let Some(quic_port) = file_gateway.quic_port {
                gateway.quic_port = quic_port;
            }
            if let Some(quic_enabled) = file_gateway.quic_enabled {
                gateway.quic_enabled = quic_enabled;
            }
            if let Some(bind_profile) = file_gateway.bind_profile {
                gateway.bind_profile =
                    GatewayBindProfile::parse(bind_profile.as_str(), "gateway.bind_profile")?;
            }
            if let Some(allow_insecure_remote) = file_gateway.allow_insecure_remote {
                gateway.allow_insecure_remote = allow_insecure_remote;
            }
            if let Some(identity_store_dir) = file_gateway.identity_store_dir {
                gateway.identity_store_dir =
                    Some(parse_identity_store_dir(identity_store_dir.as_str())?);
            }
            if let Some(vault_get_approval_required_refs) =
                file_gateway.vault_get_approval_required_refs
            {
                gateway.vault_get_approval_required_refs = parse_vault_ref_allowlist(
                    vault_get_approval_required_refs.join(",").as_str(),
                    "gateway.vault_get_approval_required_refs",
                )?;
            }
            if let Some(max_tape_entries_per_response) = file_gateway.max_tape_entries_per_response
            {
                gateway.max_tape_entries_per_response = parse_positive_usize(
                    max_tape_entries_per_response,
                    "gateway.max_tape_entries_per_response",
                )?;
            }
            if let Some(max_tape_bytes_per_response) = file_gateway.max_tape_bytes_per_response {
                gateway.max_tape_bytes_per_response = parse_positive_usize(
                    max_tape_bytes_per_response,
                    "gateway.max_tape_bytes_per_response",
                )?;
            }
            if let Some(file_tls) = file_gateway.tls {
                if let Some(enabled) = file_tls.enabled {
                    gateway.tls.enabled = enabled;
                }
                if let Some(cert_path) = file_tls.cert_path {
                    gateway.tls.cert_path = Some(parse_gateway_tls_path(cert_path.as_str())?);
                }
                if let Some(key_path) = file_tls.key_path {
                    gateway.tls.key_path = Some(parse_gateway_tls_path(key_path.as_str())?);
                }
                if let Some(client_ca_path) = file_tls.client_ca_path {
                    gateway.tls.client_ca_path =
                        Some(parse_gateway_tls_path(client_ca_path.as_str())?);
                }
            }
        }
        if let Some(file_cron) = parsed.cron {
            if let Some(timezone) = file_cron.timezone {
                cron.timezone = parse_cron_timezone_mode(timezone.as_str(), "cron.timezone")?;
            }
        }
        if let Some(file_feature_rollouts) = parsed.feature_rollouts {
            if let Some(enabled) = file_feature_rollouts.dynamic_tool_builder {
                feature_rollouts.dynamic_tool_builder = FeatureRolloutSetting::from_config(enabled);
            }
            if let Some(enabled) = file_feature_rollouts.context_engine {
                feature_rollouts.context_engine = FeatureRolloutSetting::from_config(enabled);
            }
            if let Some(enabled) = file_feature_rollouts.execution_backend_remote_node {
                feature_rollouts.execution_backend_remote_node =
                    FeatureRolloutSetting::from_config(enabled);
            }
            if let Some(enabled) = file_feature_rollouts.execution_backend_networked_worker {
                feature_rollouts.execution_backend_networked_worker =
                    FeatureRolloutSetting::from_config(enabled);
            }
            if let Some(enabled) = file_feature_rollouts.execution_backend_ssh_tunnel {
                feature_rollouts.execution_backend_ssh_tunnel =
                    FeatureRolloutSetting::from_config(enabled);
            }
            if let Some(enabled) = file_feature_rollouts.safety_boundary {
                feature_rollouts.safety_boundary = FeatureRolloutSetting::from_config(enabled);
            }
            if let Some(enabled) = file_feature_rollouts.execution_gate_pipeline_v2 {
                feature_rollouts.execution_gate_pipeline_v2 =
                    FeatureRolloutSetting::from_config(enabled);
            }
            if let Some(enabled) = file_feature_rollouts.session_queue_policy {
                feature_rollouts.session_queue_policy = FeatureRolloutSetting::from_config(enabled);
            }
            if let Some(enabled) = file_feature_rollouts.pruning_policy_matrix {
                feature_rollouts.pruning_policy_matrix =
                    FeatureRolloutSetting::from_config(enabled);
            }
            if let Some(enabled) = file_feature_rollouts.retrieval_dual_path {
                feature_rollouts.retrieval_dual_path = FeatureRolloutSetting::from_config(enabled);
            }
            if let Some(enabled) = file_feature_rollouts.auxiliary_executor {
                feature_rollouts.auxiliary_executor = FeatureRolloutSetting::from_config(enabled);
            }
            if let Some(enabled) = file_feature_rollouts.flow_orchestration {
                feature_rollouts.flow_orchestration = FeatureRolloutSetting::from_config(enabled);
            }
            if let Some(enabled) = file_feature_rollouts.delivery_arbitration {
                feature_rollouts.delivery_arbitration = FeatureRolloutSetting::from_config(enabled);
            }
            if let Some(enabled) = file_feature_rollouts.replay_capture {
                feature_rollouts.replay_capture = FeatureRolloutSetting::from_config(enabled);
            }
            if let Some(enabled) = file_feature_rollouts.networked_workers {
                feature_rollouts.networked_workers = FeatureRolloutSetting::from_config(enabled);
            }
        }
        if let Some(file_session_queue_policy) = parsed.session_queue_policy {
            if let Some(mode) = file_session_queue_policy.mode {
                session_queue_policy.mode =
                    parse_runtime_preview_mode(mode.as_str(), "session_queue_policy.mode")?;
            }
            if let Some(max_depth) = file_session_queue_policy.max_depth {
                session_queue_policy.max_depth =
                    parse_positive_usize(max_depth, "session_queue_policy.max_depth")?;
            }
            if let Some(merge_window_ms) = file_session_queue_policy.merge_window_ms {
                session_queue_policy.merge_window_ms =
                    parse_positive_u64(merge_window_ms, "session_queue_policy.merge_window_ms")?;
            }
        }
        if let Some(file_pruning_policy_matrix) = parsed.pruning_policy_matrix {
            if let Some(mode) = file_pruning_policy_matrix.mode {
                pruning_policy_matrix.mode =
                    parse_runtime_preview_mode(mode.as_str(), "pruning_policy_matrix.mode")?;
            }
            if let Some(manual_apply_enabled) = file_pruning_policy_matrix.manual_apply_enabled {
                pruning_policy_matrix.manual_apply_enabled = manual_apply_enabled;
            }
            if let Some(min_token_savings) = file_pruning_policy_matrix.min_token_savings {
                pruning_policy_matrix.min_token_savings = parse_positive_u64(
                    min_token_savings,
                    "pruning_policy_matrix.min_token_savings",
                )?;
            }
        }
        if let Some(file_retrieval_dual_path) = parsed.retrieval_dual_path {
            if let Some(mode) = file_retrieval_dual_path.mode {
                retrieval_dual_path.mode =
                    parse_runtime_preview_mode(mode.as_str(), "retrieval_dual_path.mode")?;
            }
            if let Some(branch_timeout_ms) = file_retrieval_dual_path.branch_timeout_ms {
                retrieval_dual_path.branch_timeout_ms =
                    parse_positive_u64(branch_timeout_ms, "retrieval_dual_path.branch_timeout_ms")?;
            }
            if let Some(prompt_budget_tokens) = file_retrieval_dual_path.prompt_budget_tokens {
                retrieval_dual_path.prompt_budget_tokens = parse_positive_u64(
                    prompt_budget_tokens,
                    "retrieval_dual_path.prompt_budget_tokens",
                )?;
            }
        }
        if let Some(file_auxiliary_executor) = parsed.auxiliary_executor {
            if let Some(mode) = file_auxiliary_executor.mode {
                auxiliary_executor.mode =
                    parse_runtime_preview_mode(mode.as_str(), "auxiliary_executor.mode")?;
            }
            if let Some(max_tasks_per_session) = file_auxiliary_executor.max_tasks_per_session {
                auxiliary_executor.max_tasks_per_session = parse_positive_usize(
                    max_tasks_per_session,
                    "auxiliary_executor.max_tasks_per_session",
                )?;
            }
            if let Some(default_budget_tokens) = file_auxiliary_executor.default_budget_tokens {
                auxiliary_executor.default_budget_tokens = parse_positive_u64(
                    default_budget_tokens,
                    "auxiliary_executor.default_budget_tokens",
                )?;
            }
        }
        if let Some(file_flow_orchestration) = parsed.flow_orchestration {
            if let Some(mode) = file_flow_orchestration.mode {
                flow_orchestration.mode =
                    parse_runtime_preview_mode(mode.as_str(), "flow_orchestration.mode")?;
            }
            if let Some(cancellation_gate_enabled) =
                file_flow_orchestration.cancellation_gate_enabled
            {
                flow_orchestration.cancellation_gate_enabled = cancellation_gate_enabled;
            }
            if let Some(max_retry_count) = file_flow_orchestration.max_retry_count {
                flow_orchestration.max_retry_count = max_retry_count;
            }
        }
        if let Some(file_delivery_arbitration) = parsed.delivery_arbitration {
            if let Some(mode) = file_delivery_arbitration.mode {
                delivery_arbitration.mode =
                    parse_runtime_preview_mode(mode.as_str(), "delivery_arbitration.mode")?;
            }
            if let Some(descendant_preference) = file_delivery_arbitration.descendant_preference {
                delivery_arbitration.descendant_preference = descendant_preference;
            }
            if let Some(suppression_limit) = file_delivery_arbitration.suppression_limit {
                delivery_arbitration.suppression_limit = suppression_limit;
            }
        }
        if let Some(file_replay_capture) = parsed.replay_capture {
            if let Some(mode) = file_replay_capture.mode {
                replay_capture.mode =
                    parse_runtime_preview_mode(mode.as_str(), "replay_capture.mode")?;
            }
            if let Some(capture_runtime_decisions) = file_replay_capture.capture_runtime_decisions {
                replay_capture.capture_runtime_decisions = capture_runtime_decisions;
            }
            if let Some(max_events_per_run) = file_replay_capture.max_events_per_run {
                replay_capture.max_events_per_run =
                    parse_positive_usize(max_events_per_run, "replay_capture.max_events_per_run")?;
            }
        }
        if let Some(file_networked_workers) = parsed.networked_workers {
            if let Some(mode) = file_networked_workers.mode {
                networked_workers.mode =
                    parse_runtime_preview_mode(mode.as_str(), "networked_workers.mode")?;
            }
            if let Some(lease_ttl_ms) = file_networked_workers.lease_ttl_ms {
                networked_workers.lease_ttl_ms =
                    parse_positive_u64(lease_ttl_ms, "networked_workers.lease_ttl_ms")?;
            }
            if let Some(require_attestation) = file_networked_workers.require_attestation {
                networked_workers.require_attestation = require_attestation;
            }
        }
        if let Some(file_orchestrator) = parsed.orchestrator {
            if let Some(runloop_v1_enabled) = file_orchestrator.runloop_v1_enabled {
                orchestrator.runloop_v1_enabled = runloop_v1_enabled;
            }
        }
        if let Some(file_memory) = parsed.memory {
            if let Some(max_item_bytes) = file_memory.max_item_bytes {
                memory.max_item_bytes =
                    parse_positive_usize(max_item_bytes, "memory.max_item_bytes")?;
            }
            if let Some(max_item_tokens) = file_memory.max_item_tokens {
                memory.max_item_tokens =
                    parse_positive_usize(max_item_tokens, "memory.max_item_tokens")?;
            }
            if let Some(default_ttl_ms) = file_memory.default_ttl_ms {
                memory.default_ttl_ms =
                    parse_default_memory_ttl_ms(default_ttl_ms, "memory.default_ttl_ms")?;
            }
            if let Some(file_auto_inject) = file_memory.auto_inject {
                if let Some(enabled) = file_auto_inject.enabled {
                    memory.auto_inject.enabled = enabled;
                }
                if let Some(max_items) = file_auto_inject.max_items {
                    memory.auto_inject.max_items =
                        parse_positive_usize(max_items, "memory.auto_inject.max_items")?;
                }
            }
            if let Some(file_retention) = file_memory.retention {
                if let Some(max_entries) = file_retention.max_entries {
                    memory.retention.max_entries =
                        Some(parse_positive_usize(max_entries, "memory.retention.max_entries")?);
                }
                if let Some(max_bytes) = file_retention.max_bytes {
                    memory.retention.max_bytes =
                        Some(parse_positive_u64(max_bytes, "memory.retention.max_bytes")?);
                }
                if let Some(ttl_days) = file_retention.ttl_days {
                    memory.retention.ttl_days =
                        Some(parse_positive_u32(ttl_days, "memory.retention.ttl_days")?);
                }
                if let Some(vacuum_schedule) = file_retention.vacuum_schedule {
                    memory.retention.vacuum_schedule = parse_memory_retention_vacuum_schedule(
                        vacuum_schedule.as_str(),
                        "memory.retention.vacuum_schedule",
                    )?;
                }
            }
            if let Some(file_retrieval) = file_memory.retrieval {
                apply_memory_retrieval_config(&mut memory.retrieval, file_retrieval)?;
            }
        }
        if let Some(file_media) = parsed.media {
            if let Some(download_enabled) = file_media.download_enabled {
                media.download_enabled = download_enabled;
            }
            if let Some(outbound_upload_enabled) = file_media.outbound_upload_enabled {
                media.outbound_upload_enabled = outbound_upload_enabled;
            }
            if let Some(allow_http_fixture_urls) = file_media.allow_http_fixture_urls {
                media.allow_http_fixture_urls = allow_http_fixture_urls;
            }
            if let Some(max_attachments_per_message) = file_media.max_attachments_per_message {
                media.max_attachments_per_message = parse_positive_usize(
                    max_attachments_per_message,
                    "media.max_attachments_per_message",
                )?;
            }
            if let Some(max_total_attachment_bytes_per_message) =
                file_media.max_total_attachment_bytes_per_message
            {
                media.max_total_attachment_bytes_per_message = parse_positive_u64(
                    max_total_attachment_bytes_per_message,
                    "media.max_total_attachment_bytes_per_message",
                )?;
            }
            if let Some(max_download_bytes) = file_media.max_download_bytes {
                media.max_download_bytes =
                    parse_positive_usize(max_download_bytes, "media.max_download_bytes")?;
            }
            if let Some(max_redirects) = file_media.max_redirects {
                media.max_redirects = parse_positive_usize(max_redirects, "media.max_redirects")?;
            }
            if let Some(allowed_source_hosts) = file_media.allowed_source_hosts {
                media.allowed_source_hosts = parse_host_allowlist(
                    allowed_source_hosts.join(",").as_str(),
                    "media.allowed_source_hosts",
                )?;
            }
            if let Some(allowed_download_content_types) = file_media.allowed_download_content_types
            {
                media.allowed_download_content_types = parse_content_type_allowlist(
                    allowed_download_content_types.join(",").as_str(),
                    "media.allowed_download_content_types",
                )?;
            }
            if let Some(vision_allowed_content_types) = file_media.vision_allowed_content_types {
                media.vision_allowed_content_types = parse_content_type_allowlist(
                    vision_allowed_content_types.join(",").as_str(),
                    "media.vision_allowed_content_types",
                )?;
            }
            if let Some(vision_max_image_count) = file_media.vision_max_image_count {
                media.vision_max_image_count =
                    parse_positive_usize(vision_max_image_count, "media.vision_max_image_count")?;
            }
            if let Some(vision_max_image_bytes) = file_media.vision_max_image_bytes {
                media.vision_max_image_bytes =
                    parse_positive_usize(vision_max_image_bytes, "media.vision_max_image_bytes")?;
            }
            if let Some(vision_max_total_bytes) = file_media.vision_max_total_bytes {
                media.vision_max_total_bytes =
                    parse_positive_usize(vision_max_total_bytes, "media.vision_max_total_bytes")?;
            }
            if let Some(vision_max_dimension_px) = file_media.vision_max_dimension_px {
                media.vision_max_dimension_px =
                    parse_positive_u32(vision_max_dimension_px, "media.vision_max_dimension_px")?;
            }
            if let Some(outbound_allowed_content_types) = file_media.outbound_allowed_content_types
            {
                media.outbound_allowed_content_types = parse_content_type_allowlist(
                    outbound_allowed_content_types.join(",").as_str(),
                    "media.outbound_allowed_content_types",
                )?;
            }
            if let Some(outbound_max_upload_bytes) = file_media.outbound_max_upload_bytes {
                media.outbound_max_upload_bytes = parse_positive_usize(
                    outbound_max_upload_bytes,
                    "media.outbound_max_upload_bytes",
                )?;
            }
            if let Some(store_max_bytes) = file_media.store_max_bytes {
                media.store_max_bytes =
                    parse_positive_u64(store_max_bytes, "media.store_max_bytes")?;
            }
            if let Some(store_max_artifacts) = file_media.store_max_artifacts {
                media.store_max_artifacts =
                    parse_positive_usize(store_max_artifacts, "media.store_max_artifacts")?;
            }
            if let Some(retention_ttl_ms) = file_media.retention_ttl_ms {
                media.retention_ttl_ms =
                    parse_positive_i64(retention_ttl_ms, "media.retention_ttl_ms")?;
            }
        }
        if let Some(file_model_provider) = parsed.model_provider {
            if let Some(kind) = file_model_provider.kind {
                model_provider.kind = ModelProviderKind::parse(kind.as_str()).context(
                    "model_provider.kind must be deterministic, openai_compatible, or anthropic",
                )?;
            }
            if let Some(openai_base_url) = file_model_provider.openai_base_url {
                model_provider.openai_base_url = parse_openai_base_url(openai_base_url.as_str())?;
            }
            if let Some(anthropic_base_url) = file_model_provider.anthropic_base_url {
                model_provider.anthropic_base_url =
                    parse_openai_base_url(anthropic_base_url.as_str())?;
            }
            if let Some(allow_private_base_url) = file_model_provider.allow_private_base_url {
                model_provider.allow_private_base_url = allow_private_base_url;
            }
            if let Some(openai_model) = file_model_provider.openai_model {
                model_provider.openai_model = parse_openai_model(openai_model.as_str())?;
            }
            if let Some(anthropic_model) = file_model_provider.anthropic_model {
                model_provider.anthropic_model = parse_openai_model(anthropic_model.as_str())?;
            }
            if let Some(openai_embeddings_model) = file_model_provider.openai_embeddings_model {
                model_provider.openai_embeddings_model =
                    parse_optional_openai_embeddings_model(openai_embeddings_model.as_str())?;
            }
            if let Some(openai_embeddings_dims) = file_model_provider.openai_embeddings_dims {
                model_provider.openai_embeddings_dims = Some(parse_openai_embeddings_dims(
                    openai_embeddings_dims,
                    "model_provider.openai_embeddings_dims",
                )?);
            }
            if let Some(openai_api_key) = file_model_provider.openai_api_key {
                model_provider.openai_api_key =
                    if openai_api_key.trim().is_empty() { None } else { Some(openai_api_key) };
            }
            if let Some(openai_api_key_secret_ref) = file_model_provider.openai_api_key_secret_ref {
                model_provider.openai_api_key_secret_ref = Some(parse_structured_secret_ref_field(
                    openai_api_key_secret_ref,
                    "model_provider.openai_api_key_secret_ref",
                )?);
            }
            if let Some(openai_api_key_vault_ref) = file_model_provider.openai_api_key_vault_ref {
                model_provider.openai_api_key_vault_ref = parse_optional_vault_ref_field(
                    openai_api_key_vault_ref.as_str(),
                    "model_provider.openai_api_key_vault_ref",
                )?;
                model_provider.openai_api_key_secret_ref = model_provider
                    .openai_api_key_vault_ref
                    .clone()
                    .map(SecretRef::from_legacy_vault_ref);
            }
            if let Some(anthropic_api_key) = file_model_provider.anthropic_api_key {
                model_provider.anthropic_api_key = if anthropic_api_key.trim().is_empty() {
                    None
                } else {
                    Some(anthropic_api_key)
                };
            }
            if let Some(anthropic_api_key_secret_ref) =
                file_model_provider.anthropic_api_key_secret_ref
            {
                model_provider.anthropic_api_key_secret_ref =
                    Some(parse_structured_secret_ref_field(
                        anthropic_api_key_secret_ref,
                        "model_provider.anthropic_api_key_secret_ref",
                    )?);
            }
            if let Some(anthropic_api_key_vault_ref) =
                file_model_provider.anthropic_api_key_vault_ref
            {
                model_provider.anthropic_api_key_vault_ref = parse_optional_vault_ref_field(
                    anthropic_api_key_vault_ref.as_str(),
                    "model_provider.anthropic_api_key_vault_ref",
                )?;
                model_provider.anthropic_api_key_secret_ref = model_provider
                    .anthropic_api_key_vault_ref
                    .clone()
                    .map(SecretRef::from_legacy_vault_ref);
            }
            if let Some(auth_profile_ref) = file_model_provider.auth_profile_ref {
                model_provider.auth_profile_id = parse_optional_auth_profile_id(
                    auth_profile_ref.as_str(),
                    "model_provider.auth_profile_ref",
                )?;
            }
            if let Some(auth_profile_id) = file_model_provider.auth_profile_id {
                model_provider.auth_profile_id = parse_optional_auth_profile_id(
                    auth_profile_id.as_str(),
                    "model_provider.auth_profile_id",
                )?;
            }
            if let Some(auth_provider_kind) = file_model_provider.auth_provider_kind {
                model_provider.auth_profile_provider_kind =
                    Some(parse_model_provider_auth_provider_kind(
                        auth_provider_kind.as_str(),
                        "model_provider.auth_provider_kind",
                    )?);
            }
            if let Some(request_timeout_ms) = file_model_provider.request_timeout_ms {
                model_provider.request_timeout_ms =
                    parse_positive_u64(request_timeout_ms, "model_provider.request_timeout_ms")?;
            }
            if let Some(max_retries) = file_model_provider.max_retries {
                model_provider.max_retries =
                    parse_retries(max_retries, "model_provider.max_retries")?;
            }
            if let Some(retry_backoff_ms) = file_model_provider.retry_backoff_ms {
                model_provider.retry_backoff_ms =
                    parse_positive_u64(retry_backoff_ms, "model_provider.retry_backoff_ms")?;
            }
            if let Some(failure_threshold) = file_model_provider.circuit_breaker_failure_threshold {
                model_provider.circuit_breaker_failure_threshold = parse_positive_u32(
                    failure_threshold,
                    "model_provider.circuit_breaker_failure_threshold",
                )?;
            }
            if let Some(cooldown_ms) = file_model_provider.circuit_breaker_cooldown_ms {
                model_provider.circuit_breaker_cooldown_ms =
                    parse_positive_u64(cooldown_ms, "model_provider.circuit_breaker_cooldown_ms")?;
            }
            let mut registry = model_provider.registry.clone();
            if let Some(entries) = file_model_provider.providers {
                registry.providers = entries
                    .into_iter()
                    .enumerate()
                    .map(|(index, entry)| {
                        parse_model_provider_registry_entry(entry, index, &model_provider)
                    })
                    .collect::<Result<Vec<_>>>()?;
            }
            if let Some(entries) = file_model_provider.models {
                registry.models = entries
                    .into_iter()
                    .enumerate()
                    .map(|(index, entry)| {
                        parse_model_provider_registry_model(entry, index, &registry.providers)
                    })
                    .collect::<Result<Vec<_>>>()?;
            }
            if let Some(model_id) = file_model_provider.default_chat_model_id {
                registry.default_chat_model_id =
                    parse_optional_text(model_id.as_str(), "model_provider.default_chat_model_id")?;
            }
            if let Some(model_id) = file_model_provider.default_embeddings_model_id {
                registry.default_embeddings_model_id = parse_optional_text(
                    model_id.as_str(),
                    "model_provider.default_embeddings_model_id",
                )?;
            }
            if let Some(model_id) = file_model_provider.default_audio_transcription_model_id {
                registry.default_audio_transcription_model_id = parse_optional_text(
                    model_id.as_str(),
                    "model_provider.default_audio_transcription_model_id",
                )?;
            }
            if let Some(value) = file_model_provider.failover_enabled {
                registry.failover_enabled = value;
            }
            if let Some(value) = file_model_provider.response_cache_enabled {
                registry.response_cache_enabled = value;
            }
            if let Some(value) = file_model_provider.response_cache_ttl_ms {
                registry.response_cache_ttl_ms =
                    parse_positive_u64(value, "model_provider.response_cache_ttl_ms")?;
            }
            if let Some(value) = file_model_provider.response_cache_max_entries {
                registry.response_cache_max_entries =
                    parse_positive_usize(value, "model_provider.response_cache_max_entries")?;
            }
            if let Some(value) = file_model_provider.discovery_ttl_ms {
                registry.discovery_ttl_ms =
                    parse_positive_u64(value, "model_provider.discovery_ttl_ms")?;
            }
            if let Some(value) = file_model_provider.health_ttl_ms {
                registry.health_ttl_ms = parse_positive_u64(value, "model_provider.health_ttl_ms")?;
            }
            model_provider.registry = registry;
        }
        if let Some(file_tool_call) = parsed.tool_call {
            if let Some(allowed_tools) = file_tool_call.allowed_tools {
                tool_call.allowed_tools = parse_tool_allowlist(
                    allowed_tools.join(",").as_str(),
                    "tool_call.allowed_tools",
                )?;
            }
            if let Some(max_calls_per_run) = file_tool_call.max_calls_per_run {
                tool_call.max_calls_per_run =
                    parse_positive_u32(max_calls_per_run, "tool_call.max_calls_per_run")?;
            }
            if let Some(execution_timeout_ms) = file_tool_call.execution_timeout_ms {
                tool_call.execution_timeout_ms =
                    parse_positive_u64(execution_timeout_ms, "tool_call.execution_timeout_ms")?;
            }
            if let Some(file_process_runner) = file_tool_call.process_runner {
                if let Some(enabled) = file_process_runner.enabled {
                    tool_call.process_runner.enabled = enabled;
                }
                if let Some(tier) = file_process_runner.tier {
                    tool_call.process_runner.tier =
                        parse_process_runner_tier(tier.as_str(), "tool_call.process_runner.tier")?;
                }
                if let Some(workspace_root) = file_process_runner.workspace_root {
                    tool_call.process_runner.workspace_root =
                        parse_workspace_root(workspace_root.as_str())?;
                }
                if let Some(allowed_executables) = file_process_runner.allowed_executables {
                    tool_call.process_runner.allowed_executables =
                        parse_process_executable_allowlist(
                            allowed_executables.join(",").as_str(),
                            "tool_call.process_runner.allowed_executables",
                        )?;
                }
                if let Some(allow_interpreters) = file_process_runner.allow_interpreters {
                    tool_call.process_runner.allow_interpreters = allow_interpreters;
                }
                if let Some(egress_enforcement_mode) = file_process_runner.egress_enforcement_mode {
                    tool_call.process_runner.egress_enforcement_mode =
                        parse_process_runner_egress_enforcement_mode(
                            egress_enforcement_mode.as_str(),
                            "tool_call.process_runner.egress_enforcement_mode",
                        )?;
                }
                if let Some(allowed_egress_hosts) = file_process_runner.allowed_egress_hosts {
                    tool_call.process_runner.allowed_egress_hosts = parse_host_allowlist(
                        allowed_egress_hosts.join(",").as_str(),
                        "tool_call.process_runner.allowed_egress_hosts",
                    )?;
                }
                if let Some(allowed_dns_suffixes) = file_process_runner.allowed_dns_suffixes {
                    tool_call.process_runner.allowed_dns_suffixes = parse_dns_suffix_allowlist(
                        allowed_dns_suffixes.join(",").as_str(),
                        "tool_call.process_runner.allowed_dns_suffixes",
                    )?;
                }
                if let Some(cpu_time_limit_ms) = file_process_runner.cpu_time_limit_ms {
                    tool_call.process_runner.cpu_time_limit_ms = parse_positive_u64(
                        cpu_time_limit_ms,
                        "tool_call.process_runner.cpu_time_limit_ms",
                    )?;
                }
                if let Some(memory_limit_bytes) = file_process_runner.memory_limit_bytes {
                    tool_call.process_runner.memory_limit_bytes = parse_positive_u64(
                        memory_limit_bytes,
                        "tool_call.process_runner.memory_limit_bytes",
                    )?;
                }
                if let Some(max_output_bytes) = file_process_runner.max_output_bytes {
                    tool_call.process_runner.max_output_bytes = parse_positive_u64(
                        max_output_bytes,
                        "tool_call.process_runner.max_output_bytes",
                    )?;
                }
            }
            if let Some(file_wasm_runtime) = file_tool_call.wasm_runtime {
                if let Some(enabled) = file_wasm_runtime.enabled {
                    tool_call.wasm_runtime.enabled = enabled;
                }
                if let Some(allow_inline_modules) = file_wasm_runtime.allow_inline_modules {
                    tool_call.wasm_runtime.allow_inline_modules = allow_inline_modules;
                }
                if let Some(max_module_size_bytes) = file_wasm_runtime.max_module_size_bytes {
                    tool_call.wasm_runtime.max_module_size_bytes = parse_positive_u64(
                        max_module_size_bytes,
                        "tool_call.wasm_runtime.max_module_size_bytes",
                    )?;
                }
                if let Some(fuel_budget) = file_wasm_runtime.fuel_budget {
                    tool_call.wasm_runtime.fuel_budget =
                        parse_positive_u64(fuel_budget, "tool_call.wasm_runtime.fuel_budget")?;
                }
                if let Some(max_memory_bytes) = file_wasm_runtime.max_memory_bytes {
                    tool_call.wasm_runtime.max_memory_bytes = parse_positive_u64(
                        max_memory_bytes,
                        "tool_call.wasm_runtime.max_memory_bytes",
                    )?;
                }
                if let Some(max_table_elements) = file_wasm_runtime.max_table_elements {
                    tool_call.wasm_runtime.max_table_elements = parse_positive_u64(
                        max_table_elements,
                        "tool_call.wasm_runtime.max_table_elements",
                    )?;
                }
                if let Some(max_instances) = file_wasm_runtime.max_instances {
                    tool_call.wasm_runtime.max_instances =
                        parse_positive_u64(max_instances, "tool_call.wasm_runtime.max_instances")?;
                }
                if let Some(allowed_http_hosts) = file_wasm_runtime.allowed_http_hosts {
                    tool_call.wasm_runtime.allowed_http_hosts = parse_host_allowlist(
                        allowed_http_hosts.join(",").as_str(),
                        "tool_call.wasm_runtime.allowed_http_hosts",
                    )?;
                }
                if let Some(allowed_secrets) = file_wasm_runtime.allowed_secrets {
                    tool_call.wasm_runtime.allowed_secrets = parse_identifier_allowlist(
                        allowed_secrets.join(",").as_str(),
                        "tool_call.wasm_runtime.allowed_secrets",
                        "secret handle",
                    )?;
                }
                if let Some(allowed_storage_prefixes) = file_wasm_runtime.allowed_storage_prefixes {
                    tool_call.wasm_runtime.allowed_storage_prefixes =
                        parse_storage_prefix_allowlist(
                            allowed_storage_prefixes.join(",").as_str(),
                            "tool_call.wasm_runtime.allowed_storage_prefixes",
                        )?;
                }
                if let Some(allowed_channels) = file_wasm_runtime.allowed_channels {
                    tool_call.wasm_runtime.allowed_channels = parse_identifier_allowlist(
                        allowed_channels.join(",").as_str(),
                        "tool_call.wasm_runtime.allowed_channels",
                        "channel handle",
                    )?;
                }
            }
            if let Some(file_http_fetch) = file_tool_call.http_fetch {
                if let Some(allow_private_targets) = file_http_fetch.allow_private_targets {
                    tool_call.http_fetch.allow_private_targets = allow_private_targets;
                }
                if let Some(connect_timeout_ms) = file_http_fetch.connect_timeout_ms {
                    tool_call.http_fetch.connect_timeout_ms = parse_positive_u64(
                        connect_timeout_ms,
                        "tool_call.http_fetch.connect_timeout_ms",
                    )?;
                }
                if let Some(request_timeout_ms) = file_http_fetch.request_timeout_ms {
                    tool_call.http_fetch.request_timeout_ms = parse_positive_u64(
                        request_timeout_ms,
                        "tool_call.http_fetch.request_timeout_ms",
                    )?;
                }
                if let Some(max_response_bytes) = file_http_fetch.max_response_bytes {
                    tool_call.http_fetch.max_response_bytes = parse_positive_u64(
                        max_response_bytes,
                        "tool_call.http_fetch.max_response_bytes",
                    )?;
                }
                if let Some(allow_redirects) = file_http_fetch.allow_redirects {
                    tool_call.http_fetch.allow_redirects = allow_redirects;
                }
                if let Some(max_redirects) = file_http_fetch.max_redirects {
                    tool_call.http_fetch.max_redirects =
                        parse_positive_u32(max_redirects, "tool_call.http_fetch.max_redirects")?;
                }
                if let Some(allowed_content_types) = file_http_fetch.allowed_content_types {
                    tool_call.http_fetch.allowed_content_types = parse_content_type_allowlist(
                        allowed_content_types.join(",").as_str(),
                        "tool_call.http_fetch.allowed_content_types",
                    )?;
                }
                if let Some(allowed_request_headers) = file_http_fetch.allowed_request_headers {
                    tool_call.http_fetch.allowed_request_headers = parse_http_header_allowlist(
                        allowed_request_headers.join(",").as_str(),
                        "tool_call.http_fetch.allowed_request_headers",
                    )?;
                }
                if let Some(cache_enabled) = file_http_fetch.cache_enabled {
                    tool_call.http_fetch.cache_enabled = cache_enabled;
                }
                if let Some(cache_ttl_ms) = file_http_fetch.cache_ttl_ms {
                    tool_call.http_fetch.cache_ttl_ms =
                        parse_positive_u64(cache_ttl_ms, "tool_call.http_fetch.cache_ttl_ms")?;
                }
                if let Some(max_cache_entries) = file_http_fetch.max_cache_entries {
                    tool_call.http_fetch.max_cache_entries = parse_positive_u64(
                        max_cache_entries,
                        "tool_call.http_fetch.max_cache_entries",
                    )?;
                }
            }
            if let Some(file_browser_service) = file_tool_call.browser_service {
                if let Some(enabled) = file_browser_service.enabled {
                    tool_call.browser_service.enabled = enabled;
                }
                if let Some(endpoint) = file_browser_service.endpoint {
                    tool_call.browser_service.endpoint = parse_browser_service_endpoint(
                        endpoint.as_str(),
                        "tool_call.browser_service.endpoint",
                    )?;
                }
                if let Some(auth_token) = file_browser_service.auth_token {
                    let trimmed = auth_token.trim();
                    tool_call.browser_service.auth_token =
                        if trimmed.is_empty() { None } else { Some(trimmed.to_owned()) };
                }
                if let Some(auth_token_secret_ref) = file_browser_service.auth_token_secret_ref {
                    tool_call.browser_service.auth_token_secret_ref =
                        Some(parse_structured_secret_ref_field(
                            auth_token_secret_ref,
                            "tool_call.browser_service.auth_token_secret_ref",
                        )?);
                }
                if let Some(state_dir) = file_browser_service.state_dir {
                    tool_call.browser_service.state_dir = parse_optional_browser_state_dir(
                        state_dir.as_str(),
                        "tool_call.browser_service.state_dir",
                    )?;
                }
                if let Some(state_key_secret_ref) = file_browser_service.state_key_secret_ref {
                    tool_call.browser_service.state_key_secret_ref =
                        Some(parse_structured_secret_ref_field(
                            state_key_secret_ref,
                            "tool_call.browser_service.state_key_secret_ref",
                        )?);
                }
                if let Some(state_key_vault_ref) = file_browser_service.state_key_vault_ref {
                    tool_call.browser_service.state_key_vault_ref = parse_optional_vault_ref_field(
                        state_key_vault_ref.as_str(),
                        "tool_call.browser_service.state_key_vault_ref",
                    )?;
                    tool_call.browser_service.state_key_secret_ref = tool_call
                        .browser_service
                        .state_key_vault_ref
                        .clone()
                        .map(SecretRef::from_legacy_vault_ref);
                }
                if let Some(connect_timeout_ms) = file_browser_service.connect_timeout_ms {
                    tool_call.browser_service.connect_timeout_ms = parse_positive_u64(
                        connect_timeout_ms,
                        "tool_call.browser_service.connect_timeout_ms",
                    )?;
                }
                if let Some(request_timeout_ms) = file_browser_service.request_timeout_ms {
                    tool_call.browser_service.request_timeout_ms = parse_positive_u64(
                        request_timeout_ms,
                        "tool_call.browser_service.request_timeout_ms",
                    )?;
                }
                if let Some(max_screenshot_bytes) = file_browser_service.max_screenshot_bytes {
                    tool_call.browser_service.max_screenshot_bytes = parse_positive_u64(
                        max_screenshot_bytes,
                        "tool_call.browser_service.max_screenshot_bytes",
                    )?;
                }
                if let Some(max_title_bytes) = file_browser_service.max_title_bytes {
                    tool_call.browser_service.max_title_bytes = parse_positive_u64(
                        max_title_bytes,
                        "tool_call.browser_service.max_title_bytes",
                    )?;
                }
            }
        }
        if let Some(file_channel_router) = parsed.channel_router {
            if let Some(enabled) = file_channel_router.enabled {
                channel_router.enabled = enabled;
            }
            if let Some(max_message_bytes) = file_channel_router.max_message_bytes {
                channel_router.max_message_bytes =
                    parse_positive_usize(max_message_bytes, "channel_router.max_message_bytes")?;
            }
            if let Some(max_retry_queue_depth_per_channel) =
                file_channel_router.max_retry_queue_depth_per_channel
            {
                channel_router.max_retry_queue_depth_per_channel = parse_positive_usize(
                    max_retry_queue_depth_per_channel,
                    "channel_router.max_retry_queue_depth_per_channel",
                )?;
            }
            if let Some(max_retry_attempts) = file_channel_router.max_retry_attempts {
                channel_router.max_retry_attempts =
                    parse_positive_u32(max_retry_attempts, "channel_router.max_retry_attempts")?;
            }
            if let Some(retry_backoff_ms) = file_channel_router.retry_backoff_ms {
                channel_router.retry_backoff_ms =
                    parse_positive_u64(retry_backoff_ms, "channel_router.retry_backoff_ms")?;
            }
            if let Some(default_response_prefix) = file_channel_router.default_response_prefix {
                channel_router.default_response_prefix = parse_optional_text_field(
                    default_response_prefix,
                    "channel_router.default_response_prefix",
                    256,
                )?;
            }
            if let Some(file_routing) = file_channel_router.routing {
                if let Some(default_channel_enabled) = file_routing.default_channel_enabled {
                    channel_router.default_channel_enabled = default_channel_enabled;
                }
                if let Some(default_allow_direct_messages) =
                    file_routing.default_allow_direct_messages
                {
                    channel_router.default_allow_direct_messages = default_allow_direct_messages;
                }
                if let Some(default_direct_message_policy) =
                    file_routing.default_direct_message_policy
                {
                    channel_router.default_direct_message_policy = parse_direct_message_policy(
                        default_direct_message_policy.as_str(),
                        "channel_router.routing.default_direct_message_policy",
                    )?;
                }
                if let Some(default_isolate_session_by_sender) =
                    file_routing.default_isolate_session_by_sender
                {
                    channel_router.default_isolate_session_by_sender =
                        default_isolate_session_by_sender;
                }
                if let Some(default_broadcast_strategy) = file_routing.default_broadcast_strategy {
                    channel_router.default_broadcast_strategy = parse_broadcast_strategy(
                        default_broadcast_strategy.as_str(),
                        "channel_router.routing.default_broadcast_strategy",
                    )?;
                }
                if let Some(default_concurrency_limit) = file_routing.default_concurrency_limit {
                    channel_router.default_concurrency_limit = parse_positive_usize(
                        default_concurrency_limit,
                        "channel_router.routing.default_concurrency_limit",
                    )?;
                }
                if let Some(channels) = file_routing.channels {
                    let mut parsed_channels = Vec::with_capacity(channels.len());
                    for (index, channel) in channels.into_iter().enumerate() {
                        let source_name = format!("channel_router.routing.channels[{index}]");
                        parsed_channels.push(parse_channel_routing_rule(
                            channel,
                            source_name.as_str(),
                            &channel_router,
                        )?);
                    }
                    channel_router.channels = parsed_channels;
                }
            }
        }
        if let Some(file_canvas_host) = parsed.canvas_host {
            if let Some(enabled) = file_canvas_host.enabled {
                canvas_host.enabled = enabled;
            }
            if let Some(public_base_url) = file_canvas_host.public_base_url {
                canvas_host.public_base_url = parse_canvas_host_public_base_url(
                    public_base_url.as_str(),
                    "canvas_host.public_base_url",
                )?;
            }
            if let Some(token_ttl_ms) = file_canvas_host.token_ttl_ms {
                canvas_host.token_ttl_ms =
                    parse_positive_u64(token_ttl_ms, "canvas_host.token_ttl_ms")?;
            }
            if let Some(max_state_bytes) = file_canvas_host.max_state_bytes {
                canvas_host.max_state_bytes =
                    parse_positive_u64(max_state_bytes, "canvas_host.max_state_bytes")?;
            }
            if let Some(max_bundle_bytes) = file_canvas_host.max_bundle_bytes {
                canvas_host.max_bundle_bytes =
                    parse_positive_u64(max_bundle_bytes, "canvas_host.max_bundle_bytes")?;
            }
            if let Some(max_assets_per_bundle) = file_canvas_host.max_assets_per_bundle {
                canvas_host.max_assets_per_bundle =
                    parse_positive_u32(max_assets_per_bundle, "canvas_host.max_assets_per_bundle")?;
            }
            if let Some(max_updates_per_minute) = file_canvas_host.max_updates_per_minute {
                canvas_host.max_updates_per_minute = parse_positive_u32(
                    max_updates_per_minute,
                    "canvas_host.max_updates_per_minute",
                )?;
            }
        }
        if let Some(file_admin) = parsed.admin {
            if let Some(require_auth) = file_admin.require_auth {
                admin.require_auth = require_auth;
            }
            if let Some(auth_token) = file_admin.auth_token {
                admin.auth_token =
                    if auth_token.trim().is_empty() { None } else { Some(auth_token) };
            }
            if let Some(auth_token_secret_ref) = file_admin.auth_token_secret_ref {
                admin.auth_token_secret_ref = Some(parse_structured_secret_ref_field(
                    auth_token_secret_ref,
                    "admin.auth_token_secret_ref",
                )?);
            }
            if let Some(connector_token) = file_admin.connector_token {
                admin.connector_token =
                    if connector_token.trim().is_empty() { None } else { Some(connector_token) };
            }
            if let Some(connector_token_secret_ref) = file_admin.connector_token_secret_ref {
                admin.connector_token_secret_ref = Some(parse_structured_secret_ref_field(
                    connector_token_secret_ref,
                    "admin.connector_token_secret_ref",
                )?);
            }
            if let Some(bound_principal) = file_admin.bound_principal {
                let trimmed = bound_principal.trim();
                admin.bound_principal =
                    if trimmed.is_empty() { None } else { Some(trimmed.to_owned()) };
            }
        }
        if let Some(file_identity) = parsed.identity {
            if let Some(allow_insecure) = file_identity.allow_insecure_node_rpc_without_mtls {
                identity.allow_insecure_node_rpc_without_mtls = allow_insecure;
            }
        }
        if let Some(file_storage) = parsed.storage {
            if let Some(path) = file_storage.journal_db_path {
                storage.journal_db_path = parse_journal_db_path(&path)?;
            }
            if let Some(hash_chain_enabled) = file_storage.journal_hash_chain_enabled {
                storage.journal_hash_chain_enabled = hash_chain_enabled;
            }
            if let Some(max_journal_payload_bytes) = file_storage.max_journal_payload_bytes {
                storage.max_journal_payload_bytes = parse_positive_usize(
                    max_journal_payload_bytes,
                    "storage.max_journal_payload_bytes",
                )?;
            }
            if let Some(max_journal_events) = file_storage.max_journal_events {
                storage.max_journal_events =
                    parse_positive_usize(max_journal_events, "storage.max_journal_events")?;
            }
            if let Some(vault_dir) = file_storage.vault_dir {
                storage.vault_dir = parse_vault_dir(&vault_dir)?;
            }
        }
        source = path.to_string_lossy().into_owned();
        if migration.migrated {
            source.push_str(" +migration(v0->v1)");
        }
    }

    if let Ok(bind_addr) = env::var("PALYRA_DAEMON_BIND_ADDR") {
        daemon.bind_addr = bind_addr;
        source.push_str(" +env(PALYRA_DAEMON_BIND_ADDR)");
    }

    if let Ok(port) = env::var("PALYRA_DAEMON_PORT") {
        daemon.port = port.parse::<u16>().context("PALYRA_DAEMON_PORT must be a valid u16")?;
        source.push_str(" +env(PALYRA_DAEMON_PORT)");
    }

    if let Ok(mode) = env::var("PALYRA_DEPLOYMENT_MODE") {
        deployment.mode = DeploymentMode::parse(mode.as_str(), "PALYRA_DEPLOYMENT_MODE")?;
        source.push_str(" +env(PALYRA_DEPLOYMENT_MODE)");
    }

    if let Ok(dangerous_remote_bind_ack) = env::var("PALYRA_DEPLOYMENT_DANGEROUS_REMOTE_BIND_ACK") {
        deployment.dangerous_remote_bind_ack = dangerous_remote_bind_ack
            .parse::<bool>()
            .context("PALYRA_DEPLOYMENT_DANGEROUS_REMOTE_BIND_ACK must be true or false")?;
        source.push_str(" +env(PALYRA_DEPLOYMENT_DANGEROUS_REMOTE_BIND_ACK)");
    }

    if let Ok(grpc_bind_addr) = env::var("PALYRA_GATEWAY_GRPC_BIND_ADDR") {
        gateway.grpc_bind_addr = grpc_bind_addr;
        source.push_str(" +env(PALYRA_GATEWAY_GRPC_BIND_ADDR)");
    }

    if let Ok(grpc_port) = env::var("PALYRA_GATEWAY_GRPC_PORT") {
        gateway.grpc_port =
            grpc_port.parse::<u16>().context("PALYRA_GATEWAY_GRPC_PORT must be a valid u16")?;
        source.push_str(" +env(PALYRA_GATEWAY_GRPC_PORT)");
    }

    if let Ok(quic_bind_addr) = env::var("PALYRA_GATEWAY_QUIC_BIND_ADDR") {
        gateway.quic_bind_addr = quic_bind_addr;
        source.push_str(" +env(PALYRA_GATEWAY_QUIC_BIND_ADDR)");
    }

    if let Ok(quic_port) = env::var("PALYRA_GATEWAY_QUIC_PORT") {
        gateway.quic_port =
            quic_port.parse::<u16>().context("PALYRA_GATEWAY_QUIC_PORT must be a valid u16")?;
        source.push_str(" +env(PALYRA_GATEWAY_QUIC_PORT)");
    }

    if let Ok(quic_enabled) = env::var("PALYRA_GATEWAY_QUIC_ENABLED") {
        gateway.quic_enabled = quic_enabled
            .parse::<bool>()
            .context("PALYRA_GATEWAY_QUIC_ENABLED must be true or false")?;
        source.push_str(" +env(PALYRA_GATEWAY_QUIC_ENABLED)");
    }

    if let Ok(bind_profile) = env::var("PALYRA_GATEWAY_BIND_PROFILE") {
        gateway.bind_profile =
            GatewayBindProfile::parse(bind_profile.as_str(), "PALYRA_GATEWAY_BIND_PROFILE")?;
        source.push_str(" +env(PALYRA_GATEWAY_BIND_PROFILE)");
    }

    if let Ok(allow_insecure_remote) = env::var("PALYRA_GATEWAY_ALLOW_INSECURE_REMOTE") {
        gateway.allow_insecure_remote = allow_insecure_remote
            .parse::<bool>()
            .context("PALYRA_GATEWAY_ALLOW_INSECURE_REMOTE must be true or false")?;
        source.push_str(" +env(PALYRA_GATEWAY_ALLOW_INSECURE_REMOTE)");
    }

    if let Ok(identity_store_dir) = env::var("PALYRA_GATEWAY_IDENTITY_STORE_DIR") {
        gateway.identity_store_dir = Some(parse_identity_store_dir(identity_store_dir.as_str())?);
        source.push_str(" +env(PALYRA_GATEWAY_IDENTITY_STORE_DIR)");
    }

    if let Ok(vault_get_approval_required_refs) =
        env::var("PALYRA_VAULT_GET_APPROVAL_REQUIRED_REFS")
    {
        gateway.vault_get_approval_required_refs = parse_vault_ref_allowlist(
            vault_get_approval_required_refs.as_str(),
            "PALYRA_VAULT_GET_APPROVAL_REQUIRED_REFS",
        )?;
        source.push_str(" +env(PALYRA_VAULT_GET_APPROVAL_REQUIRED_REFS)");
    }

    if let Ok(max_tape_entries_per_response) =
        env::var("PALYRA_GATEWAY_MAX_TAPE_ENTRIES_PER_RESPONSE")
    {
        gateway.max_tape_entries_per_response = parse_positive_usize(
            max_tape_entries_per_response
                .parse::<u64>()
                .context("PALYRA_GATEWAY_MAX_TAPE_ENTRIES_PER_RESPONSE must be a valid u64")?,
            "PALYRA_GATEWAY_MAX_TAPE_ENTRIES_PER_RESPONSE",
        )?;
        source.push_str(" +env(PALYRA_GATEWAY_MAX_TAPE_ENTRIES_PER_RESPONSE)");
    }

    if let Ok(max_tape_bytes_per_response) = env::var("PALYRA_GATEWAY_MAX_TAPE_BYTES_PER_RESPONSE")
    {
        gateway.max_tape_bytes_per_response = parse_positive_usize(
            max_tape_bytes_per_response
                .parse::<u64>()
                .context("PALYRA_GATEWAY_MAX_TAPE_BYTES_PER_RESPONSE must be a valid u64")?,
            "PALYRA_GATEWAY_MAX_TAPE_BYTES_PER_RESPONSE",
        )?;
        source.push_str(" +env(PALYRA_GATEWAY_MAX_TAPE_BYTES_PER_RESPONSE)");
    }

    if let Ok(tls_enabled) = env::var("PALYRA_GATEWAY_TLS_ENABLED") {
        gateway.tls.enabled = tls_enabled
            .parse::<bool>()
            .context("PALYRA_GATEWAY_TLS_ENABLED must be true or false")?;
        source.push_str(" +env(PALYRA_GATEWAY_TLS_ENABLED)");
    }

    if let Ok(tls_cert_path) = env::var("PALYRA_GATEWAY_TLS_CERT_PATH") {
        gateway.tls.cert_path = Some(parse_gateway_tls_path(tls_cert_path.as_str())?);
        source.push_str(" +env(PALYRA_GATEWAY_TLS_CERT_PATH)");
    }

    if let Ok(tls_key_path) = env::var("PALYRA_GATEWAY_TLS_KEY_PATH") {
        gateway.tls.key_path = Some(parse_gateway_tls_path(tls_key_path.as_str())?);
        source.push_str(" +env(PALYRA_GATEWAY_TLS_KEY_PATH)");
    }

    if let Ok(tls_client_ca_path) = env::var("PALYRA_GATEWAY_TLS_CLIENT_CA_PATH") {
        gateway.tls.client_ca_path = Some(parse_gateway_tls_path(tls_client_ca_path.as_str())?);
        source.push_str(" +env(PALYRA_GATEWAY_TLS_CLIENT_CA_PATH)");
    }

    if let Ok(cron_timezone) = env::var("PALYRA_CRON_TIMEZONE") {
        cron.timezone = parse_cron_timezone_mode(cron_timezone.as_str(), "PALYRA_CRON_TIMEZONE")?;
        source.push_str(" +env(PALYRA_CRON_TIMEZONE)");
    }

    if let Ok(runloop_v1_enabled) = env::var("PALYRA_ORCHESTRATOR_RUNLOOP_V1_ENABLED") {
        orchestrator.runloop_v1_enabled = runloop_v1_enabled
            .parse::<bool>()
            .context("PALYRA_ORCHESTRATOR_RUNLOOP_V1_ENABLED must be true or false")?;
        source.push_str(" +env(PALYRA_ORCHESTRATOR_RUNLOOP_V1_ENABLED)");
    }

    if let Ok(max_item_bytes) = env::var("PALYRA_MEMORY_MAX_ITEM_BYTES") {
        memory.max_item_bytes = parse_positive_usize(
            max_item_bytes
                .parse::<u64>()
                .context("PALYRA_MEMORY_MAX_ITEM_BYTES must be a valid u64")?,
            "PALYRA_MEMORY_MAX_ITEM_BYTES",
        )?;
        source.push_str(" +env(PALYRA_MEMORY_MAX_ITEM_BYTES)");
    }

    if let Ok(max_item_tokens) = env::var("PALYRA_MEMORY_MAX_ITEM_TOKENS") {
        memory.max_item_tokens = parse_positive_usize(
            max_item_tokens
                .parse::<u64>()
                .context("PALYRA_MEMORY_MAX_ITEM_TOKENS must be a valid u64")?,
            "PALYRA_MEMORY_MAX_ITEM_TOKENS",
        )?;
        source.push_str(" +env(PALYRA_MEMORY_MAX_ITEM_TOKENS)");
    }

    if let Ok(default_ttl_ms) = env::var("PALYRA_MEMORY_DEFAULT_TTL_MS") {
        memory.default_ttl_ms = parse_default_memory_ttl_ms(
            default_ttl_ms
                .parse::<i64>()
                .context("PALYRA_MEMORY_DEFAULT_TTL_MS must be a valid i64")?,
            "PALYRA_MEMORY_DEFAULT_TTL_MS",
        )?;
        source.push_str(" +env(PALYRA_MEMORY_DEFAULT_TTL_MS)");
    }

    if let Ok(auto_inject_enabled) = env::var("PALYRA_MEMORY_AUTO_INJECT_ENABLED") {
        memory.auto_inject.enabled = auto_inject_enabled
            .parse::<bool>()
            .context("PALYRA_MEMORY_AUTO_INJECT_ENABLED must be true or false")?;
        source.push_str(" +env(PALYRA_MEMORY_AUTO_INJECT_ENABLED)");
    }

    if let Ok(auto_inject_max_items) = env::var("PALYRA_MEMORY_AUTO_INJECT_MAX_ITEMS") {
        memory.auto_inject.max_items = parse_positive_usize(
            auto_inject_max_items
                .parse::<u64>()
                .context("PALYRA_MEMORY_AUTO_INJECT_MAX_ITEMS must be a valid u64")?,
            "PALYRA_MEMORY_AUTO_INJECT_MAX_ITEMS",
        )?;
        source.push_str(" +env(PALYRA_MEMORY_AUTO_INJECT_MAX_ITEMS)");
    }
    if let Ok(retention_max_entries) = env::var("PALYRA_MEMORY_RETENTION_MAX_ENTRIES") {
        memory.retention.max_entries = Some(parse_positive_usize(
            retention_max_entries
                .parse::<u64>()
                .context("PALYRA_MEMORY_RETENTION_MAX_ENTRIES must be a valid u64")?,
            "PALYRA_MEMORY_RETENTION_MAX_ENTRIES",
        )?);
        source.push_str(" +env(PALYRA_MEMORY_RETENTION_MAX_ENTRIES)");
    }
    if let Ok(retention_max_bytes) = env::var("PALYRA_MEMORY_RETENTION_MAX_BYTES") {
        memory.retention.max_bytes = Some(parse_positive_u64(
            retention_max_bytes
                .parse::<u64>()
                .context("PALYRA_MEMORY_RETENTION_MAX_BYTES must be a valid u64")?,
            "PALYRA_MEMORY_RETENTION_MAX_BYTES",
        )?);
        source.push_str(" +env(PALYRA_MEMORY_RETENTION_MAX_BYTES)");
    }
    if let Ok(retention_ttl_days) = env::var("PALYRA_MEMORY_RETENTION_TTL_DAYS") {
        memory.retention.ttl_days = Some(parse_positive_u32(
            retention_ttl_days
                .parse::<u32>()
                .context("PALYRA_MEMORY_RETENTION_TTL_DAYS must be a valid u32")?,
            "PALYRA_MEMORY_RETENTION_TTL_DAYS",
        )?);
        source.push_str(" +env(PALYRA_MEMORY_RETENTION_TTL_DAYS)");
    }
    if let Ok(retention_vacuum_schedule) = env::var("PALYRA_MEMORY_RETENTION_VACUUM_SCHEDULE") {
        memory.retention.vacuum_schedule = parse_memory_retention_vacuum_schedule(
            retention_vacuum_schedule.as_str(),
            "PALYRA_MEMORY_RETENTION_VACUUM_SCHEDULE",
        )?;
        source.push_str(" +env(PALYRA_MEMORY_RETENTION_VACUUM_SCHEDULE)");
    }

    if let Ok(kind) = env::var("PALYRA_MODEL_PROVIDER_KIND") {
        model_provider.kind = ModelProviderKind::parse(kind.as_str())
            .context("PALYRA_MODEL_PROVIDER_KIND must be deterministic or openai_compatible")?;
        source.push_str(" +env(PALYRA_MODEL_PROVIDER_KIND)");
    }

    if let Ok(openai_base_url) = env::var("PALYRA_MODEL_PROVIDER_OPENAI_BASE_URL") {
        model_provider.openai_base_url = parse_openai_base_url(openai_base_url.as_str())?;
        source.push_str(" +env(PALYRA_MODEL_PROVIDER_OPENAI_BASE_URL)");
    }

    if let Ok(allow_private_base_url) = env::var("PALYRA_MODEL_PROVIDER_ALLOW_PRIVATE_BASE_URL") {
        model_provider.allow_private_base_url = allow_private_base_url
            .parse::<bool>()
            .context("PALYRA_MODEL_PROVIDER_ALLOW_PRIVATE_BASE_URL must be true or false")?;
        source.push_str(" +env(PALYRA_MODEL_PROVIDER_ALLOW_PRIVATE_BASE_URL)");
    }

    if let Ok(openai_model) = env::var("PALYRA_MODEL_PROVIDER_OPENAI_MODEL") {
        model_provider.openai_model = parse_openai_model(openai_model.as_str())?;
        source.push_str(" +env(PALYRA_MODEL_PROVIDER_OPENAI_MODEL)");
    }
    if let Ok(openai_embeddings_model) = env::var("PALYRA_MODEL_PROVIDER_OPENAI_EMBEDDINGS_MODEL") {
        model_provider.openai_embeddings_model =
            parse_optional_openai_embeddings_model(openai_embeddings_model.as_str())?;
        source.push_str(" +env(PALYRA_MODEL_PROVIDER_OPENAI_EMBEDDINGS_MODEL)");
    }
    if let Ok(openai_embeddings_dims) = env::var("PALYRA_MODEL_PROVIDER_OPENAI_EMBEDDINGS_DIMS") {
        model_provider.openai_embeddings_dims = Some(parse_openai_embeddings_dims(
            openai_embeddings_dims
                .parse::<u32>()
                .context("PALYRA_MODEL_PROVIDER_OPENAI_EMBEDDINGS_DIMS must be a valid u32")?,
            "PALYRA_MODEL_PROVIDER_OPENAI_EMBEDDINGS_DIMS",
        )?);
        source.push_str(" +env(PALYRA_MODEL_PROVIDER_OPENAI_EMBEDDINGS_DIMS)");
    }

    if let Ok(openai_api_key) = env::var("PALYRA_MODEL_PROVIDER_OPENAI_API_KEY") {
        model_provider.openai_api_key =
            if openai_api_key.trim().is_empty() { None } else { Some(openai_api_key) };
        source.push_str(" +env(PALYRA_MODEL_PROVIDER_OPENAI_API_KEY)");
    }

    if let Ok(openai_api_key_vault_ref) = env::var("PALYRA_MODEL_PROVIDER_OPENAI_API_KEY_VAULT_REF")
    {
        model_provider.openai_api_key_vault_ref = parse_optional_vault_ref_field(
            openai_api_key_vault_ref.as_str(),
            "PALYRA_MODEL_PROVIDER_OPENAI_API_KEY_VAULT_REF",
        )?;
        model_provider.openai_api_key_secret_ref =
            model_provider.openai_api_key_vault_ref.clone().map(SecretRef::from_legacy_vault_ref);
        source.push_str(" +env(PALYRA_MODEL_PROVIDER_OPENAI_API_KEY_VAULT_REF)");
    }
    if let Ok(auth_profile_ref) = env::var("PALYRA_MODEL_PROVIDER_AUTH_PROFILE_REF") {
        model_provider.auth_profile_id = parse_optional_auth_profile_id(
            auth_profile_ref.as_str(),
            "PALYRA_MODEL_PROVIDER_AUTH_PROFILE_REF",
        )?;
        source.push_str(" +env(PALYRA_MODEL_PROVIDER_AUTH_PROFILE_REF)");
    }
    if let Ok(auth_profile_id) = env::var("PALYRA_MODEL_PROVIDER_AUTH_PROFILE_ID") {
        model_provider.auth_profile_id = parse_optional_auth_profile_id(
            auth_profile_id.as_str(),
            "PALYRA_MODEL_PROVIDER_AUTH_PROFILE_ID",
        )?;
        source.push_str(" +env(PALYRA_MODEL_PROVIDER_AUTH_PROFILE_ID)");
    }
    if let Ok(auth_provider_kind) = env::var("PALYRA_MODEL_PROVIDER_AUTH_PROVIDER_KIND") {
        model_provider.auth_profile_provider_kind = Some(parse_model_provider_auth_provider_kind(
            auth_provider_kind.as_str(),
            "PALYRA_MODEL_PROVIDER_AUTH_PROVIDER_KIND",
        )?);
        source.push_str(" +env(PALYRA_MODEL_PROVIDER_AUTH_PROVIDER_KIND)");
    }

    if let Ok(request_timeout_ms) = env::var("PALYRA_MODEL_PROVIDER_REQUEST_TIMEOUT_MS") {
        model_provider.request_timeout_ms = parse_positive_u64(
            request_timeout_ms
                .parse::<u64>()
                .context("PALYRA_MODEL_PROVIDER_REQUEST_TIMEOUT_MS must be a valid u64")?,
            "PALYRA_MODEL_PROVIDER_REQUEST_TIMEOUT_MS",
        )?;
        source.push_str(" +env(PALYRA_MODEL_PROVIDER_REQUEST_TIMEOUT_MS)");
    }

    if let Ok(max_retries) = env::var("PALYRA_MODEL_PROVIDER_MAX_RETRIES") {
        model_provider.max_retries = parse_retries(
            max_retries
                .parse::<u32>()
                .context("PALYRA_MODEL_PROVIDER_MAX_RETRIES must be a valid u32")?,
            "PALYRA_MODEL_PROVIDER_MAX_RETRIES",
        )?;
        source.push_str(" +env(PALYRA_MODEL_PROVIDER_MAX_RETRIES)");
    }

    if let Ok(retry_backoff_ms) = env::var("PALYRA_MODEL_PROVIDER_RETRY_BACKOFF_MS") {
        model_provider.retry_backoff_ms = parse_positive_u64(
            retry_backoff_ms
                .parse::<u64>()
                .context("PALYRA_MODEL_PROVIDER_RETRY_BACKOFF_MS must be a valid u64")?,
            "PALYRA_MODEL_PROVIDER_RETRY_BACKOFF_MS",
        )?;
        source.push_str(" +env(PALYRA_MODEL_PROVIDER_RETRY_BACKOFF_MS)");
    }

    if let Ok(failure_threshold) =
        env::var("PALYRA_MODEL_PROVIDER_CIRCUIT_BREAKER_FAILURE_THRESHOLD")
    {
        model_provider.circuit_breaker_failure_threshold = parse_positive_u32(
            failure_threshold.parse::<u32>().context(
                "PALYRA_MODEL_PROVIDER_CIRCUIT_BREAKER_FAILURE_THRESHOLD must be a valid u32",
            )?,
            "PALYRA_MODEL_PROVIDER_CIRCUIT_BREAKER_FAILURE_THRESHOLD",
        )?;
        source.push_str(" +env(PALYRA_MODEL_PROVIDER_CIRCUIT_BREAKER_FAILURE_THRESHOLD)");
    }

    if let Ok(cooldown_ms) = env::var("PALYRA_MODEL_PROVIDER_CIRCUIT_BREAKER_COOLDOWN_MS") {
        model_provider.circuit_breaker_cooldown_ms = parse_positive_u64(
            cooldown_ms
                .parse::<u64>()
                .context("PALYRA_MODEL_PROVIDER_CIRCUIT_BREAKER_COOLDOWN_MS must be a valid u64")?,
            "PALYRA_MODEL_PROVIDER_CIRCUIT_BREAKER_COOLDOWN_MS",
        )?;
        source.push_str(" +env(PALYRA_MODEL_PROVIDER_CIRCUIT_BREAKER_COOLDOWN_MS)");
    }

    if let Ok(allowed_tools) = env::var("PALYRA_TOOL_CALL_ALLOWED_TOOLS") {
        tool_call.allowed_tools =
            parse_tool_allowlist(allowed_tools.as_str(), "PALYRA_TOOL_CALL_ALLOWED_TOOLS")?;
        source.push_str(" +env(PALYRA_TOOL_CALL_ALLOWED_TOOLS)");
    }

    if let Ok(max_calls_per_run) = env::var("PALYRA_TOOL_CALL_MAX_CALLS_PER_RUN") {
        tool_call.max_calls_per_run = parse_positive_u32(
            max_calls_per_run
                .parse::<u32>()
                .context("PALYRA_TOOL_CALL_MAX_CALLS_PER_RUN must be a valid u32")?,
            "PALYRA_TOOL_CALL_MAX_CALLS_PER_RUN",
        )?;
        source.push_str(" +env(PALYRA_TOOL_CALL_MAX_CALLS_PER_RUN)");
    }

    if let Ok(execution_timeout_ms) = env::var("PALYRA_TOOL_CALL_TIMEOUT_MS") {
        tool_call.execution_timeout_ms = parse_positive_u64(
            execution_timeout_ms
                .parse::<u64>()
                .context("PALYRA_TOOL_CALL_TIMEOUT_MS must be a valid u64")?,
            "PALYRA_TOOL_CALL_TIMEOUT_MS",
        )?;
        source.push_str(" +env(PALYRA_TOOL_CALL_TIMEOUT_MS)");
    }
    if let Ok(allow_private_targets) = env::var("PALYRA_HTTP_FETCH_ALLOW_PRIVATE_TARGETS") {
        tool_call.http_fetch.allow_private_targets = allow_private_targets
            .parse::<bool>()
            .context("PALYRA_HTTP_FETCH_ALLOW_PRIVATE_TARGETS must be true or false")?;
        source.push_str(" +env(PALYRA_HTTP_FETCH_ALLOW_PRIVATE_TARGETS)");
    }
    if let Ok(connect_timeout_ms) = env::var("PALYRA_HTTP_FETCH_CONNECT_TIMEOUT_MS") {
        tool_call.http_fetch.connect_timeout_ms = parse_positive_u64(
            connect_timeout_ms
                .parse::<u64>()
                .context("PALYRA_HTTP_FETCH_CONNECT_TIMEOUT_MS must be a valid u64")?,
            "PALYRA_HTTP_FETCH_CONNECT_TIMEOUT_MS",
        )?;
        source.push_str(" +env(PALYRA_HTTP_FETCH_CONNECT_TIMEOUT_MS)");
    }
    if let Ok(request_timeout_ms) = env::var("PALYRA_HTTP_FETCH_REQUEST_TIMEOUT_MS") {
        tool_call.http_fetch.request_timeout_ms = parse_positive_u64(
            request_timeout_ms
                .parse::<u64>()
                .context("PALYRA_HTTP_FETCH_REQUEST_TIMEOUT_MS must be a valid u64")?,
            "PALYRA_HTTP_FETCH_REQUEST_TIMEOUT_MS",
        )?;
        source.push_str(" +env(PALYRA_HTTP_FETCH_REQUEST_TIMEOUT_MS)");
    }
    if let Ok(max_response_bytes) = env::var("PALYRA_HTTP_FETCH_MAX_RESPONSE_BYTES") {
        tool_call.http_fetch.max_response_bytes = parse_positive_u64(
            max_response_bytes
                .parse::<u64>()
                .context("PALYRA_HTTP_FETCH_MAX_RESPONSE_BYTES must be a valid u64")?,
            "PALYRA_HTTP_FETCH_MAX_RESPONSE_BYTES",
        )?;
        source.push_str(" +env(PALYRA_HTTP_FETCH_MAX_RESPONSE_BYTES)");
    }
    if let Ok(allow_redirects) = env::var("PALYRA_HTTP_FETCH_ALLOW_REDIRECTS") {
        tool_call.http_fetch.allow_redirects = allow_redirects
            .parse::<bool>()
            .context("PALYRA_HTTP_FETCH_ALLOW_REDIRECTS must be true or false")?;
        source.push_str(" +env(PALYRA_HTTP_FETCH_ALLOW_REDIRECTS)");
    }
    if let Ok(max_redirects) = env::var("PALYRA_HTTP_FETCH_MAX_REDIRECTS") {
        tool_call.http_fetch.max_redirects = parse_positive_u32(
            max_redirects
                .parse::<u32>()
                .context("PALYRA_HTTP_FETCH_MAX_REDIRECTS must be a valid u32")?,
            "PALYRA_HTTP_FETCH_MAX_REDIRECTS",
        )?;
        source.push_str(" +env(PALYRA_HTTP_FETCH_MAX_REDIRECTS)");
    }
    if let Ok(allowed_content_types) = env::var("PALYRA_HTTP_FETCH_ALLOWED_CONTENT_TYPES") {
        tool_call.http_fetch.allowed_content_types = parse_content_type_allowlist(
            allowed_content_types.as_str(),
            "PALYRA_HTTP_FETCH_ALLOWED_CONTENT_TYPES",
        )?;
        source.push_str(" +env(PALYRA_HTTP_FETCH_ALLOWED_CONTENT_TYPES)");
    }
    if let Ok(allowed_headers) = env::var("PALYRA_HTTP_FETCH_ALLOWED_HEADERS") {
        tool_call.http_fetch.allowed_request_headers = parse_http_header_allowlist(
            allowed_headers.as_str(),
            "PALYRA_HTTP_FETCH_ALLOWED_HEADERS",
        )?;
        source.push_str(" +env(PALYRA_HTTP_FETCH_ALLOWED_HEADERS)");
    }
    if let Ok(cache_enabled) = env::var("PALYRA_HTTP_FETCH_CACHE_ENABLED") {
        tool_call.http_fetch.cache_enabled = cache_enabled
            .parse::<bool>()
            .context("PALYRA_HTTP_FETCH_CACHE_ENABLED must be true or false")?;
        source.push_str(" +env(PALYRA_HTTP_FETCH_CACHE_ENABLED)");
    }
    if let Ok(cache_ttl_ms) = env::var("PALYRA_HTTP_FETCH_CACHE_TTL_MS") {
        tool_call.http_fetch.cache_ttl_ms = parse_positive_u64(
            cache_ttl_ms
                .parse::<u64>()
                .context("PALYRA_HTTP_FETCH_CACHE_TTL_MS must be a valid u64")?,
            "PALYRA_HTTP_FETCH_CACHE_TTL_MS",
        )?;
        source.push_str(" +env(PALYRA_HTTP_FETCH_CACHE_TTL_MS)");
    }
    if let Ok(max_cache_entries) = env::var("PALYRA_HTTP_FETCH_MAX_CACHE_ENTRIES") {
        tool_call.http_fetch.max_cache_entries = parse_positive_u64(
            max_cache_entries
                .parse::<u64>()
                .context("PALYRA_HTTP_FETCH_MAX_CACHE_ENTRIES must be a valid u64")?,
            "PALYRA_HTTP_FETCH_MAX_CACHE_ENTRIES",
        )?;
        source.push_str(" +env(PALYRA_HTTP_FETCH_MAX_CACHE_ENTRIES)");
    }
    if let Ok(browser_service_enabled) = env::var("PALYRA_BROWSER_SERVICE_ENABLED") {
        tool_call.browser_service.enabled = browser_service_enabled
            .parse::<bool>()
            .context("PALYRA_BROWSER_SERVICE_ENABLED must be true or false")?;
        source.push_str(" +env(PALYRA_BROWSER_SERVICE_ENABLED)");
    }
    if let Ok(browser_service_endpoint) = env::var("PALYRA_BROWSER_SERVICE_ENDPOINT") {
        tool_call.browser_service.endpoint = parse_browser_service_endpoint(
            browser_service_endpoint.as_str(),
            "PALYRA_BROWSER_SERVICE_ENDPOINT",
        )?;
        source.push_str(" +env(PALYRA_BROWSER_SERVICE_ENDPOINT)");
    }
    if let Ok(browser_service_auth_token) = env::var("PALYRA_BROWSER_SERVICE_AUTH_TOKEN") {
        let trimmed = browser_service_auth_token.trim();
        tool_call.browser_service.auth_token =
            if trimmed.is_empty() { None } else { Some(trimmed.to_owned()) };
        source.push_str(" +env(PALYRA_BROWSER_SERVICE_AUTH_TOKEN)");
    }
    if let Ok(browserd_state_dir) = env::var("PALYRA_BROWSERD_STATE_DIR") {
        tool_call.browser_service.state_dir = parse_optional_browser_state_dir(
            browserd_state_dir.as_str(),
            "PALYRA_BROWSERD_STATE_DIR",
        )?;
        source.push_str(" +env(PALYRA_BROWSERD_STATE_DIR)");
    }
    if let Ok(browserd_state_key_vault_ref) =
        env::var("PALYRA_BROWSERD_STATE_ENCRYPTION_KEY_VAULT_REF")
    {
        tool_call.browser_service.state_key_vault_ref = parse_optional_vault_ref_field(
            browserd_state_key_vault_ref.as_str(),
            "PALYRA_BROWSERD_STATE_ENCRYPTION_KEY_VAULT_REF",
        )?;
        tool_call.browser_service.state_key_secret_ref = tool_call
            .browser_service
            .state_key_vault_ref
            .clone()
            .map(SecretRef::from_legacy_vault_ref);
        source.push_str(" +env(PALYRA_BROWSERD_STATE_ENCRYPTION_KEY_VAULT_REF)");
    }
    if let Ok(connect_timeout_ms) = env::var("PALYRA_BROWSER_SERVICE_CONNECT_TIMEOUT_MS") {
        tool_call.browser_service.connect_timeout_ms = parse_positive_u64(
            connect_timeout_ms
                .parse::<u64>()
                .context("PALYRA_BROWSER_SERVICE_CONNECT_TIMEOUT_MS must be a valid u64")?,
            "PALYRA_BROWSER_SERVICE_CONNECT_TIMEOUT_MS",
        )?;
        source.push_str(" +env(PALYRA_BROWSER_SERVICE_CONNECT_TIMEOUT_MS)");
    }
    if let Ok(request_timeout_ms) = env::var("PALYRA_BROWSER_SERVICE_REQUEST_TIMEOUT_MS") {
        tool_call.browser_service.request_timeout_ms = parse_positive_u64(
            request_timeout_ms
                .parse::<u64>()
                .context("PALYRA_BROWSER_SERVICE_REQUEST_TIMEOUT_MS must be a valid u64")?,
            "PALYRA_BROWSER_SERVICE_REQUEST_TIMEOUT_MS",
        )?;
        source.push_str(" +env(PALYRA_BROWSER_SERVICE_REQUEST_TIMEOUT_MS)");
    }
    if let Ok(max_screenshot_bytes) = env::var("PALYRA_BROWSER_SERVICE_MAX_SCREENSHOT_BYTES") {
        tool_call.browser_service.max_screenshot_bytes = parse_positive_u64(
            max_screenshot_bytes
                .parse::<u64>()
                .context("PALYRA_BROWSER_SERVICE_MAX_SCREENSHOT_BYTES must be a valid u64")?,
            "PALYRA_BROWSER_SERVICE_MAX_SCREENSHOT_BYTES",
        )?;
        source.push_str(" +env(PALYRA_BROWSER_SERVICE_MAX_SCREENSHOT_BYTES)");
    }
    if let Ok(max_title_bytes) = env::var("PALYRA_BROWSER_SERVICE_MAX_TITLE_BYTES") {
        tool_call.browser_service.max_title_bytes = parse_positive_u64(
            max_title_bytes
                .parse::<u64>()
                .context("PALYRA_BROWSER_SERVICE_MAX_TITLE_BYTES must be a valid u64")?,
            "PALYRA_BROWSER_SERVICE_MAX_TITLE_BYTES",
        )?;
        source.push_str(" +env(PALYRA_BROWSER_SERVICE_MAX_TITLE_BYTES)");
    }

    if let Ok(channel_router_enabled) = env::var("PALYRA_CHANNEL_ROUTER_ENABLED") {
        channel_router.enabled = channel_router_enabled
            .parse::<bool>()
            .context("PALYRA_CHANNEL_ROUTER_ENABLED must be true or false")?;
        source.push_str(" +env(PALYRA_CHANNEL_ROUTER_ENABLED)");
    }

    if let Ok(max_message_bytes) = env::var("PALYRA_CHANNEL_ROUTER_MAX_MESSAGE_BYTES") {
        channel_router.max_message_bytes = parse_positive_usize(
            max_message_bytes
                .parse::<u64>()
                .context("PALYRA_CHANNEL_ROUTER_MAX_MESSAGE_BYTES must be a valid u64")?,
            "PALYRA_CHANNEL_ROUTER_MAX_MESSAGE_BYTES",
        )?;
        source.push_str(" +env(PALYRA_CHANNEL_ROUTER_MAX_MESSAGE_BYTES)");
    }

    if let Ok(max_retry_queue_depth_per_channel) =
        env::var("PALYRA_CHANNEL_ROUTER_MAX_RETRY_QUEUE_DEPTH_PER_CHANNEL")
    {
        channel_router.max_retry_queue_depth_per_channel = parse_positive_usize(
            max_retry_queue_depth_per_channel.parse::<u64>().context(
                "PALYRA_CHANNEL_ROUTER_MAX_RETRY_QUEUE_DEPTH_PER_CHANNEL must be a valid u64",
            )?,
            "PALYRA_CHANNEL_ROUTER_MAX_RETRY_QUEUE_DEPTH_PER_CHANNEL",
        )?;
        source.push_str(" +env(PALYRA_CHANNEL_ROUTER_MAX_RETRY_QUEUE_DEPTH_PER_CHANNEL)");
    }

    if let Ok(max_retry_attempts) = env::var("PALYRA_CHANNEL_ROUTER_MAX_RETRY_ATTEMPTS") {
        channel_router.max_retry_attempts = parse_positive_u32(
            max_retry_attempts
                .parse::<u32>()
                .context("PALYRA_CHANNEL_ROUTER_MAX_RETRY_ATTEMPTS must be a valid u32")?,
            "PALYRA_CHANNEL_ROUTER_MAX_RETRY_ATTEMPTS",
        )?;
        source.push_str(" +env(PALYRA_CHANNEL_ROUTER_MAX_RETRY_ATTEMPTS)");
    }

    if let Ok(retry_backoff_ms) = env::var("PALYRA_CHANNEL_ROUTER_RETRY_BACKOFF_MS") {
        channel_router.retry_backoff_ms = parse_positive_u64(
            retry_backoff_ms
                .parse::<u64>()
                .context("PALYRA_CHANNEL_ROUTER_RETRY_BACKOFF_MS must be a valid u64")?,
            "PALYRA_CHANNEL_ROUTER_RETRY_BACKOFF_MS",
        )?;
        source.push_str(" +env(PALYRA_CHANNEL_ROUTER_RETRY_BACKOFF_MS)");
    }

    if let Ok(canvas_host_enabled) = env::var("PALYRA_CANVAS_HOST_ENABLED") {
        canvas_host.enabled = canvas_host_enabled
            .parse::<bool>()
            .context("PALYRA_CANVAS_HOST_ENABLED must be true or false")?;
        source.push_str(" +env(PALYRA_CANVAS_HOST_ENABLED)");
    }

    if let Ok(public_base_url) = env::var("PALYRA_CANVAS_HOST_PUBLIC_BASE_URL") {
        canvas_host.public_base_url = parse_canvas_host_public_base_url(
            public_base_url.as_str(),
            "PALYRA_CANVAS_HOST_PUBLIC_BASE_URL",
        )?;
        source.push_str(" +env(PALYRA_CANVAS_HOST_PUBLIC_BASE_URL)");
    }

    if let Ok(token_ttl_ms) = env::var("PALYRA_CANVAS_HOST_TOKEN_TTL_MS") {
        canvas_host.token_ttl_ms = parse_positive_u64(
            token_ttl_ms
                .parse::<u64>()
                .context("PALYRA_CANVAS_HOST_TOKEN_TTL_MS must be a valid u64")?,
            "PALYRA_CANVAS_HOST_TOKEN_TTL_MS",
        )?;
        source.push_str(" +env(PALYRA_CANVAS_HOST_TOKEN_TTL_MS)");
    }

    if let Ok(max_state_bytes) = env::var("PALYRA_CANVAS_HOST_MAX_STATE_BYTES") {
        canvas_host.max_state_bytes = parse_positive_u64(
            max_state_bytes
                .parse::<u64>()
                .context("PALYRA_CANVAS_HOST_MAX_STATE_BYTES must be a valid u64")?,
            "PALYRA_CANVAS_HOST_MAX_STATE_BYTES",
        )?;
        source.push_str(" +env(PALYRA_CANVAS_HOST_MAX_STATE_BYTES)");
    }

    if let Ok(max_bundle_bytes) = env::var("PALYRA_CANVAS_HOST_MAX_BUNDLE_BYTES") {
        canvas_host.max_bundle_bytes = parse_positive_u64(
            max_bundle_bytes
                .parse::<u64>()
                .context("PALYRA_CANVAS_HOST_MAX_BUNDLE_BYTES must be a valid u64")?,
            "PALYRA_CANVAS_HOST_MAX_BUNDLE_BYTES",
        )?;
        source.push_str(" +env(PALYRA_CANVAS_HOST_MAX_BUNDLE_BYTES)");
    }

    if let Ok(max_assets_per_bundle) = env::var("PALYRA_CANVAS_HOST_MAX_ASSETS_PER_BUNDLE") {
        canvas_host.max_assets_per_bundle = parse_positive_u32(
            max_assets_per_bundle
                .parse::<u32>()
                .context("PALYRA_CANVAS_HOST_MAX_ASSETS_PER_BUNDLE must be a valid u32")?,
            "PALYRA_CANVAS_HOST_MAX_ASSETS_PER_BUNDLE",
        )?;
        source.push_str(" +env(PALYRA_CANVAS_HOST_MAX_ASSETS_PER_BUNDLE)");
    }

    if let Ok(max_updates_per_minute) = env::var("PALYRA_CANVAS_HOST_MAX_UPDATES_PER_MINUTE") {
        canvas_host.max_updates_per_minute = parse_positive_u32(
            max_updates_per_minute
                .parse::<u32>()
                .context("PALYRA_CANVAS_HOST_MAX_UPDATES_PER_MINUTE must be a valid u32")?,
            "PALYRA_CANVAS_HOST_MAX_UPDATES_PER_MINUTE",
        )?;
        source.push_str(" +env(PALYRA_CANVAS_HOST_MAX_UPDATES_PER_MINUTE)");
    }

    if let Ok(require_auth) = env::var("PALYRA_ADMIN_REQUIRE_AUTH") {
        admin.require_auth = require_auth
            .parse::<bool>()
            .context("PALYRA_ADMIN_REQUIRE_AUTH must be true or false")?;
        source.push_str(" +env(PALYRA_ADMIN_REQUIRE_AUTH)");
    }

    if let Ok(admin_token) = env::var("PALYRA_ADMIN_TOKEN") {
        admin.auth_token = if admin_token.trim().is_empty() { None } else { Some(admin_token) };
        source.push_str(" +env(PALYRA_ADMIN_TOKEN)");
    }

    if let Ok(connector_token) = env::var("PALYRA_CONNECTOR_TOKEN") {
        admin.connector_token =
            if connector_token.trim().is_empty() { None } else { Some(connector_token) };
        source.push_str(" +env(PALYRA_CONNECTOR_TOKEN)");
    }

    if let Ok(bound_principal) = env::var("PALYRA_ADMIN_BOUND_PRINCIPAL") {
        let trimmed = bound_principal.trim();
        admin.bound_principal = if trimmed.is_empty() { None } else { Some(trimmed.to_owned()) };
        source.push_str(" +env(PALYRA_ADMIN_BOUND_PRINCIPAL)");
    }

    if let Ok(allow_insecure) = env::var("PALYRA_ALLOW_INSECURE_NODE_RPC_WITHOUT_MTLS") {
        identity.allow_insecure_node_rpc_without_mtls = allow_insecure
            .parse::<bool>()
            .context("PALYRA_ALLOW_INSECURE_NODE_RPC_WITHOUT_MTLS must be true or false")?;
        source.push_str(" +env(PALYRA_ALLOW_INSECURE_NODE_RPC_WITHOUT_MTLS)");
    }

    if let Ok(path) = env::var("PALYRA_JOURNAL_DB_PATH") {
        storage.journal_db_path = parse_journal_db_path(&path)?;
        source.push_str(" +env(PALYRA_JOURNAL_DB_PATH)");
    }

    if let Ok(hash_chain_enabled) = env::var("PALYRA_JOURNAL_HASH_CHAIN_ENABLED") {
        storage.journal_hash_chain_enabled = hash_chain_enabled
            .parse::<bool>()
            .context("PALYRA_JOURNAL_HASH_CHAIN_ENABLED must be true or false")?;
        source.push_str(" +env(PALYRA_JOURNAL_HASH_CHAIN_ENABLED)");
    }

    if let Ok(max_journal_payload_bytes) = env::var("PALYRA_JOURNAL_MAX_PAYLOAD_BYTES") {
        storage.max_journal_payload_bytes = parse_positive_usize(
            max_journal_payload_bytes
                .parse::<u64>()
                .context("PALYRA_JOURNAL_MAX_PAYLOAD_BYTES must be a valid u64")?,
            "PALYRA_JOURNAL_MAX_PAYLOAD_BYTES",
        )?;
        source.push_str(" +env(PALYRA_JOURNAL_MAX_PAYLOAD_BYTES)");
    }

    if let Ok(max_journal_events) = env::var("PALYRA_JOURNAL_MAX_EVENTS") {
        storage.max_journal_events = parse_positive_usize(
            max_journal_events
                .parse::<u64>()
                .context("PALYRA_JOURNAL_MAX_EVENTS must be a valid u64")?,
            "PALYRA_JOURNAL_MAX_EVENTS",
        )?;
        source.push_str(" +env(PALYRA_JOURNAL_MAX_EVENTS)");
    }

    if let Ok(vault_dir) = env::var("PALYRA_VAULT_DIR") {
        storage.vault_dir = parse_vault_dir(&vault_dir)?;
        source.push_str(" +env(PALYRA_VAULT_DIR)");
    }

    feature_rollouts.dynamic_tool_builder = apply_feature_rollout_env_override(
        feature_rollouts.dynamic_tool_builder,
        DYNAMIC_TOOL_BUILDER_ROLLOUT_ENV,
        &mut source,
    )?;
    feature_rollouts.context_engine = apply_feature_rollout_env_override(
        feature_rollouts.context_engine,
        CONTEXT_ENGINE_ROLLOUT_ENV,
        &mut source,
    )?;
    feature_rollouts.execution_backend_remote_node = apply_feature_rollout_env_override(
        feature_rollouts.execution_backend_remote_node,
        EXECUTION_BACKEND_REMOTE_NODE_ROLLOUT_ENV,
        &mut source,
    )?;
    feature_rollouts.execution_backend_networked_worker = apply_feature_rollout_env_override(
        feature_rollouts.execution_backend_networked_worker,
        EXECUTION_BACKEND_NETWORKED_WORKER_ROLLOUT_ENV,
        &mut source,
    )?;
    feature_rollouts.execution_backend_ssh_tunnel = apply_feature_rollout_env_override(
        feature_rollouts.execution_backend_ssh_tunnel,
        EXECUTION_BACKEND_SSH_TUNNEL_ROLLOUT_ENV,
        &mut source,
    )?;
    feature_rollouts.safety_boundary = apply_feature_rollout_env_override(
        feature_rollouts.safety_boundary,
        SAFETY_BOUNDARY_ROLLOUT_ENV,
        &mut source,
    )?;
    feature_rollouts.execution_gate_pipeline_v2 = apply_feature_rollout_env_override(
        feature_rollouts.execution_gate_pipeline_v2,
        EXECUTION_GATE_PIPELINE_V2_ROLLOUT_ENV,
        &mut source,
    )?;
    feature_rollouts.session_queue_policy = apply_feature_rollout_env_override(
        feature_rollouts.session_queue_policy,
        SESSION_QUEUE_POLICY_ROLLOUT_ENV,
        &mut source,
    )?;
    feature_rollouts.pruning_policy_matrix = apply_feature_rollout_env_override(
        feature_rollouts.pruning_policy_matrix,
        PRUNING_POLICY_MATRIX_ROLLOUT_ENV,
        &mut source,
    )?;
    feature_rollouts.retrieval_dual_path = apply_feature_rollout_env_override(
        feature_rollouts.retrieval_dual_path,
        RETRIEVAL_DUAL_PATH_ROLLOUT_ENV,
        &mut source,
    )?;
    feature_rollouts.auxiliary_executor = apply_feature_rollout_env_override(
        feature_rollouts.auxiliary_executor,
        AUXILIARY_EXECUTOR_ROLLOUT_ENV,
        &mut source,
    )?;
    feature_rollouts.flow_orchestration = apply_feature_rollout_env_override(
        feature_rollouts.flow_orchestration,
        FLOW_ORCHESTRATION_ROLLOUT_ENV,
        &mut source,
    )?;
    feature_rollouts.delivery_arbitration = apply_feature_rollout_env_override(
        feature_rollouts.delivery_arbitration,
        DELIVERY_ARBITRATION_ROLLOUT_ENV,
        &mut source,
    )?;
    feature_rollouts.replay_capture = apply_feature_rollout_env_override(
        feature_rollouts.replay_capture,
        REPLAY_CAPTURE_ROLLOUT_ENV,
        &mut source,
    )?;
    feature_rollouts.networked_workers = apply_feature_rollout_env_override(
        feature_rollouts.networked_workers,
        NETWORKED_WORKERS_ROLLOUT_ENV,
        &mut source,
    )?;

    if gateway.tls.enabled && (gateway.tls.cert_path.is_none() || gateway.tls.key_path.is_none()) {
        anyhow::bail!(
            "gateway.tls.enabled=true requires both gateway.tls.cert_path and gateway.tls.key_path"
        );
    }
    if model_provider.auth_profile_id.is_some()
        && model_provider.auth_profile_provider_kind.is_none()
    {
        model_provider.auth_profile_provider_kind = Some(ModelProviderAuthProviderKind::Openai);
    }
    if model_provider.kind == ModelProviderKind::OpenAiCompatible {
        validate_openai_base_url_network_policy(
            model_provider.openai_base_url.as_str(),
            model_provider.allow_private_base_url,
        )?;
    }
    memory.retrieval.validate()?;
    validate_runtime_preview_config(
        &feature_rollouts,
        &session_queue_policy,
        &pruning_policy_matrix,
        &retrieval_dual_path,
        &auxiliary_executor,
        &flow_orchestration,
        &delivery_arbitration,
        &replay_capture,
        &networked_workers,
    )?;
    validate_secret_source_conflicts(&model_provider, &tool_call.browser_service, &admin)?;

    Ok(LoadedConfig {
        source,
        config_version,
        migrated_from_version,
        deployment,
        daemon,
        gateway,
        feature_rollouts,
        session_queue_policy,
        pruning_policy_matrix,
        retrieval_dual_path,
        auxiliary_executor,
        flow_orchestration,
        delivery_arbitration,
        replay_capture,
        networked_workers,
        cron,
        orchestrator,
        memory,
        media,
        model_provider,
        tool_call,
        channel_router,
        canvas_host,
        admin,
        identity,
        storage,
    })
}

fn parse_root_file_config(content: &str) -> Result<(RootFileConfig, ConfigMigrationInfo)> {
    let (document, migration) =
        parse_document_with_migration(content).context("failed to migrate config document")?;
    let normalized =
        serialize_document_pretty(&document).context("failed to serialize normalized config")?;
    let parsed: RootFileConfig =
        toml::from_str(&normalized).context("invalid daemon config schema")?;
    Ok((parsed, migration))
}

fn apply_feature_rollout_env_override(
    current: FeatureRolloutSetting,
    env_name: &'static str,
    source: &mut String,
) -> Result<FeatureRolloutSetting> {
    let Ok(raw) = env::var(env_name) else {
        return Ok(current);
    };
    let enabled = parse_boolish_feature_rollout(raw.as_str(), env_name)?;
    source.push_str(&format!(" +env({env_name})"));
    Ok(FeatureRolloutSetting::from_env(enabled))
}

#[allow(clippy::too_many_arguments)]
fn validate_runtime_preview_config(
    feature_rollouts: &FeatureRolloutsConfig,
    session_queue_policy: &SessionQueuePolicyConfig,
    pruning_policy_matrix: &PruningPolicyMatrixConfig,
    retrieval_dual_path: &RetrievalDualPathConfig,
    auxiliary_executor: &AuxiliaryExecutorConfig,
    flow_orchestration: &FlowOrchestrationConfig,
    delivery_arbitration: &DeliveryArbitrationConfig,
    replay_capture: &ReplayCaptureConfig,
    networked_workers: &NetworkedWorkersConfig,
) -> Result<()> {
    validate_enabled_mode_requires_rollout(
        session_queue_policy.mode,
        feature_rollouts.session_queue_policy,
        "session_queue_policy.mode",
        "feature_rollouts.session_queue_policy",
        SESSION_QUEUE_POLICY_ROLLOUT_ENV,
    )?;
    validate_enabled_mode_requires_rollout(
        pruning_policy_matrix.mode,
        feature_rollouts.pruning_policy_matrix,
        "pruning_policy_matrix.mode",
        "feature_rollouts.pruning_policy_matrix",
        PRUNING_POLICY_MATRIX_ROLLOUT_ENV,
    )?;
    validate_enabled_mode_requires_rollout(
        retrieval_dual_path.mode,
        feature_rollouts.retrieval_dual_path,
        "retrieval_dual_path.mode",
        "feature_rollouts.retrieval_dual_path",
        RETRIEVAL_DUAL_PATH_ROLLOUT_ENV,
    )?;
    validate_enabled_mode_requires_rollout(
        auxiliary_executor.mode,
        feature_rollouts.auxiliary_executor,
        "auxiliary_executor.mode",
        "feature_rollouts.auxiliary_executor",
        AUXILIARY_EXECUTOR_ROLLOUT_ENV,
    )?;
    validate_enabled_mode_requires_rollout(
        flow_orchestration.mode,
        feature_rollouts.flow_orchestration,
        "flow_orchestration.mode",
        "feature_rollouts.flow_orchestration",
        FLOW_ORCHESTRATION_ROLLOUT_ENV,
    )?;
    validate_enabled_mode_requires_rollout(
        delivery_arbitration.mode,
        feature_rollouts.delivery_arbitration,
        "delivery_arbitration.mode",
        "feature_rollouts.delivery_arbitration",
        DELIVERY_ARBITRATION_ROLLOUT_ENV,
    )?;
    validate_enabled_mode_requires_rollout(
        replay_capture.mode,
        feature_rollouts.replay_capture,
        "replay_capture.mode",
        "feature_rollouts.replay_capture",
        REPLAY_CAPTURE_ROLLOUT_ENV,
    )?;
    validate_enabled_mode_requires_rollout(
        networked_workers.mode,
        feature_rollouts.networked_workers,
        "networked_workers.mode",
        "feature_rollouts.networked_workers",
        NETWORKED_WORKERS_ROLLOUT_ENV,
    )?;

    if flow_orchestration.max_retry_count > 8 {
        anyhow::bail!("flow_orchestration.max_retry_count must be in range 0..=8");
    }
    if delivery_arbitration.suppression_limit > 16 {
        anyhow::bail!("delivery_arbitration.suppression_limit must be in range 0..=16");
    }
    if !(512..=4096).contains(&retrieval_dual_path.prompt_budget_tokens) {
        anyhow::bail!("retrieval_dual_path.prompt_budget_tokens must be in range 512..=4096");
    }
    if !(250..=10_000).contains(&retrieval_dual_path.branch_timeout_ms) {
        anyhow::bail!("retrieval_dual_path.branch_timeout_ms must be in range 250..=10000");
    }
    if auxiliary_executor.max_tasks_per_session > 32 {
        anyhow::bail!("auxiliary_executor.max_tasks_per_session must be in range 1..=32");
    }
    if auxiliary_executor.default_budget_tokens > 16_384 {
        anyhow::bail!("auxiliary_executor.default_budget_tokens must be in range 1..=16384");
    }
    if session_queue_policy.max_depth > 128 {
        anyhow::bail!("session_queue_policy.max_depth must be in range 1..=128");
    }
    if session_queue_policy.merge_window_ms > 60_000 {
        anyhow::bail!("session_queue_policy.merge_window_ms must be in range 1..=60000");
    }
    if pruning_policy_matrix.min_token_savings > 1_000_000 {
        anyhow::bail!("pruning_policy_matrix.min_token_savings must be in range 1..=1000000");
    }
    if replay_capture.max_events_per_run > 4_096 {
        anyhow::bail!("replay_capture.max_events_per_run must be in range 1..=4096");
    }
    if !(60_000..=3_600_000).contains(&networked_workers.lease_ttl_ms) {
        anyhow::bail!("networked_workers.lease_ttl_ms must be in range 60000..=3600000");
    }

    Ok(())
}

fn validate_enabled_mode_requires_rollout(
    mode: RuntimePreviewMode,
    rollout: FeatureRolloutSetting,
    mode_path: &str,
    rollout_path: &str,
    rollout_env_var: &str,
) -> Result<()> {
    if matches!(mode, RuntimePreviewMode::Enabled) && !rollout.enabled {
        anyhow::bail!("{mode_path}=enabled requires {rollout_path}=true or {rollout_env_var}=1");
    }
    Ok(())
}

fn find_config_path() -> Result<Option<PathBuf>> {
    if let Ok(path) = env::var("PALYRA_CONFIG") {
        let path =
            parse_config_path(&path).context("PALYRA_CONFIG contains an invalid config path")?;
        if !path.exists() {
            anyhow::bail!("PALYRA_CONFIG points to a missing file: {}", path.to_string_lossy());
        }
        return Ok(Some(path));
    }

    for candidate in default_config_search_paths() {
        if candidate.exists() {
            return Ok(Some(candidate));
        }
    }

    Ok(None)
}

fn parse_journal_db_path(raw: &str) -> Result<PathBuf> {
    if raw.trim().is_empty() {
        anyhow::bail!("journal db path cannot be empty");
    }
    if raw.contains('\0') {
        anyhow::bail!("journal db path cannot contain embedded NUL byte");
    }
    let path = PathBuf::from(raw);
    if path.components().any(|component| matches!(component, Component::ParentDir)) {
        anyhow::bail!("journal db path cannot contain parent traversal ('..')");
    }
    Ok(path)
}

fn parse_vault_dir(raw: &str) -> Result<PathBuf> {
    if raw.trim().is_empty() {
        anyhow::bail!("vault directory cannot be empty");
    }
    if raw.contains('\0') {
        anyhow::bail!("vault directory cannot contain embedded NUL byte");
    }
    Ok(PathBuf::from(raw))
}

fn parse_optional_browser_state_dir(raw: &str, source_name: &str) -> Result<Option<PathBuf>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if trimmed.contains('\0') {
        anyhow::bail!("{source_name} cannot contain embedded NUL byte");
    }
    Ok(Some(PathBuf::from(trimmed)))
}

fn parse_optional_vault_ref_field(raw: &str, source_name: &str) -> Result<Option<String>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let refs = parse_vault_ref_allowlist(trimmed, source_name)?;
    if refs.len() != 1 {
        anyhow::bail!("{source_name} must contain exactly one <scope>/<key> entry");
    }
    Ok(refs.into_iter().next())
}

fn parse_structured_secret_ref_field(
    secret_ref: SecretRef,
    source_name: &str,
) -> Result<SecretRef> {
    let mut parsed = secret_ref;
    if let SecretSource::Vault { vault_ref } = &mut parsed.source {
        *vault_ref =
            parse_optional_vault_ref_field(vault_ref.as_str(), source_name)?.ok_or_else(|| {
                anyhow::anyhow!("{source_name} must contain exactly one <scope>/<key> entry")
            })?;
    }
    parsed.validate().map_err(|error| anyhow::anyhow!("{source_name}: {error}"))?;
    Ok(parsed)
}

fn parse_optional_auth_profile_id(raw: &str, source_name: &str) -> Result<Option<String>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if trimmed.len() > 128 {
        anyhow::bail!("{source_name} exceeds maximum bytes ({} > 128)", trimmed.len());
    }
    let normalized = trimmed.to_ascii_lowercase();
    if !normalized
        .chars()
        .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '.' | '_' | '-'))
    {
        anyhow::bail!("{source_name} contains unsupported characters");
    }
    Ok(Some(normalized))
}

fn validate_secret_source_conflicts(
    model_provider: &ModelProviderConfig,
    browser_service: &BrowserServiceConfig,
    admin: &AdminConfig,
) -> Result<()> {
    validate_secret_field_conflict(
        "model_provider.openai_api_key",
        model_provider.openai_api_key.is_some(),
        model_provider.openai_api_key_secret_ref.is_some(),
        model_provider.openai_api_key_vault_ref.is_some(),
    )?;
    validate_secret_field_conflict(
        "model_provider.anthropic_api_key",
        model_provider.anthropic_api_key.is_some(),
        model_provider.anthropic_api_key_secret_ref.is_some(),
        model_provider.anthropic_api_key_vault_ref.is_some(),
    )?;
    validate_secret_field_conflict(
        "tool_call.browser_service.auth_token",
        browser_service.auth_token.is_some(),
        browser_service.auth_token_secret_ref.is_some(),
        false,
    )?;
    validate_secret_field_conflict(
        "tool_call.browser_service.state_key",
        false,
        browser_service.state_key_secret_ref.is_some(),
        browser_service.state_key_vault_ref.is_some(),
    )?;
    validate_secret_field_conflict(
        "admin.auth_token",
        admin.auth_token.is_some(),
        admin.auth_token_secret_ref.is_some(),
        false,
    )?;
    validate_secret_field_conflict(
        "admin.connector_token",
        admin.connector_token.is_some(),
        admin.connector_token_secret_ref.is_some(),
        false,
    )?;
    Ok(())
}

fn validate_secret_field_conflict(
    field_name: &str,
    inline_present: bool,
    secret_ref_present: bool,
    legacy_vault_ref_present: bool,
) -> Result<()> {
    if inline_present && secret_ref_present {
        anyhow::bail!("{field_name} cannot set both inline value and *_secret_ref");
    }
    if inline_present && legacy_vault_ref_present {
        anyhow::bail!("{field_name} cannot set both inline value and legacy vault_ref");
    }
    if secret_ref_present && legacy_vault_ref_present {
        anyhow::bail!("{field_name} cannot set both *_secret_ref and legacy vault_ref");
    }
    Ok(())
}

fn parse_optional_text(raw: &str, source_name: &str) -> Result<Option<String>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if trimmed.contains('\0') {
        anyhow::bail!("{source_name} cannot contain embedded NUL byte");
    }
    if trimmed.len() > 256 {
        anyhow::bail!("{source_name} exceeds maximum bytes ({} > 256)", trimmed.len());
    }
    Ok(Some(trimmed.to_owned()))
}

fn parse_registry_identifier(raw: &str, source_name: &str) -> Result<String> {
    let normalized = raw.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        anyhow::bail!("{source_name} cannot be empty");
    }
    if normalized.len() > 128 {
        anyhow::bail!("{source_name} exceeds maximum bytes ({} > 128)", normalized.len());
    }
    if !normalized
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | ':'))
    {
        anyhow::bail!("{source_name} contains invalid identifier '{raw}'");
    }
    Ok(normalized)
}

fn parse_optional_display_name(raw: &str, source_name: &str) -> Result<Option<String>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if trimmed.len() > 128 {
        anyhow::bail!("{source_name} exceeds maximum bytes ({} > 128)", trimmed.len());
    }
    Ok(Some(trimmed.to_owned()))
}

fn parse_registry_string_list(raw: &[String], source_name: &str) -> Result<Vec<String>> {
    if raw.len() > 32 {
        anyhow::bail!("{source_name} exceeds maximum entries ({} > 32)", raw.len());
    }
    let mut values = Vec::new();
    let mut seen = HashSet::new();
    for candidate in raw.iter().map(String::as_str).map(str::trim).filter(|value| !value.is_empty())
    {
        if candidate.len() > 128 {
            anyhow::bail!("{source_name} contains oversized entry ({} > 128)", candidate.len());
        }
        let normalized = candidate.to_owned();
        if seen.insert(normalized.clone()) {
            values.push(normalized);
        }
    }
    Ok(values)
}

fn parse_provider_model_role(raw: &str, source_name: &str) -> Result<ProviderModelRole> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "chat" => Ok(ProviderModelRole::Chat),
        "embeddings" => Ok(ProviderModelRole::Embeddings),
        "audio_transcription" | "audio-transcription" | "audio" | "transcription" => {
            Ok(ProviderModelRole::AudioTranscription)
        }
        _ => anyhow::bail!("{source_name} must be one of: chat, embeddings, audio_transcription"),
    }
}

fn parse_provider_metadata_source(raw: &str, source_name: &str) -> Result<ProviderMetadataSource> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "legacy_migration" | "legacy-migration" => Ok(ProviderMetadataSource::LegacyMigration),
        "static" => Ok(ProviderMetadataSource::Static),
        "discovery" => Ok(ProviderMetadataSource::Discovery),
        "operator_override" | "operator-override" => Ok(ProviderMetadataSource::OperatorOverride),
        _ => anyhow::bail!(
            "{source_name} must be one of: legacy_migration, static, discovery, operator_override"
        ),
    }
}

fn parse_provider_cost_tier(raw: &str, source_name: &str) -> Result<ProviderCostTier> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "low" => Ok(ProviderCostTier::Low),
        "standard" => Ok(ProviderCostTier::Standard),
        "premium" => Ok(ProviderCostTier::Premium),
        _ => anyhow::bail!("{source_name} must be one of: low, standard, premium"),
    }
}

fn parse_provider_latency_tier(raw: &str, source_name: &str) -> Result<ProviderLatencyTier> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "low" => Ok(ProviderLatencyTier::Low),
        "standard" => Ok(ProviderLatencyTier::Standard),
        "high" => Ok(ProviderLatencyTier::High),
        _ => anyhow::bail!("{source_name} must be one of: low, standard, high"),
    }
}

fn default_provider_auth_kind(kind: ModelProviderKind) -> Option<ModelProviderAuthProviderKind> {
    match kind {
        ModelProviderKind::Deterministic => None,
        ModelProviderKind::OpenAiCompatible => Some(ModelProviderAuthProviderKind::Openai),
        ModelProviderKind::Anthropic => Some(ModelProviderAuthProviderKind::Anthropic),
    }
}

fn provider_capability_defaults(
    kind: ModelProviderKind,
    role: ProviderModelRole,
) -> ProviderCapabilitiesSnapshot {
    match (kind, role) {
        (ModelProviderKind::Deterministic, ProviderModelRole::Chat) => {
            ProviderCapabilitiesSnapshot {
                streaming_tokens: true,
                tool_calls: false,
                json_mode: true,
                vision: false,
                audio_transcribe: false,
                embeddings: false,
                max_context_tokens: Some(8_192),
                cost_tier: ProviderCostTier::Low.as_str().to_owned(),
                latency_tier: ProviderLatencyTier::Low.as_str().to_owned(),
                recommended_use_cases: vec![
                    "offline testing".to_owned(),
                    "deterministic smoke flows".to_owned(),
                ],
                known_limitations: vec!["no real provider auth".to_owned(), "no vision".to_owned()],
                operator_override: false,
                metadata_source: ProviderMetadataSource::Static.as_str().to_owned(),
            }
        }
        (ModelProviderKind::Deterministic, ProviderModelRole::Embeddings)
        | (ModelProviderKind::Deterministic, ProviderModelRole::AudioTranscription) => {
            ProviderCapabilitiesSnapshot {
                streaming_tokens: false,
                tool_calls: false,
                json_mode: false,
                vision: false,
                audio_transcribe: false,
                embeddings: false,
                max_context_tokens: None,
                cost_tier: ProviderCostTier::Low.as_str().to_owned(),
                latency_tier: ProviderLatencyTier::Low.as_str().to_owned(),
                recommended_use_cases: vec!["offline testing".to_owned()],
                known_limitations: vec!["role unsupported by deterministic provider".to_owned()],
                operator_override: false,
                metadata_source: ProviderMetadataSource::Static.as_str().to_owned(),
            }
        }
        (ModelProviderKind::OpenAiCompatible, ProviderModelRole::Chat) => {
            ProviderCapabilitiesSnapshot {
                streaming_tokens: true,
                tool_calls: true,
                json_mode: true,
                vision: true,
                audio_transcribe: true,
                embeddings: false,
                max_context_tokens: Some(128_000),
                cost_tier: ProviderCostTier::Standard.as_str().to_owned(),
                latency_tier: ProviderLatencyTier::Standard.as_str().to_owned(),
                recommended_use_cases: vec![
                    "general chat".to_owned(),
                    "JSON workflows".to_owned(),
                    "tool-calling agents".to_owned(),
                ],
                known_limitations: vec!["provider-specific compatibility varies".to_owned()],
                operator_override: false,
                metadata_source: ProviderMetadataSource::Static.as_str().to_owned(),
            }
        }
        (ModelProviderKind::OpenAiCompatible, ProviderModelRole::Embeddings) => {
            ProviderCapabilitiesSnapshot {
                streaming_tokens: false,
                tool_calls: false,
                json_mode: false,
                vision: false,
                audio_transcribe: false,
                embeddings: true,
                max_context_tokens: None,
                cost_tier: ProviderCostTier::Low.as_str().to_owned(),
                latency_tier: ProviderLatencyTier::Low.as_str().to_owned(),
                recommended_use_cases: vec![
                    "semantic search".to_owned(),
                    "workspace recall".to_owned(),
                ],
                known_limitations: vec!["no chat completions".to_owned()],
                operator_override: false,
                metadata_source: ProviderMetadataSource::Static.as_str().to_owned(),
            }
        }
        (ModelProviderKind::OpenAiCompatible, ProviderModelRole::AudioTranscription) => {
            ProviderCapabilitiesSnapshot {
                streaming_tokens: false,
                tool_calls: false,
                json_mode: false,
                vision: false,
                audio_transcribe: true,
                embeddings: false,
                max_context_tokens: None,
                cost_tier: ProviderCostTier::Standard.as_str().to_owned(),
                latency_tier: ProviderLatencyTier::Standard.as_str().to_owned(),
                recommended_use_cases: vec!["meeting notes".to_owned(), "voice uploads".to_owned()],
                known_limitations: vec!["no chat completions".to_owned()],
                operator_override: false,
                metadata_source: ProviderMetadataSource::Static.as_str().to_owned(),
            }
        }
        (ModelProviderKind::Anthropic, ProviderModelRole::Chat) => ProviderCapabilitiesSnapshot {
            streaming_tokens: true,
            tool_calls: true,
            json_mode: true,
            vision: true,
            audio_transcribe: false,
            embeddings: false,
            max_context_tokens: Some(200_000),
            cost_tier: ProviderCostTier::Premium.as_str().to_owned(),
            latency_tier: ProviderLatencyTier::Standard.as_str().to_owned(),
            recommended_use_cases: vec![
                "high-context reasoning".to_owned(),
                "vision-assisted analysis".to_owned(),
            ],
            known_limitations: vec![
                "no native embeddings".to_owned(),
                "no audio transcription".to_owned(),
            ],
            operator_override: false,
            metadata_source: ProviderMetadataSource::Static.as_str().to_owned(),
        },
        (ModelProviderKind::Anthropic, ProviderModelRole::Embeddings)
        | (ModelProviderKind::Anthropic, ProviderModelRole::AudioTranscription) => {
            ProviderCapabilitiesSnapshot {
                streaming_tokens: false,
                tool_calls: false,
                json_mode: false,
                vision: false,
                audio_transcribe: false,
                embeddings: false,
                max_context_tokens: None,
                cost_tier: ProviderCostTier::Premium.as_str().to_owned(),
                latency_tier: ProviderLatencyTier::Standard.as_str().to_owned(),
                recommended_use_cases: vec!["chat workloads".to_owned()],
                known_limitations: vec!["role unsupported by anthropic provider".to_owned()],
                operator_override: false,
                metadata_source: ProviderMetadataSource::Static.as_str().to_owned(),
            }
        }
    }
}

fn parse_model_provider_registry_entry(
    raw: palyra_common::daemon_config_schema::FileModelProviderRegistryEntry,
    index: usize,
    defaults: &ModelProviderConfig,
) -> Result<ProviderRegistryEntryConfig> {
    let source_name = format!("model_provider.providers[{index}]");
    let provider_id = parse_registry_identifier(
        raw.provider_id.unwrap_or_default().as_str(),
        format!("{source_name}.provider_id").as_str(),
    )?;
    let kind =
        ModelProviderKind::parse(raw.kind.unwrap_or_default().as_str()).with_context(|| {
            format!(
                "{source_name}.kind must be one of: deterministic, openai_compatible, anthropic"
            )
        })?;
    let display_name = parse_optional_display_name(
        raw.display_name.unwrap_or_default().as_str(),
        format!("{source_name}.display_name").as_str(),
    )?;
    let base_url = match raw.base_url {
        Some(value) => Some(parse_openai_base_url(value.as_str()).with_context(|| {
            format!("{source_name}.base_url must be a valid provider base URL")
        })?),
        None => None,
    };
    let auth_profile_id = parse_optional_auth_profile_id(
        raw.auth_profile_id.unwrap_or_default().as_str(),
        format!("{source_name}.auth_profile_id").as_str(),
    )?;
    let auth_profile_provider_kind = if let Some(value) = raw.auth_provider_kind {
        Some(parse_model_provider_auth_provider_kind(
            value.as_str(),
            format!("{source_name}.auth_provider_kind").as_str(),
        )?)
    } else {
        default_provider_auth_kind(kind)
    };
    let api_key = parse_optional_text(
        raw.api_key.unwrap_or_default().as_str(),
        format!("{source_name}.api_key").as_str(),
    )?;
    let api_key_secret_ref = raw
        .api_key_secret_ref
        .map(|secret_ref| {
            parse_structured_secret_ref_field(
                secret_ref,
                format!("{source_name}.api_key_secret_ref").as_str(),
            )
        })
        .transpose()?;
    let api_key_vault_ref = parse_optional_vault_ref_field(
        raw.api_key_vault_ref.unwrap_or_default().as_str(),
        format!("{source_name}.api_key_vault_ref").as_str(),
    )?;
    if api_key.is_some() && api_key_secret_ref.is_some() {
        anyhow::bail!(
            "{source_name} cannot set both api_key and api_key_secret_ref for the same provider entry"
        );
    }
    if api_key.is_some() && api_key_vault_ref.is_some() {
        anyhow::bail!(
            "{source_name} cannot set both api_key and api_key_vault_ref for the same provider entry"
        );
    }
    if api_key_secret_ref.is_some() && api_key_vault_ref.is_some() {
        anyhow::bail!(
            "{source_name} cannot set both api_key_secret_ref and api_key_vault_ref for the same provider entry"
        );
    }
    let resolved_api_key_secret_ref = api_key_secret_ref
        .clone()
        .or_else(|| api_key_vault_ref.clone().map(SecretRef::from_legacy_vault_ref));
    let credential_source = if api_key.is_some() {
        Some(ModelProviderCredentialSource::InlineConfig)
    } else if let Some(secret_ref) = resolved_api_key_secret_ref.as_ref() {
        Some(secret_ref_credential_source(secret_ref))
    } else if api_key_vault_ref.is_some() {
        Some(ModelProviderCredentialSource::VaultRef)
    } else if auth_profile_id.is_some() {
        auth_profile_provider_kind.map(|kind| match kind {
            ModelProviderAuthProviderKind::Openai => {
                ModelProviderCredentialSource::AuthProfileApiKey
            }
            ModelProviderAuthProviderKind::Anthropic => {
                ModelProviderCredentialSource::AuthProfileApiKey
            }
        })
    } else {
        None
    };

    Ok(ProviderRegistryEntryConfig {
        provider_id,
        display_name,
        kind,
        base_url,
        allow_private_base_url: raw
            .allow_private_base_url
            .unwrap_or(defaults.allow_private_base_url),
        enabled: raw.enabled.unwrap_or(true),
        auth_profile_id,
        auth_profile_provider_kind,
        api_key,
        api_key_secret_ref: resolved_api_key_secret_ref,
        api_key_vault_ref,
        credential_source,
        request_timeout_ms: raw
            .request_timeout_ms
            .map(|value| {
                parse_positive_u64(value, format!("{source_name}.request_timeout_ms").as_str())
            })
            .transpose()?
            .unwrap_or(defaults.request_timeout_ms),
        max_retries: raw
            .max_retries
            .map(|value| parse_retries(value, format!("{source_name}.max_retries").as_str()))
            .transpose()?
            .unwrap_or(defaults.max_retries),
        retry_backoff_ms: raw
            .retry_backoff_ms
            .map(|value| {
                parse_positive_u64(value, format!("{source_name}.retry_backoff_ms").as_str())
            })
            .transpose()?
            .unwrap_or(defaults.retry_backoff_ms),
        circuit_breaker_failure_threshold: raw
            .circuit_breaker_failure_threshold
            .map(|value| {
                parse_positive_u32(
                    value,
                    format!("{source_name}.circuit_breaker_failure_threshold").as_str(),
                )
            })
            .transpose()?
            .unwrap_or(defaults.circuit_breaker_failure_threshold),
        circuit_breaker_cooldown_ms: raw
            .circuit_breaker_cooldown_ms
            .map(|value| {
                parse_positive_u64(
                    value,
                    format!("{source_name}.circuit_breaker_cooldown_ms").as_str(),
                )
            })
            .transpose()?
            .unwrap_or(defaults.circuit_breaker_cooldown_ms),
    })
}

fn secret_ref_credential_source(secret_ref: &SecretRef) -> ModelProviderCredentialSource {
    match secret_ref.source {
        SecretSource::Vault { .. } => ModelProviderCredentialSource::VaultRef,
        _ => ModelProviderCredentialSource::SecretRef,
    }
}

fn parse_model_provider_registry_model(
    raw: palyra_common::daemon_config_schema::FileModelProviderRegistryModel,
    index: usize,
    providers: &[ProviderRegistryEntryConfig],
) -> Result<ProviderModelEntryConfig> {
    let source_name = format!("model_provider.models[{index}]");
    let model_id = parse_optional_text(
        raw.model_id.unwrap_or_default().as_str(),
        format!("{source_name}.model_id").as_str(),
    )?
    .ok_or_else(|| anyhow::anyhow!("{source_name}.model_id cannot be empty"))?;
    let provider_id = parse_registry_identifier(
        raw.provider_id.unwrap_or_default().as_str(),
        format!("{source_name}.provider_id").as_str(),
    )?;
    let role = parse_provider_model_role(
        raw.role.unwrap_or_else(|| "chat".to_owned()).as_str(),
        format!("{source_name}.role").as_str(),
    )?;
    let provider_kind = providers
        .iter()
        .find(|entry| entry.provider_id == provider_id)
        .map(|entry| entry.kind)
        .ok_or_else(|| {
            anyhow::anyhow!("{source_name}.provider_id references unknown provider '{provider_id}'")
        })?;
    let metadata_source = if let Some(value) = raw.metadata_source {
        parse_provider_metadata_source(
            value.as_str(),
            format!("{source_name}.metadata_source").as_str(),
        )?
    } else {
        ProviderMetadataSource::Static
    };
    let operator_override = raw.operator_override.unwrap_or(false);
    let defaults = provider_capability_defaults(provider_kind, role);
    let cost_tier = if let Some(value) = raw.cost_tier {
        parse_provider_cost_tier(value.as_str(), format!("{source_name}.cost_tier").as_str())?
            .as_str()
            .to_owned()
    } else {
        defaults.cost_tier.clone()
    };
    let latency_tier = if let Some(value) = raw.latency_tier {
        parse_provider_latency_tier(value.as_str(), format!("{source_name}.latency_tier").as_str())?
            .as_str()
            .to_owned()
    } else {
        defaults.latency_tier.clone()
    };
    let recommended_use_cases = raw
        .recommended_use_cases
        .as_ref()
        .map(|values| {
            parse_registry_string_list(
                values,
                format!("{source_name}.recommended_use_cases").as_str(),
            )
        })
        .transpose()?
        .unwrap_or_else(|| defaults.recommended_use_cases.clone());
    let known_limitations = raw
        .known_limitations
        .as_ref()
        .map(|values| {
            parse_registry_string_list(values, format!("{source_name}.known_limitations").as_str())
        })
        .transpose()?
        .unwrap_or_else(|| defaults.known_limitations.clone());

    Ok(ProviderModelEntryConfig {
        model_id,
        provider_id,
        role,
        enabled: raw.enabled.unwrap_or(true),
        metadata_source,
        operator_override,
        capabilities: ProviderCapabilitiesSnapshot {
            streaming_tokens: defaults.streaming_tokens,
            tool_calls: raw.tool_calls.unwrap_or(defaults.tool_calls),
            json_mode: raw.json_mode.unwrap_or(defaults.json_mode),
            vision: raw.vision.unwrap_or(defaults.vision),
            audio_transcribe: raw.audio_transcribe.unwrap_or(defaults.audio_transcribe),
            embeddings: raw.embeddings.unwrap_or(defaults.embeddings),
            max_context_tokens: raw.max_context_tokens.or(defaults.max_context_tokens),
            cost_tier,
            latency_tier,
            recommended_use_cases,
            known_limitations,
            operator_override,
            metadata_source: metadata_source.as_str().to_owned(),
        },
    })
}

fn parse_model_provider_auth_provider_kind(
    raw: &str,
    source_name: &str,
) -> Result<ModelProviderAuthProviderKind> {
    ModelProviderAuthProviderKind::parse(raw).with_context(|| {
        format!("{source_name} must be one of: openai, openai_compatible, anthropic")
    })
}

fn parse_identity_store_dir(raw: &str) -> Result<PathBuf> {
    if raw.trim().is_empty() {
        anyhow::bail!("identity store directory cannot be empty");
    }
    if raw.contains('\0') {
        anyhow::bail!("identity store directory cannot contain embedded NUL byte");
    }
    Ok(PathBuf::from(raw))
}

fn parse_gateway_tls_path(raw: &str) -> Result<PathBuf> {
    if raw.trim().is_empty() {
        anyhow::bail!("gateway tls path cannot be empty");
    }
    if raw.contains('\0') {
        anyhow::bail!("gateway tls path cannot contain embedded NUL byte");
    }
    Ok(PathBuf::from(raw))
}

fn parse_openai_base_url(raw: &str) -> Result<String> {
    if raw.trim().is_empty() {
        anyhow::bail!("openai base URL cannot be empty");
    }
    let normalized = raw.trim();
    let parsed =
        reqwest::Url::parse(normalized).context("openai base URL must be a valid absolute URL")?;
    let host =
        parsed.host_str().ok_or_else(|| anyhow::anyhow!("openai base URL must include a host"))?;
    let loopback_http_allowed = host.eq_ignore_ascii_case("localhost")
        || host.parse::<std::net::IpAddr>().is_ok_and(|ip| ip.is_loopback());
    if parsed.scheme() != "https" && !(parsed.scheme() == "http" && loopback_http_allowed) {
        anyhow::bail!("openai base URL must use https (http is only allowed for loopback hosts)");
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        anyhow::bail!("openai base URL must not embed credentials");
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        anyhow::bail!("openai base URL must not include query or fragment");
    }
    Ok(parsed.as_str().trim_end_matches('/').to_owned())
}

fn parse_openai_model(raw: &str) -> Result<String> {
    if raw.trim().is_empty() {
        anyhow::bail!("openai model cannot be empty");
    }
    Ok(raw.trim().to_owned())
}

fn parse_optional_openai_embeddings_model(raw: &str) -> Result<Option<String>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    Ok(Some(trimmed.to_owned()))
}

fn parse_openai_embeddings_dims(raw: u32, source_name: &str) -> Result<u32> {
    parse_positive_u32(raw, source_name)
}

fn push_unique_string(values: &mut Vec<String>, seen: &mut HashSet<String>, value: String) {
    if seen.insert(value.clone()) {
        values.push(value);
    }
}

fn parse_vault_ref_allowlist(raw: &str, source_name: &str) -> Result<Vec<String>> {
    let mut refs = Vec::new();
    let mut seen_refs = HashSet::new();
    for candidate in raw.split(',').map(str::trim).filter(|value| !value.is_empty()) {
        let parsed = VaultRef::parse(candidate).map_err(|error| {
            anyhow::anyhow!("{source_name} contains invalid vault ref '{candidate}': {error}")
        })?;
        let normalized = format!("{}/{}", parsed.scope, parsed.key).to_ascii_lowercase();
        push_unique_string(&mut refs, &mut seen_refs, normalized);
    }
    if refs.is_empty() {
        anyhow::bail!("{source_name} must include at least one <scope>/<key> entry");
    }
    Ok(refs)
}

fn parse_tool_allowlist(raw: &str, source_name: &str) -> Result<Vec<String>> {
    parse_identifier_allowlist(raw, source_name, "tool name")
}

fn parse_process_executable_allowlist(raw: &str, source_name: &str) -> Result<Vec<String>> {
    parse_identifier_allowlist(raw, source_name, "executable name")
}

fn parse_process_runner_tier(raw: &str, source_name: &str) -> Result<SandboxProcessRunnerTier> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "b" | "tier_b" => Ok(SandboxProcessRunnerTier::B),
        "c" | "tier_c" => Ok(SandboxProcessRunnerTier::C),
        _ => anyhow::bail!("{source_name} must be one of: b, c"),
    }
}

fn parse_process_runner_egress_enforcement_mode(
    raw: &str,
    source_name: &str,
) -> Result<EgressEnforcementMode> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "none" => Ok(EgressEnforcementMode::None),
        "preflight" => Ok(EgressEnforcementMode::Preflight),
        "strict" => Ok(EgressEnforcementMode::Strict),
        _ => anyhow::bail!("{source_name} must be one of: none, preflight, strict"),
    }
}

fn parse_cron_timezone_mode(raw: &str, source_name: &str) -> Result<CronTimezoneMode> {
    CronTimezoneMode::from_str(raw)
        .ok_or_else(|| anyhow::anyhow!("{source_name} must be one of: utc, local"))
}

fn parse_identifier_allowlist(raw: &str, source_name: &str, label: &str) -> Result<Vec<String>> {
    let mut allowlist = Vec::new();
    let mut seen_values = HashSet::new();
    for candidate in raw.split(',').map(str::trim).filter(|value| !value.is_empty()) {
        if !candidate.chars().all(|ch| {
            ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '.' | '_' | '-')
        }) {
            anyhow::bail!("{source_name} contains invalid {label} '{candidate}'");
        }
        push_unique_string(&mut allowlist, &mut seen_values, candidate.to_owned());
    }
    Ok(allowlist)
}

fn parse_workspace_root(raw: &str) -> Result<PathBuf> {
    if raw.trim().is_empty() {
        anyhow::bail!("process runner workspace root cannot be empty");
    }
    if raw.contains('\0') {
        anyhow::bail!("process runner workspace root cannot contain embedded NUL byte");
    }
    Ok(PathBuf::from(raw))
}

fn parse_host_allowlist(raw: &str, source_name: &str) -> Result<Vec<String>> {
    let mut allowlist = Vec::new();
    let mut seen_values = HashSet::new();
    for candidate in raw.split(',').map(str::trim).filter(|value| !value.is_empty()) {
        let normalized = normalize_host_candidate(candidate)
            .with_context(|| format!("{source_name} contains invalid host '{candidate}'"))?;
        push_unique_string(&mut allowlist, &mut seen_values, normalized);
    }
    Ok(allowlist)
}

fn parse_dns_suffix_allowlist(raw: &str, source_name: &str) -> Result<Vec<String>> {
    let mut allowlist = Vec::new();
    let mut seen_values = HashSet::new();
    for candidate in raw.split(',').map(str::trim).filter(|value| !value.is_empty()) {
        let normalized = normalize_dns_suffix_candidate(candidate)
            .with_context(|| format!("{source_name} contains invalid dns suffix '{candidate}'"))?;
        push_unique_string(&mut allowlist, &mut seen_values, normalized);
    }
    Ok(allowlist)
}

fn parse_content_type_allowlist(raw: &str, source_name: &str) -> Result<Vec<String>> {
    let mut allowlist = Vec::new();
    let mut seen_values = HashSet::new();
    for candidate in raw.split(',').map(str::trim).filter(|value| !value.is_empty()) {
        let normalized = candidate.to_ascii_lowercase();
        if normalized.len() > 128 {
            anyhow::bail!("{source_name} contains oversized content type '{candidate}'");
        }
        if !normalized.contains('/') || normalized.starts_with('/') || normalized.ends_with('/') {
            anyhow::bail!("{source_name} contains invalid content type '{candidate}'");
        }
        if normalized.contains(';') || normalized.chars().any(|ch| ch.is_ascii_whitespace()) {
            anyhow::bail!(
                "{source_name} content type entries must not include parameters or whitespace"
            );
        }
        if !normalized.chars().all(|ch| {
            ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '/' | '+' | '.' | '-')
        }) {
            anyhow::bail!("{source_name} contains invalid content type '{candidate}'");
        }
        push_unique_string(&mut allowlist, &mut seen_values, normalized);
    }
    if allowlist.is_empty() {
        anyhow::bail!("{source_name} must include at least one content type");
    }
    Ok(allowlist)
}

fn parse_http_header_allowlist(raw: &str, source_name: &str) -> Result<Vec<String>> {
    let mut allowlist = Vec::new();
    let mut seen_values = HashSet::new();
    for candidate in raw.split(',').map(str::trim).filter(|value| !value.is_empty()) {
        let normalized = candidate.to_ascii_lowercase();
        if normalized.len() > 128 {
            anyhow::bail!("{source_name} contains oversized header name '{candidate}'");
        }
        if normalized.starts_with('-')
            || normalized.ends_with('-')
            || !normalized
                .chars()
                .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-')
        {
            anyhow::bail!("{source_name} contains invalid header name '{candidate}'");
        }
        push_unique_string(&mut allowlist, &mut seen_values, normalized);
    }
    if allowlist.is_empty() {
        anyhow::bail!("{source_name} must include at least one header name");
    }
    Ok(allowlist)
}

fn parse_storage_prefix_allowlist(raw: &str, source_name: &str) -> Result<Vec<String>> {
    let mut allowlist = Vec::new();
    let mut seen_values = HashSet::new();
    for candidate in raw.split(',').map(str::trim).filter(|value| !value.is_empty()) {
        if candidate.contains('\0')
            || candidate.contains("..")
            || candidate.starts_with('/')
            || candidate.starts_with('\\')
            || !candidate.chars().all(|ch| {
                ch.is_ascii_lowercase()
                    || ch.is_ascii_digit()
                    || matches!(ch, '/' | '.' | '_' | '-')
            })
        {
            anyhow::bail!("{source_name} contains invalid storage prefix '{candidate}'");
        }
        push_unique_string(&mut allowlist, &mut seen_values, candidate.to_owned());
    }
    Ok(allowlist)
}

fn parse_broadcast_strategy(raw: &str, source_name: &str) -> Result<BroadcastStrategy> {
    BroadcastStrategy::parse(raw)
        .ok_or_else(|| anyhow::anyhow!("{source_name} must be one of: deny, mention_only, allow"))
}

fn parse_direct_message_policy(raw: &str, source_name: &str) -> Result<DirectMessagePolicy> {
    DirectMessagePolicy::parse(raw)
        .ok_or_else(|| anyhow::anyhow!("{source_name} must be one of: deny, pairing, allow"))
}

fn parse_optional_text_field(
    raw: String,
    source_name: &str,
    max_bytes: usize,
) -> Result<Option<String>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if trimmed.len() > max_bytes {
        anyhow::bail!("{source_name} exceeds maximum bytes ({} > {max_bytes})", trimmed.len());
    }
    Ok(Some(trimmed.to_owned()))
}

fn parse_browser_service_endpoint(raw: &str, source_name: &str) -> Result<String> {
    if raw.trim().is_empty() {
        anyhow::bail!("{source_name} cannot be empty");
    }
    let parsed = reqwest::Url::parse(raw.trim())
        .with_context(|| format!("{source_name} must be a valid absolute URL"))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        anyhow::bail!("{source_name} must use http or https scheme");
    }
    if parsed.host_str().is_none() {
        anyhow::bail!("{source_name} must include a host");
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        anyhow::bail!("{source_name} must not embed credentials");
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        anyhow::bail!("{source_name} must not include query or fragment");
    }
    if parsed.path() != "/" && !parsed.path().is_empty() {
        anyhow::bail!("{source_name} must not include a path");
    }
    Ok(parsed.as_str().trim_end_matches('/').to_owned())
}

fn parse_canvas_host_public_base_url(raw: &str, source_name: &str) -> Result<String> {
    if raw.trim().is_empty() {
        anyhow::bail!("{source_name} cannot be empty");
    }
    let parsed = reqwest::Url::parse(raw.trim())
        .with_context(|| format!("{source_name} must be a valid absolute URL"))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        anyhow::bail!("{source_name} must use http or https scheme");
    }
    if parsed.host_str().is_none() {
        anyhow::bail!("{source_name} must include a host");
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        anyhow::bail!("{source_name} must not embed credentials");
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        anyhow::bail!("{source_name} must not include query or fragment");
    }
    Ok(parsed.as_str().trim_end_matches('/').to_owned())
}

fn parse_channel_identifier(raw: &str, source_name: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        anyhow::bail!("{source_name} cannot be empty");
    }
    if !trimmed.chars().all(|ch| {
        ch.is_ascii_lowercase()
            || ch.is_ascii_uppercase()
            || ch.is_ascii_digit()
            || matches!(ch, '.' | '_' | '-' | ':')
    }) {
        anyhow::bail!("{source_name} contains invalid channel identifier '{trimmed}'");
    }
    Ok(trimmed.to_ascii_lowercase())
}

fn parse_sender_identifier_list(raw: &[String], source_name: &str) -> Result<Vec<String>> {
    let mut values = Vec::new();
    for candidate in raw.iter().map(String::as_str).map(str::trim).filter(|value| !value.is_empty())
    {
        if !candidate.chars().all(|ch| {
            ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-' | '@' | ':' | '/' | '#')
        }) {
            anyhow::bail!("{source_name} contains invalid sender identifier '{candidate}'");
        }
        let normalized = candidate.to_ascii_lowercase();
        if !values.iter().any(|existing| existing == &normalized) {
            values.push(normalized);
        }
    }
    Ok(values)
}

fn parse_mention_patterns(raw: &[String], source_name: &str) -> Result<Vec<String>> {
    if raw.len() > 64 {
        anyhow::bail!("{source_name} exceeds maximum entries ({} > 64)", raw.len());
    }
    let mut patterns = Vec::new();
    for candidate in raw.iter().map(String::as_str) {
        let trimmed = candidate.trim();
        if trimmed.is_empty() {
            anyhow::bail!("{source_name} cannot contain empty mention patterns");
        }
        if trimmed.len() > 128 {
            anyhow::bail!(
                "{source_name} contains oversized mention pattern ({} > 128)",
                trimmed.len()
            );
        }
        let normalized = trimmed.to_ascii_lowercase();
        if !patterns.iter().any(|existing| existing == &normalized) {
            patterns.push(normalized);
        }
    }
    Ok(patterns)
}

fn parse_channel_routing_rule(
    raw: palyra_common::daemon_config_schema::FileChannelRoutingRule,
    source_name: &str,
    defaults: &ChannelRouterConfig,
) -> Result<ChannelRoutingRule> {
    let channel = parse_channel_identifier(
        raw.channel.unwrap_or_default().as_str(),
        format!("{source_name}.channel").as_str(),
    )?;
    let mention_patterns = parse_mention_patterns(
        raw.mention_patterns.unwrap_or_default().as_slice(),
        format!("{source_name}.mention_patterns").as_str(),
    )?;
    let allow_from = parse_sender_identifier_list(
        raw.allow_from.unwrap_or_default().as_slice(),
        format!("{source_name}.allow_from").as_str(),
    )?;
    let deny_from = parse_sender_identifier_list(
        raw.deny_from.unwrap_or_default().as_slice(),
        format!("{source_name}.deny_from").as_str(),
    )?;
    let response_prefix = parse_optional_text_field(
        raw.response_prefix.unwrap_or_default(),
        format!("{source_name}.response_prefix").as_str(),
        256,
    )?;
    let auto_ack_text = parse_optional_text_field(
        raw.auto_ack_text.unwrap_or_default(),
        format!("{source_name}.auto_ack_text").as_str(),
        256,
    )?;
    let auto_reaction = parse_optional_text_field(
        raw.auto_reaction.unwrap_or_default(),
        format!("{source_name}.auto_reaction").as_str(),
        64,
    )?;
    let broadcast_strategy = if let Some(value) = raw.broadcast_strategy {
        parse_broadcast_strategy(
            value.as_str(),
            format!("{source_name}.broadcast_strategy").as_str(),
        )?
    } else {
        defaults.default_broadcast_strategy
    };
    let direct_message_policy = if let Some(value) = raw.direct_message_policy {
        parse_direct_message_policy(
            value.as_str(),
            format!("{source_name}.direct_message_policy").as_str(),
        )?
    } else {
        defaults.default_direct_message_policy
    };
    let concurrency_limit = if let Some(value) = raw.concurrency_limit {
        Some(parse_positive_usize(value, format!("{source_name}.concurrency_limit").as_str())?)
    } else {
        Some(defaults.default_concurrency_limit)
    };

    Ok(ChannelRoutingRule {
        channel,
        enabled: raw.enabled.unwrap_or(defaults.default_channel_enabled),
        mention_patterns,
        allow_from,
        deny_from,
        allow_direct_messages: raw
            .allow_direct_messages
            .unwrap_or(defaults.default_allow_direct_messages),
        direct_message_policy,
        isolate_session_by_sender: raw
            .isolate_session_by_sender
            .unwrap_or(defaults.default_isolate_session_by_sender),
        response_prefix,
        auto_ack_text,
        auto_reaction,
        broadcast_strategy,
        concurrency_limit,
    })
}

fn normalize_host_candidate(raw: &str) -> Result<String> {
    let trimmed = raw.trim().trim_end_matches('.').to_ascii_lowercase();
    if trimmed.is_empty() {
        anyhow::bail!("host cannot be empty");
    }
    if !trimmed
        .chars()
        .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '.' | '-'))
    {
        anyhow::bail!("host must contain only ASCII letters, digits, dots, and hyphens");
    }
    if trimmed.starts_with('-')
        || trimmed.ends_with('-')
        || trimmed.starts_with('.')
        || trimmed.ends_with('.')
        || trimmed.contains("..")
    {
        anyhow::bail!("host has invalid dot/hyphen placement");
    }
    Ok(trimmed)
}

fn normalize_dns_suffix_candidate(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        anyhow::bail!("dns suffix cannot be empty");
    }
    if trimmed.contains("..") {
        anyhow::bail!("dns suffix cannot contain empty labels");
    }
    let normalized_host = normalize_host_candidate(trimmed.trim_start_matches('.'))?;
    Ok(format!(".{normalized_host}"))
}

fn parse_positive_u64(value: u64, name: &str) -> Result<u64> {
    if value == 0 {
        anyhow::bail!("{name} must be greater than 0");
    }
    Ok(value)
}

fn parse_positive_u32(value: u32, name: &str) -> Result<u32> {
    if value == 0 {
        anyhow::bail!("{name} must be greater than 0");
    }
    Ok(value)
}

fn parse_positive_i64(value: i64, name: &str) -> Result<i64> {
    if value <= 0 {
        anyhow::bail!("{name} must be greater than 0");
    }
    Ok(value)
}

fn parse_positive_usize(value: u64, name: &str) -> Result<usize> {
    if value == 0 {
        anyhow::bail!("{name} must be greater than 0");
    }
    usize::try_from(value).with_context(|| format!("{name} exceeds platform usize range"))
}

fn parse_default_memory_ttl_ms(value: i64, name: &str) -> Result<Option<i64>> {
    if value < 0 {
        anyhow::bail!("{name} must be >= 0");
    }
    if value == 0 {
        return Ok(None);
    }
    Ok(Some(value))
}

fn parse_memory_retention_vacuum_schedule(raw: &str, name: &str) -> Result<String> {
    let normalized = raw.split_whitespace().collect::<Vec<_>>();
    if normalized.is_empty() {
        anyhow::bail!("{name} must not be empty");
    }
    if normalized.len() < 5 || normalized.len() > 6 {
        anyhow::bail!("{name} must be a cron expression with 5 or 6 fields");
    }
    let joined = normalized.join(" ");
    if joined.len() > 128 {
        anyhow::bail!("{name} must be <= 128 characters");
    }
    Ok(joined)
}

fn parse_retrieval_backend_kind(raw: &str, name: &str) -> Result<RetrievalBackendKind> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "journal_sqlite_fts" | "journal-sqlite-fts" | "journal" | "sqlite_fts" | "sqlite-fts" => {
            Ok(RetrievalBackendKind::JournalSqliteFts)
        }
        _ => anyhow::bail!("{name} must be one of: journal_sqlite_fts"),
    }
}

fn parse_basis_points(value: u16, name: &str) -> Result<u16> {
    if value > 10_000 {
        anyhow::bail!("{name} must be between 0 and 10000");
    }
    Ok(value)
}

fn apply_retrieval_profile_override(
    profile: &mut RetrievalSourceScoringProfile,
    file_profile: FileRetrievalSourceScoringProfile,
    prefix: &str,
) -> Result<()> {
    if let Some(value) = file_profile.lexical_bps {
        profile.lexical_bps = parse_basis_points(value, format!("{prefix}.lexical_bps").as_str())?;
    }
    if let Some(value) = file_profile.vector_bps {
        profile.vector_bps = parse_basis_points(value, format!("{prefix}.vector_bps").as_str())?;
    }
    if let Some(value) = file_profile.recency_bps {
        profile.recency_bps = parse_basis_points(value, format!("{prefix}.recency_bps").as_str())?;
    }
    if let Some(value) = file_profile.source_quality_bps {
        profile.source_quality_bps =
            parse_basis_points(value, format!("{prefix}.source_quality_bps").as_str())?;
    }
    if let Some(value) = file_profile.min_recency_bps {
        profile.min_recency_bps =
            parse_basis_points(value, format!("{prefix}.min_recency_bps").as_str())?;
    }
    if let Some(value) = file_profile.min_source_quality_bps {
        profile.min_source_quality_bps =
            parse_basis_points(value, format!("{prefix}.min_source_quality_bps").as_str())?;
    }
    if let Some(value) = file_profile.pinned_bonus_bps {
        profile.pinned_bonus_bps =
            parse_basis_points(value, format!("{prefix}.pinned_bonus_bps").as_str())?;
    }
    Ok(())
}

fn apply_memory_retrieval_config(
    config: &mut RetrievalRuntimeConfig,
    file_retrieval: FileMemoryRetrievalConfig,
) -> Result<()> {
    if let Some(file_backend) = file_retrieval.backend {
        if let Some(kind) = file_backend.kind {
            config.backend.kind =
                parse_retrieval_backend_kind(kind.as_str(), "memory.retrieval.backend.kind")?;
        }
    }
    if let Some(file_scoring) = file_retrieval.scoring {
        if let Some(value) = file_scoring.phrase_match_bonus_bps {
            config.scoring.phrase_match_bonus_bps =
                parse_basis_points(value, "memory.retrieval.scoring.phrase_match_bonus_bps")?;
        }
        if let Some(default_profile) = file_scoring.default_profile {
            apply_retrieval_profile_override(
                &mut config.scoring.default_profile,
                default_profile,
                "memory.retrieval.scoring.default_profile",
            )?;
        }
        if let Some(memory) = file_scoring.memory {
            apply_retrieval_profile_override(
                &mut config.scoring.memory,
                memory,
                "memory.retrieval.scoring.memory",
            )?;
        }
        if let Some(workspace) = file_scoring.workspace {
            apply_retrieval_profile_override(
                &mut config.scoring.workspace,
                workspace,
                "memory.retrieval.scoring.workspace",
            )?;
        }
        if let Some(transcript) = file_scoring.transcript {
            apply_retrieval_profile_override(
                &mut config.scoring.transcript,
                transcript,
                "memory.retrieval.scoring.transcript",
            )?;
        }
        if let Some(checkpoint) = file_scoring.checkpoint {
            apply_retrieval_profile_override(
                &mut config.scoring.checkpoint,
                checkpoint,
                "memory.retrieval.scoring.checkpoint",
            )?;
        }
        if let Some(compaction) = file_scoring.compaction {
            apply_retrieval_profile_override(
                &mut config.scoring.compaction,
                compaction,
                "memory.retrieval.scoring.compaction",
            )?;
        }
    }
    config.validate()
}

fn parse_retries(value: u32, name: &str) -> Result<u32> {
    const MAX_RETRIES: u32 = 10;
    if value > MAX_RETRIES {
        anyhow::bail!("{name} must be <= {MAX_RETRIES}");
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use std::{
        ffi::OsString,
        path::PathBuf,
        sync::{Mutex, OnceLock},
    };

    use super::{
        apply_feature_rollout_env_override, parse_broadcast_strategy,
        parse_browser_service_endpoint, parse_canvas_host_public_base_url,
        parse_content_type_allowlist, parse_cron_timezone_mode, parse_default_memory_ttl_ms,
        parse_direct_message_policy, parse_dns_suffix_allowlist, parse_host_allowlist,
        parse_http_header_allowlist, parse_journal_db_path, parse_memory_retention_vacuum_schedule,
        parse_model_provider_auth_provider_kind, parse_model_provider_registry_entry,
        parse_model_provider_registry_model, parse_openai_base_url, parse_openai_embeddings_dims,
        parse_optional_auth_profile_id, parse_optional_browser_state_dir,
        parse_optional_openai_embeddings_model, parse_optional_vault_ref_field, parse_positive_u32,
        parse_positive_usize, parse_process_executable_allowlist,
        parse_process_runner_egress_enforcement_mode, parse_process_runner_tier,
        parse_root_file_config, parse_storage_prefix_allowlist, parse_tool_allowlist,
        parse_vault_dir, parse_vault_ref_allowlist, validate_runtime_preview_config, AdminConfig,
        AuxiliaryExecutorConfig, BrowserServiceConfig, CanvasHostConfig, ChannelRouterConfig,
        CronConfig, DeliveryArbitrationConfig, DeploymentConfig, DeploymentMode,
        FlowOrchestrationConfig, GatewayBindProfile, GatewayConfig, GatewayTlsConfig,
        HttpFetchConfig, IdentityConfig, MemoryConfig, ModelProviderConfig, NetworkedWorkersConfig,
        OrchestratorConfig, PruningPolicyMatrixConfig, ReplayCaptureConfig,
        RetrievalDualPathConfig, SessionQueuePolicyConfig, StorageConfig, ToolCallConfig,
    };
    use crate::channel_router::{BroadcastStrategy, DirectMessagePolicy};
    use crate::model_provider::{
        ModelProviderAuthProviderKind, ModelProviderKind, ProviderMetadataSource, ProviderModelRole,
    };
    use crate::sandbox_runner::{EgressEnforcementMode, SandboxProcessRunnerTier};
    use palyra_common::{
        daemon_config_schema::{
            FileModelProviderRegistryEntry, FileModelProviderRegistryModel, RootFileConfig,
        },
        feature_rollouts::{
            FeatureRolloutSetting, FeatureRolloutSource, DYNAMIC_TOOL_BUILDER_ROLLOUT_ENV,
            SESSION_QUEUE_POLICY_ROLLOUT_ENV,
        },
        runtime_preview::RuntimePreviewMode,
    };

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    struct ScopedEnvVar {
        key: &'static str,
        previous: Option<OsString>,
    }

    impl ScopedEnvVar {
        fn set(key: &'static str, value: &str) -> Self {
            let previous = std::env::var_os(key);
            unsafe {
                std::env::set_var(key, value);
            }
            Self { key, previous }
        }

        fn unset(key: &'static str) -> Self {
            let previous = std::env::var_os(key);
            unsafe {
                std::env::remove_var(key);
            }
            Self { key, previous }
        }
    }

    impl Drop for ScopedEnvVar {
        fn drop(&mut self) {
            if let Some(previous) = self.previous.take() {
                unsafe {
                    std::env::set_var(self.key, previous);
                }
            } else {
                unsafe {
                    std::env::remove_var(self.key);
                }
            }
        }
    }

    #[test]
    fn identity_config_defaults_to_secure_mode() {
        let config = IdentityConfig::default();
        assert!(!config.allow_insecure_node_rpc_without_mtls);
    }

    #[test]
    fn identity_config_parses_file_override() {
        let parsed: RootFileConfig = toml::from_str(
            r#"
            [identity]
            allow_insecure_node_rpc_without_mtls = true
            "#,
        )
        .expect("toml should parse");
        let identity = parsed.identity.expect("identity config should be present");
        assert_eq!(identity.allow_insecure_node_rpc_without_mtls, Some(true));
    }

    #[test]
    fn deployment_config_defaults_to_local_desktop_with_danger_ack_disabled() {
        let config = DeploymentConfig::default();
        assert_eq!(config.mode, DeploymentMode::LocalDesktop);
        assert!(
            !config.dangerous_remote_bind_ack,
            "danger acknowledgement must default to disabled"
        );
    }

    #[test]
    fn gateway_config_defaults_to_quic_and_grpc_loopback() {
        let config = GatewayConfig::default();
        assert_eq!(config.grpc_bind_addr, "127.0.0.1");
        assert_eq!(config.grpc_port, 7443);
        assert_eq!(config.quic_bind_addr, "127.0.0.1");
        assert_eq!(config.quic_port, 7444);
        assert!(config.quic_enabled, "gateway transport should default to QUIC-enabled mode");
        assert_eq!(
            config.bind_profile,
            GatewayBindProfile::LoopbackOnly,
            "gateway bind profile should default to loopback-only"
        );
        assert!(
            !config.allow_insecure_remote,
            "remote exposure must require explicit insecure opt-in"
        );
        assert_eq!(
            config.vault_get_approval_required_refs,
            vec!["global/openai_api_key".to_owned()],
            "sensitive vault reads should require explicit approval by default"
        );
        assert_eq!(config.max_tape_entries_per_response, 1_000);
        assert_eq!(config.max_tape_bytes_per_response, 2 * 1024 * 1024);
        assert_eq!(config.tls, GatewayTlsConfig::default());
    }

    #[test]
    fn deployment_and_gateway_bind_profile_parse_expected_values() {
        let (parsed, _) = parse_root_file_config(
            r#"
            [deployment]
            mode = "remote_vps"
            dangerous_remote_bind_ack = true

            [gateway]
            bind_profile = "public_tls"
            "#,
        )
        .expect("deployment and bind profile should parse");
        let deployment = parsed.deployment.expect("deployment section should exist");
        assert_eq!(deployment.mode.as_deref(), Some("remote_vps"));
        assert_eq!(deployment.dangerous_remote_bind_ack, Some(true));
        let gateway = parsed.gateway.expect("gateway section should exist");
        assert_eq!(gateway.bind_profile.as_deref(), Some("public_tls"));
    }

    #[test]
    fn gateway_config_parses_vault_get_approval_required_refs() {
        let (parsed, _) = parse_root_file_config(
            r#"
            [gateway]
            vault_get_approval_required_refs = [
                "global/openai_api_key",
                "principal:user/openai_api_key",
            ]
            "#,
        )
        .expect("gateway vault approval refs should parse");
        let gateway = parsed.gateway.expect("gateway section should exist");
        assert_eq!(
            gateway.vault_get_approval_required_refs,
            Some(vec![
                "global/openai_api_key".to_owned(),
                "principal:user/openai_api_key".to_owned(),
            ])
        );
    }

    #[test]
    fn cron_config_defaults_to_utc_timezone() {
        let config = CronConfig::default();
        assert_eq!(
            config.timezone,
            crate::cron::CronTimezoneMode::Utc,
            "cron scheduler should default to UTC for deterministic cross-host behavior"
        );
    }

    #[test]
    fn feature_rollouts_config_parses_expected_values() {
        let (parsed, _) = parse_root_file_config(
            r#"
            [feature_rollouts]
            dynamic_tool_builder = true
            context_engine = true
            execution_backend_remote_node = false
            execution_backend_networked_worker = true
            execution_backend_ssh_tunnel = true
            safety_boundary = true
            execution_gate_pipeline_v2 = false
            session_queue_policy = true
            pruning_policy_matrix = false
            retrieval_dual_path = true
            auxiliary_executor = false
            flow_orchestration = true
            delivery_arbitration = false
            replay_capture = true
            networked_workers = false
            "#,
        )
        .expect("feature_rollouts should parse");
        let feature_rollouts =
            parsed.feature_rollouts.expect("feature_rollouts section should be present");
        assert_eq!(feature_rollouts.dynamic_tool_builder, Some(true));
        assert_eq!(feature_rollouts.context_engine, Some(true));
        assert_eq!(feature_rollouts.execution_backend_remote_node, Some(false));
        assert_eq!(feature_rollouts.execution_backend_networked_worker, Some(true));
        assert_eq!(feature_rollouts.execution_backend_ssh_tunnel, Some(true));
        assert_eq!(feature_rollouts.safety_boundary, Some(true));
        assert_eq!(feature_rollouts.execution_gate_pipeline_v2, Some(false));
        assert_eq!(feature_rollouts.session_queue_policy, Some(true));
        assert_eq!(feature_rollouts.pruning_policy_matrix, Some(false));
        assert_eq!(feature_rollouts.retrieval_dual_path, Some(true));
        assert_eq!(feature_rollouts.auxiliary_executor, Some(false));
        assert_eq!(feature_rollouts.flow_orchestration, Some(true));
        assert_eq!(feature_rollouts.delivery_arbitration, Some(false));
        assert_eq!(feature_rollouts.replay_capture, Some(true));
        assert_eq!(feature_rollouts.networked_workers, Some(false));
    }

    #[test]
    fn feature_rollout_env_override_defaults_to_disabled_without_overrides() {
        let _lock = env_lock().lock().expect("env lock should be available");
        let _clear = ScopedEnvVar::unset(DYNAMIC_TOOL_BUILDER_ROLLOUT_ENV);
        let mut source = "defaults".to_owned();
        let setting = apply_feature_rollout_env_override(
            FeatureRolloutSetting::default(),
            DYNAMIC_TOOL_BUILDER_ROLLOUT_ENV,
            &mut source,
        )
        .expect("missing env should preserve default");
        assert!(!setting.enabled, "missing env should not accidentally enable the rollout");
        assert_eq!(setting.source, FeatureRolloutSource::Default);
        assert_eq!(source, "defaults");
    }

    #[test]
    fn feature_rollout_env_override_takes_precedence_over_config_value() {
        let _lock = env_lock().lock().expect("env lock should be available");
        let _env = ScopedEnvVar::set(DYNAMIC_TOOL_BUILDER_ROLLOUT_ENV, "off");
        let mut source = "config:/tmp/palyra.toml".to_owned();
        let setting = apply_feature_rollout_env_override(
            FeatureRolloutSetting::from_config(true),
            DYNAMIC_TOOL_BUILDER_ROLLOUT_ENV,
            &mut source,
        )
        .expect("env override should parse");
        assert!(!setting.enabled, "env override should win over config");
        assert_eq!(setting.source, FeatureRolloutSource::Env);
        assert!(source.contains(DYNAMIC_TOOL_BUILDER_ROLLOUT_ENV));
    }

    #[test]
    fn feature_rollout_env_override_rejects_invalid_values() {
        let _lock = env_lock().lock().expect("env lock should be available");
        let _env = ScopedEnvVar::set(DYNAMIC_TOOL_BUILDER_ROLLOUT_ENV, "maybe");
        let mut source = "defaults".to_owned();
        let error = apply_feature_rollout_env_override(
            FeatureRolloutSetting::default(),
            DYNAMIC_TOOL_BUILDER_ROLLOUT_ENV,
            &mut source,
        )
        .expect_err("invalid env value should fail");
        let rendered = error.to_string();
        assert!(rendered.contains(DYNAMIC_TOOL_BUILDER_ROLLOUT_ENV));
        assert!(rendered.contains("boolean-like value"));
    }

    #[test]
    fn runtime_preview_config_validation_rejects_enabled_mode_without_rollout() {
        let error = validate_runtime_preview_config(
            &crate::config::FeatureRolloutsConfig::default(),
            &SessionQueuePolicyConfig {
                mode: RuntimePreviewMode::Enabled,
                ..SessionQueuePolicyConfig::default()
            },
            &PruningPolicyMatrixConfig::default(),
            &RetrievalDualPathConfig::default(),
            &AuxiliaryExecutorConfig::default(),
            &FlowOrchestrationConfig::default(),
            &DeliveryArbitrationConfig::default(),
            &ReplayCaptureConfig::default(),
            &NetworkedWorkersConfig::default(),
        )
        .expect_err("enabled mode without rollout should fail");
        let rendered = error.to_string();
        assert!(rendered.contains("session_queue_policy.mode=enabled"));
        assert!(rendered.contains("feature_rollouts.session_queue_policy"));
        assert!(rendered.contains(SESSION_QUEUE_POLICY_ROLLOUT_ENV));
    }

    #[test]
    fn runtime_preview_config_validation_accepts_preview_only_defaults() {
        validate_runtime_preview_config(
            &crate::config::FeatureRolloutsConfig::default(),
            &SessionQueuePolicyConfig::default(),
            &PruningPolicyMatrixConfig::default(),
            &RetrievalDualPathConfig::default(),
            &AuxiliaryExecutorConfig::default(),
            &FlowOrchestrationConfig::default(),
            &DeliveryArbitrationConfig::default(),
            &ReplayCaptureConfig::default(),
            &NetworkedWorkersConfig::default(),
        )
        .expect("preview defaults should validate");
    }

    #[test]
    fn cron_config_parses_timezone_override() {
        let (parsed, _) = parse_root_file_config(
            r#"
            [cron]
            timezone = "local"
            "#,
        )
        .expect("cron timezone override should parse");
        let cron = parsed.cron.expect("cron section should be present");
        assert_eq!(cron.timezone.as_deref(), Some("local"));
    }

    #[test]
    fn orchestrator_config_defaults_to_disabled_runloop() {
        let config = OrchestratorConfig::default();
        assert!(
            !config.runloop_v1_enabled,
            "orchestrator run loop should default disabled until explicitly enabled"
        );
    }

    #[test]
    fn memory_config_defaults_to_constrained_ingestion_with_auto_inject_disabled() {
        let config = MemoryConfig::default();
        assert_eq!(config.max_item_bytes, 16 * 1024);
        assert_eq!(config.max_item_tokens, 2_048);
        assert_eq!(config.default_ttl_ms, Some(30 * 24 * 60 * 60 * 1_000));
        assert!(!config.auto_inject.enabled, "memory auto-inject must default to disabled");
        assert_eq!(config.auto_inject.max_items, 3);
        assert!(
            config.retention.max_entries.is_none(),
            "retention max entries should default to unset"
        );
        assert!(
            config.retention.max_bytes.is_none(),
            "retention max bytes should default to unset"
        );
        assert!(config.retention.ttl_days.is_none(), "retention ttl days should default to unset");
        assert_eq!(
            config.retention.vacuum_schedule, "0 0 * * 0",
            "retention vacuum schedule should default to weekly cadence"
        );
    }

    #[test]
    fn memory_config_parses_retention_fields() {
        let (parsed, _) = parse_root_file_config(
            r#"
            [memory.retention]
            max_entries = 5000
            max_bytes = 10485760
            ttl_days = 30
            vacuum_schedule = "0 3 * * 0"
            "#,
        )
        .expect("memory retention fields should parse");
        let memory = parsed.memory.expect("memory section should exist");
        let retention = memory.retention.expect("memory.retention section should exist");
        assert_eq!(retention.max_entries, Some(5000));
        assert_eq!(retention.max_bytes, Some(10485760));
        assert_eq!(retention.ttl_days, Some(30));
        assert_eq!(retention.vacuum_schedule.as_deref(), Some("0 3 * * 0"));
    }

    #[test]
    fn memory_retrieval_config_applies_backend_and_scoring_overrides() {
        let (parsed, _) = parse_root_file_config(
            r#"
            [memory.retrieval.backend]
            kind = "journal_sqlite_fts"

            [memory.retrieval.scoring]
            phrase_match_bonus_bps = 2500

            [memory.retrieval.scoring.memory]
            lexical_bps = 5200
            vector_bps = 3200
            recency_bps = 1000
            source_quality_bps = 600
            min_recency_bps = 1200
            min_source_quality_bps = 1500
            pinned_bonus_bps = 0

            [memory.retrieval.scoring.workspace]
            lexical_bps = 4800
            vector_bps = 2800
            recency_bps = 1200
            source_quality_bps = 1200
            min_recency_bps = 1500
            min_source_quality_bps = 1800
            pinned_bonus_bps = 800
            "#,
        )
        .expect("memory retrieval config should parse");
        let file_memory = parsed.memory.expect("memory section should exist");
        let file_retrieval = file_memory.retrieval.expect("memory.retrieval section should exist");

        let mut runtime = MemoryConfig::default();
        super::apply_memory_retrieval_config(&mut runtime.retrieval, file_retrieval)
            .expect("memory retrieval overrides should apply");

        assert_eq!(
            runtime.retrieval.scoring.phrase_match_bonus_bps, 2_500,
            "phrase match bonus should follow file override"
        );
        assert_eq!(
            runtime.retrieval.scoring.memory.lexical_bps, 5_200,
            "memory lexical weight should be configurable"
        );
        assert_eq!(
            runtime.retrieval.scoring.workspace.pinned_bonus_bps, 800,
            "workspace pinned bonus should follow file override"
        );
        runtime.retrieval.validate().expect("applied retrieval config should remain valid");
    }

    #[test]
    fn memory_retrieval_config_rejects_invalid_weight_sum() {
        let (parsed, _) = parse_root_file_config(
            r#"
            [memory.retrieval.scoring.memory]
            lexical_bps = 7000
            vector_bps = 2500
            recency_bps = 500
            source_quality_bps = 500
            "#,
        )
        .expect("invalid retrieval weight sum should still parse as TOML");
        let file_memory = parsed.memory.expect("memory section should exist");
        let file_retrieval = file_memory.retrieval.expect("memory.retrieval section should exist");

        let mut runtime = MemoryConfig::default();
        let error = super::apply_memory_retrieval_config(&mut runtime.retrieval, file_retrieval)
            .expect_err("invalid retrieval weights must be rejected during override application");
        assert!(
            error.to_string().contains("memory.retrieval.scoring.memory weights must sum to 10000"),
            "validation error should explain the invalid retrieval weight sum: {error}"
        );
    }

    #[test]
    fn channel_router_defaults_to_disabled_deny_by_default() {
        let config = ChannelRouterConfig::default();
        assert!(!config.enabled, "channel router must require explicit opt-in");
        assert_eq!(config.max_message_bytes, 32 * 1024);
        assert_eq!(config.max_retry_queue_depth_per_channel, 64);
        assert_eq!(config.max_retry_attempts, 3);
        assert_eq!(config.retry_backoff_ms, 250);
        assert!(
            !config.default_channel_enabled,
            "per-channel routing should default disabled until explicitly configured"
        );
        assert_eq!(config.default_direct_message_policy, DirectMessagePolicy::Deny);
        assert_eq!(config.default_broadcast_strategy, BroadcastStrategy::Deny);
        assert_eq!(config.default_concurrency_limit, 2);
    }

    #[test]
    fn channel_router_config_parses_routing_rules() {
        let (parsed, _) = parse_root_file_config(
            r#"
            [channel_router]
            enabled = true
            max_message_bytes = 2048
            max_retry_queue_depth_per_channel = 8
            max_retry_attempts = 2
            retry_backoff_ms = 150
            default_response_prefix = "Palyra: "

            [channel_router.routing]
            default_channel_enabled = false
            default_allow_direct_messages = false
            default_direct_message_policy = "pairing"
            default_isolate_session_by_sender = true
            default_broadcast_strategy = "mention_only"
            default_concurrency_limit = 3
            channels = [
                { channel = "slack", enabled = true, mention_patterns = ["@palyra"], allow_from = ["U123"], allow_direct_messages = true, direct_message_policy = "allow", broadcast_strategy = "allow", concurrency_limit = 1 }
            ]
            "#,
        )
        .expect("channel router config should parse");
        let channel_router = parsed.channel_router.expect("channel_router section should exist");
        assert_eq!(channel_router.enabled, Some(true));
        assert_eq!(channel_router.max_message_bytes, Some(2048));
        let routing = channel_router.routing.expect("routing section should exist");
        assert_eq!(routing.default_concurrency_limit, Some(3));
        assert_eq!(routing.default_direct_message_policy.as_deref(), Some("pairing"));
        let channels = routing.channels.expect("channels list should exist");
        assert_eq!(channels.len(), 1);
        assert_eq!(channels[0].channel.as_deref(), Some("slack"));
        assert_eq!(channels[0].direct_message_policy.as_deref(), Some("allow"));
        assert_eq!(channels[0].broadcast_strategy.as_deref(), Some("allow"));
    }

    #[test]
    fn media_config_parses_attachment_pipeline_limits() {
        let (parsed, _) = parse_root_file_config(
            r#"
            [media]
            download_enabled = true
            outbound_upload_enabled = true
            max_attachments_per_message = 2
            max_total_attachment_bytes_per_message = 8192
            allowed_source_hosts = ["cdn.discordapp.com", "media.discordapp.net"]
            allowed_download_content_types = ["image/png", "image/jpeg"]
            vision_allowed_content_types = ["image/png"]
            outbound_allowed_content_types = ["image/png", "text/plain"]
            outbound_max_upload_bytes = 4096
            retention_ttl_ms = 86400000
            "#,
        )
        .expect("media attachment pipeline config should parse");

        let media = parsed.media.expect("media section should exist");
        assert_eq!(media.download_enabled, Some(true));
        assert_eq!(media.outbound_upload_enabled, Some(true));
        assert_eq!(media.max_attachments_per_message, Some(2));
        assert_eq!(media.max_total_attachment_bytes_per_message, Some(8192));
        assert_eq!(
            media.allowed_source_hosts,
            Some(vec!["cdn.discordapp.com".to_owned(), "media.discordapp.net".to_owned()])
        );
        assert_eq!(
            media.allowed_download_content_types,
            Some(vec!["image/png".to_owned(), "image/jpeg".to_owned()])
        );
        assert_eq!(media.vision_allowed_content_types, Some(vec!["image/png".to_owned()]));
        assert_eq!(
            media.outbound_allowed_content_types,
            Some(vec!["image/png".to_owned(), "text/plain".to_owned()])
        );
        assert_eq!(media.outbound_max_upload_bytes, Some(4096));
        assert_eq!(media.retention_ttl_ms, Some(86_400_000));
    }

    #[test]
    fn parse_broadcast_strategy_accepts_and_rejects_expected_values() {
        assert_eq!(
            parse_broadcast_strategy("deny", "channel_router.routing.default_broadcast_strategy")
                .expect("deny should parse"),
            BroadcastStrategy::Deny
        );
        assert_eq!(
            parse_broadcast_strategy(
                "mention_only",
                "channel_router.routing.default_broadcast_strategy",
            )
            .expect("mention_only should parse"),
            BroadcastStrategy::MentionOnly
        );
        assert_eq!(
            parse_broadcast_strategy("allow", "channel_router.routing.default_broadcast_strategy")
                .expect("allow should parse"),
            BroadcastStrategy::Allow
        );
        assert!(
            parse_broadcast_strategy("always", "channel_router.routing.default_broadcast_strategy")
                .is_err(),
            "unsupported broadcast strategy should be rejected"
        );
    }

    #[test]
    fn parse_direct_message_policy_accepts_and_rejects_expected_values() {
        assert_eq!(
            parse_direct_message_policy(
                "deny",
                "channel_router.routing.default_direct_message_policy",
            )
            .expect("deny should parse"),
            DirectMessagePolicy::Deny
        );
        assert_eq!(
            parse_direct_message_policy(
                "pairing",
                "channel_router.routing.default_direct_message_policy",
            )
            .expect("pairing should parse"),
            DirectMessagePolicy::Pairing
        );
        assert_eq!(
            parse_direct_message_policy(
                "allow",
                "channel_router.routing.default_direct_message_policy",
            )
            .expect("allow should parse"),
            DirectMessagePolicy::Allow
        );
        assert!(
            parse_direct_message_policy(
                "always",
                "channel_router.routing.default_direct_message_policy",
            )
            .is_err(),
            "unsupported DM policy should be rejected"
        );
    }

    #[test]
    fn tool_call_config_defaults_to_deny_by_default_with_execution_limits() {
        let config = ToolCallConfig::default();
        assert!(
            config.allowed_tools.is_empty(),
            "tool call allowlist must default empty to enforce deny-by-default"
        );
        assert_eq!(config.max_calls_per_run, 4);
        assert_eq!(config.execution_timeout_ms, 750);
        assert!(!config.process_runner.enabled, "sandbox process runner must default to disabled");
        assert_eq!(
            config.process_runner.tier,
            SandboxProcessRunnerTier::B,
            "process runner tier must default to tier b until operator opts into tier c"
        );
        assert!(
            config.process_runner.allowed_executables.is_empty(),
            "sandbox process runner executable allowlist must default empty"
        );
        assert!(
            !config.process_runner.allow_interpreters,
            "interpreter execution must default to explicit opt-in"
        );
        assert_eq!(
            config.process_runner.egress_enforcement_mode,
            EgressEnforcementMode::Strict,
            "process runner egress enforcement must default to strict"
        );
        assert!(!config.wasm_runtime.enabled, "wasm plugin runtime must default to disabled");
        assert!(
            !config.wasm_runtime.allow_inline_modules,
            "inline wasm module payloads must default to explicit opt-in"
        );
        assert_eq!(config.wasm_runtime.max_module_size_bytes, 256 * 1024);
        assert!(
            config.wasm_runtime.allowed_http_hosts.is_empty(),
            "wasm runtime http allowlist must default empty"
        );
        assert!(
            !config.http_fetch.allow_private_targets,
            "http fetch must default to private-target denial"
        );
        assert_eq!(config.http_fetch.max_response_bytes, 512 * 1024);
        assert_eq!(config.http_fetch.max_redirects, 3);
        assert!(
            config.http_fetch.allowed_content_types.iter().any(|value| value == "text/html"),
            "http fetch default content-type allowlist should include text/html"
        );
        assert!(
            !config.browser_service.enabled,
            "browser service broker must default to explicit opt-in"
        );
        assert_eq!(config.browser_service.endpoint, "http://127.0.0.1:7543");
    }

    #[test]
    fn http_fetch_config_defaults_enforce_safe_limits() {
        let config = HttpFetchConfig::default();
        assert!(!config.allow_private_targets);
        assert_eq!(config.connect_timeout_ms, 1_500);
        assert_eq!(config.request_timeout_ms, 10_000);
        assert_eq!(config.max_response_bytes, 512 * 1024);
        assert!(config.allow_redirects);
        assert_eq!(config.max_redirects, 3);
        assert!(config.cache_enabled);
        assert_eq!(config.cache_ttl_ms, 30_000);
        assert_eq!(config.max_cache_entries, 256);
    }

    #[test]
    fn browser_service_config_defaults_are_local_and_bounded() {
        let config = BrowserServiceConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.endpoint, "http://127.0.0.1:7543");
        assert!(config.auth_token.is_none());
        assert!(
            config.state_dir.is_none(),
            "browser service state_dir should default to unset unless explicitly configured"
        );
        assert!(
            config.state_key_vault_ref.is_none(),
            "browser service state key vault ref should default to unset unless explicitly configured"
        );
        assert_eq!(config.connect_timeout_ms, 1_500);
        assert_eq!(config.request_timeout_ms, 15_000);
        assert_eq!(config.max_screenshot_bytes, 256 * 1024);
        assert_eq!(config.max_title_bytes, 4 * 1024);
    }

    #[test]
    fn canvas_host_config_defaults_to_disabled_with_bounded_limits() {
        let config = CanvasHostConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.public_base_url, "http://127.0.0.1:7142");
        assert_eq!(config.token_ttl_ms, 15 * 60 * 1_000);
        assert_eq!(config.max_state_bytes, 64 * 1024);
        assert_eq!(config.max_bundle_bytes, 512 * 1024);
        assert_eq!(config.max_assets_per_bundle, 32);
        assert_eq!(config.max_updates_per_minute, 120);
    }

    #[test]
    fn canvas_host_config_parses_overrides() {
        let (parsed, _) = parse_root_file_config(
            r#"
            [canvas_host]
            enabled = true
            public_base_url = "https://console.example.com/palyra"
            token_ttl_ms = 120000
            max_state_bytes = 8192
            max_bundle_bytes = 131072
            max_assets_per_bundle = 8
            max_updates_per_minute = 30
            "#,
        )
        .expect("canvas_host override should parse");
        let canvas_host = parsed.canvas_host.expect("canvas_host section should be present");
        assert_eq!(canvas_host.enabled, Some(true));
        assert_eq!(
            canvas_host.public_base_url,
            Some("https://console.example.com/palyra".to_owned())
        );
        assert_eq!(canvas_host.token_ttl_ms, Some(120_000));
        assert_eq!(canvas_host.max_state_bytes, Some(8_192));
        assert_eq!(canvas_host.max_bundle_bytes, Some(131_072));
        assert_eq!(canvas_host.max_assets_per_bundle, Some(8));
        assert_eq!(canvas_host.max_updates_per_minute, Some(30));
    }

    #[test]
    fn wasm_runtime_config_parses_allow_inline_modules_override() {
        let (parsed, _) = parse_root_file_config(
            r#"
            [tool_call.wasm_runtime]
            allow_inline_modules = true
            "#,
        )
        .expect("wasm runtime override should parse");
        let tool_call = parsed.tool_call.expect("tool_call section should be present");
        let wasm_runtime = tool_call.wasm_runtime.expect("wasm_runtime section should be present");
        assert_eq!(wasm_runtime.allow_inline_modules, Some(true));
    }

    #[test]
    fn process_runner_config_parses_tier_override() {
        let (parsed, _) = parse_root_file_config(
            r#"
            [tool_call.process_runner]
            tier = "c"
            "#,
        )
        .expect("process runner tier override should parse");
        let tool_call = parsed.tool_call.expect("tool_call section should be present");
        let process_runner =
            tool_call.process_runner.expect("process_runner section should be present");
        assert_eq!(process_runner.tier.as_deref(), Some("c"));
    }

    #[test]
    fn tool_call_config_parses_http_fetch_and_browser_service_overrides() {
        let (parsed, _) = parse_root_file_config(
            r#"
            [tool_call.http_fetch]
            request_timeout_ms = 22000
            max_response_bytes = 12345
            allowed_content_types = ["application/json"]
            allowed_request_headers = ["accept"]
            cache_enabled = false

            [tool_call.browser_service]
            enabled = true
            endpoint = "http://127.0.0.1:7543"
            state_dir = "data/browserd-state"
            state_key_vault_ref = "global/browserd_state_key"
            connect_timeout_ms = 2000
            request_timeout_ms = 18000
            max_screenshot_bytes = 131072
            max_title_bytes = 2048
            "#,
        )
        .expect("http fetch + browser service override should parse");
        let tool_call = parsed.tool_call.expect("tool_call section should be present");
        let http_fetch = tool_call.http_fetch.expect("http_fetch section should be present");
        assert_eq!(http_fetch.request_timeout_ms, Some(22_000));
        assert_eq!(http_fetch.max_response_bytes, Some(12_345));
        assert_eq!(http_fetch.allowed_content_types, Some(vec!["application/json".to_owned()]));
        assert_eq!(http_fetch.allowed_request_headers, Some(vec!["accept".to_owned()]));
        assert_eq!(http_fetch.cache_enabled, Some(false));

        let browser_service =
            tool_call.browser_service.expect("browser_service section should be present");
        assert_eq!(browser_service.enabled, Some(true));
        assert_eq!(browser_service.endpoint, Some("http://127.0.0.1:7543".to_owned()));
        assert_eq!(browser_service.state_dir, Some("data/browserd-state".to_owned()));
        assert_eq!(
            browser_service.state_key_vault_ref,
            Some("global/browserd_state_key".to_owned())
        );
        assert_eq!(browser_service.connect_timeout_ms, Some(2_000));
        assert_eq!(browser_service.request_timeout_ms, Some(18_000));
        assert_eq!(browser_service.max_screenshot_bytes, Some(131_072));
        assert_eq!(browser_service.max_title_bytes, Some(2_048));
    }

    #[test]
    fn model_provider_defaults_to_deterministic_with_safe_retry_policy() {
        let config = ModelProviderConfig::default();
        assert_eq!(config.kind, ModelProviderKind::Deterministic);
        assert_eq!(config.openai_base_url, "https://api.openai.com/v1");
        assert!(
            !config.allow_private_base_url,
            "model provider private-network base URLs must require explicit opt-in"
        );
        assert_eq!(config.openai_model, "gpt-4o-mini");
        assert!(
            config.openai_embeddings_model.is_none(),
            "openai embeddings model should default to unset"
        );
        assert!(
            config.openai_embeddings_dims.is_none(),
            "openai embeddings dims should default to unset"
        );
        assert!(config.openai_api_key.is_none(), "openai API key should default to unset");
        assert!(
            config.openai_api_key_vault_ref.is_none(),
            "openai API key vault ref should default to unset"
        );
        assert!(config.auth_profile_id.is_none(), "auth profile id should default to unset");
        assert!(
            config.auth_profile_provider_kind.is_none(),
            "auth provider kind should default to unset"
        );
        assert_eq!(config.max_retries, 2);
    }

    #[test]
    fn admin_config_defaults_to_deny_when_token_missing() {
        let config = AdminConfig::default();
        assert!(config.require_auth, "admin auth should default to required");
        assert!(config.auth_token.is_none(), "admin token should default to missing");
        assert!(
            config.connector_token.is_none(),
            "connector token should default to missing until explicitly configured"
        );
        assert!(
            config.bound_principal.is_none(),
            "admin token principal binding should default to missing until explicitly configured"
        );
    }

    #[test]
    fn storage_config_defaults_to_safe_journal_mode() {
        let config = StorageConfig::default();
        assert_eq!(config.journal_db_path, PathBuf::from("data/journal.sqlite3"));
        assert!(
            config.journal_hash_chain_enabled,
            "hash chain must default to enabled for tamper-evident audit journaling"
        );
        assert_eq!(
            config.max_journal_payload_bytes,
            256 * 1024,
            "journal payload limit should default to 256 KiB"
        );
        assert_eq!(
            config.max_journal_events, 10_000,
            "journal event capacity should default to a bounded fail-closed limit"
        );
        assert!(
            config.vault_dir.ends_with("vault"),
            "default vault directory should be rooted under state/vault"
        );
    }

    #[test]
    fn config_rejects_unknown_top_level_key() {
        let result: Result<RootFileConfig, _> =
            toml::from_str("unexpected=true\n[daemon]\nport=7142\n");
        assert!(result.is_err(), "unknown top-level keys must be rejected");
    }

    #[test]
    fn config_migrates_legacy_documents_without_explicit_version() {
        let (parsed, migration) =
            parse_root_file_config("[daemon]\nport=7142\n").expect("legacy config should parse");
        assert_eq!(parsed.version, Some(1));
        assert!(migration.migrated, "legacy config should trigger migration");
        assert_eq!(migration.source_version, 0);
        assert_eq!(migration.target_version, 1);
    }

    #[test]
    fn config_rejects_unsupported_future_version() {
        let error =
            parse_root_file_config("version=2\n[daemon]\nport=7142\n").expect_err("must fail");
        let rendered = format!("{error:#}");
        assert!(rendered.contains("unsupported config version 2"), "unexpected error: {rendered}");
    }

    #[test]
    fn config_rejects_unknown_daemon_key() {
        let result: Result<RootFileConfig, _> =
            toml::from_str("[daemon]\nport=7142\nunexpected=true\n");
        assert!(result.is_err(), "unknown daemon keys must be rejected");
    }

    #[test]
    fn config_rejects_unknown_deployment_key() {
        let result: Result<RootFileConfig, _> =
            toml::from_str("[deployment]\nmode='local_desktop'\nunexpected=true\n");
        assert!(result.is_err(), "unknown deployment keys must be rejected");
    }

    #[test]
    fn config_rejects_unknown_identity_key() {
        let result: Result<RootFileConfig, _> = toml::from_str(
            "[identity]\nallow_insecure_node_rpc_without_mtls=true\nunexpected=true\n",
        );
        assert!(result.is_err(), "unknown identity keys must be rejected");
    }

    #[test]
    fn config_rejects_unknown_gateway_key() {
        let result: Result<RootFileConfig, _> =
            toml::from_str("[gateway]\ngrpc_port=7443\nunexpected=true\n");
        assert!(result.is_err(), "unknown gateway keys must be rejected");
    }

    #[test]
    fn config_rejects_unknown_orchestrator_key() {
        let result: Result<RootFileConfig, _> =
            toml::from_str("[orchestrator]\nrunloop_v1_enabled=true\nunexpected=true\n");
        assert!(result.is_err(), "unknown orchestrator keys must be rejected");
    }

    #[test]
    fn config_rejects_unknown_cron_key() {
        let result: Result<RootFileConfig, _> =
            toml::from_str("[cron]\ntimezone='utc'\nunexpected=true\n");
        assert!(result.is_err(), "unknown cron keys must be rejected");
    }

    #[test]
    fn config_rejects_unknown_memory_key() {
        let result: Result<RootFileConfig, _> =
            toml::from_str("[memory]\nmax_item_bytes=4096\nunexpected=true\n");
        assert!(result.is_err(), "unknown memory keys must be rejected");
    }

    #[test]
    fn config_rejects_unknown_memory_auto_inject_key() {
        let result: Result<RootFileConfig, _> =
            toml::from_str("[memory.auto_inject]\nenabled=true\nunexpected=true\n");
        assert!(result.is_err(), "unknown memory.auto_inject keys must be rejected");
    }

    #[test]
    fn config_rejects_unknown_memory_retention_key() {
        let result: Result<RootFileConfig, _> =
            toml::from_str("[memory.retention]\nmax_entries=100\nunexpected=true\n");
        assert!(result.is_err(), "unknown memory.retention keys must be rejected");
    }

    #[test]
    fn config_rejects_unknown_model_provider_key() {
        let result: Result<RootFileConfig, _> =
            toml::from_str("[model_provider]\nkind='deterministic'\nunexpected=true\n");
        assert!(result.is_err(), "unknown model_provider keys must be rejected");
    }

    #[test]
    fn config_rejects_unknown_admin_key() {
        let result: Result<RootFileConfig, _> =
            toml::from_str("[admin]\nrequire_auth=true\nunexpected=true\n");
        assert!(result.is_err(), "unknown admin keys must be rejected");
    }

    #[test]
    fn config_rejects_unknown_storage_key() {
        let result: Result<RootFileConfig, _> =
            toml::from_str("[storage]\njournal_db_path='data/journal.sqlite3'\nunexpected=true\n");
        assert!(result.is_err(), "unknown storage keys must be rejected");
    }

    #[test]
    fn config_rejects_unknown_process_runner_key() {
        let result: Result<RootFileConfig, _> =
            toml::from_str("[tool_call.process_runner]\nenabled=true\nunexpected=true\n");
        assert!(result.is_err(), "unknown process runner keys must be rejected");
    }

    #[test]
    fn config_rejects_unknown_wasm_runtime_key() {
        let result: Result<RootFileConfig, _> =
            toml::from_str("[tool_call.wasm_runtime]\nenabled=true\nunexpected=true\n");
        assert!(result.is_err(), "unknown wasm runtime keys must be rejected");
    }

    #[test]
    fn config_rejects_unknown_http_fetch_key() {
        let result: Result<RootFileConfig, _> = toml::from_str(
            "[tool_call.http_fetch]\nallow_private_targets=false\nunexpected=true\n",
        );
        assert!(result.is_err(), "unknown http fetch keys must be rejected");
    }

    #[test]
    fn config_rejects_unknown_browser_service_key() {
        let result: Result<RootFileConfig, _> =
            toml::from_str("[tool_call.browser_service]\nenabled=true\nunexpected=true\n");
        assert!(result.is_err(), "unknown browser service keys must be rejected");
    }

    #[test]
    fn config_rejects_unknown_channel_router_key() {
        let result: Result<RootFileConfig, _> =
            toml::from_str("[channel_router]\nenabled=true\nunexpected=true\n");
        assert!(result.is_err(), "unknown channel_router keys must be rejected");
    }

    #[test]
    fn config_rejects_unknown_channel_router_routing_key() {
        let result: Result<RootFileConfig, _> = toml::from_str(
            "[channel_router.routing]\ndefault_channel_enabled=true\nunexpected=true\n",
        );
        assert!(result.is_err(), "unknown channel_router.routing keys must be rejected");
    }

    #[test]
    fn config_rejects_unknown_channel_router_channel_rule_key() {
        let result: Result<RootFileConfig, _> = toml::from_str(
            "[channel_router.routing]\nchannels = [{ channel = 'slack', enabled = true, unexpected = true }]\n",
        );
        assert!(
            result.is_err(),
            "unknown channel_router.routing.channels[*] keys must be rejected"
        );
    }

    #[test]
    fn config_rejects_unknown_canvas_host_key() {
        let result: Result<RootFileConfig, _> =
            toml::from_str("[canvas_host]\nenabled=true\nunexpected=true\n");
        assert!(result.is_err(), "unknown canvas_host keys must be rejected");
    }

    #[test]
    fn journal_db_path_rejects_parent_traversal() {
        let result = parse_journal_db_path("../secrets/journal.sqlite3");
        assert!(result.is_err(), "journal db path must reject parent traversal");
    }

    #[test]
    fn vault_dir_rejects_empty_and_nul() {
        assert!(parse_vault_dir("").is_err(), "vault dir must reject empty values");
        assert!(parse_vault_dir("vault\0dir").is_err(), "vault dir must reject embedded NUL");
    }

    #[test]
    fn openai_base_url_requires_https_scheme() {
        let result = parse_openai_base_url("file:///tmp/openai");
        assert!(result.is_err(), "openai base URL without https scheme must fail");
    }

    #[test]
    fn openai_base_url_rejects_non_loopback_http_url() {
        let result = parse_openai_base_url("http://example.com/v1");
        assert!(result.is_err(), "openai base URL over non-loopback HTTP must be rejected");
    }

    #[test]
    fn openai_base_url_accepts_loopback_http_url() {
        let parsed = parse_openai_base_url("http://127.0.0.1:8080/v1")
            .expect("loopback HTTP should be allowed for local testing");
        assert_eq!(parsed, "http://127.0.0.1:8080/v1");
    }

    #[test]
    fn openai_base_url_rejects_embedded_credentials() {
        let result = parse_openai_base_url("https://user:pass@example.com/v1");
        assert!(result.is_err(), "openai base URL with embedded credentials must be rejected");
    }

    #[test]
    fn openai_base_url_rejects_query_and_fragment() {
        let result = parse_openai_base_url("https://example.com/v1?api_key=secret#anchor");
        assert!(result.is_err(), "openai base URL with query or fragment must be rejected");
    }

    #[test]
    fn openai_base_url_accepts_clean_https_url() {
        let parsed =
            parse_openai_base_url("https://api.openai.com/v1").expect("base URL should parse");
        assert_eq!(parsed, "https://api.openai.com/v1");
    }

    #[test]
    fn model_provider_config_parses_private_base_url_opt_in_flag() {
        let (parsed, _) = parse_root_file_config(
            r#"
            [model_provider]
            allow_private_base_url = true
            "#,
        )
        .expect("model provider private-base-url opt-in should parse");
        let model_provider = parsed.model_provider.expect("model_provider section should exist");
        assert_eq!(model_provider.allow_private_base_url, Some(true));
    }

    #[test]
    fn model_provider_config_parses_embeddings_fields() {
        let (parsed, _) = parse_root_file_config(
            r#"
            [model_provider]
            kind = "openai_compatible"
            openai_embeddings_model = "text-embedding-3-small"
            openai_embeddings_dims = 1536
            "#,
        )
        .expect("model provider embeddings fields should parse");
        let model_provider = parsed.model_provider.expect("model_provider section should exist");
        assert_eq!(
            model_provider.openai_embeddings_model.as_deref(),
            Some("text-embedding-3-small")
        );
        assert_eq!(model_provider.openai_embeddings_dims, Some(1536));
    }

    #[test]
    fn parse_optional_openai_embeddings_model_trims_and_allows_clear() {
        let parsed = parse_optional_openai_embeddings_model(" text-embedding-3-large ")
            .expect("embeddings model should parse");
        assert_eq!(parsed, Some("text-embedding-3-large".to_owned()));

        let cleared = parse_optional_openai_embeddings_model("   ")
            .expect("blank embeddings model should clear configuration");
        assert!(cleared.is_none());
    }

    #[test]
    fn parse_openai_embeddings_dims_rejects_zero() {
        let error = parse_openai_embeddings_dims(0, "model_provider.openai_embeddings_dims")
            .expect_err("zero embeddings dims must be rejected");
        assert!(
            error.to_string().contains("greater than 0"),
            "error should explain positive dimensions requirement: {error}"
        );
    }

    #[test]
    fn parse_optional_auth_profile_id_normalizes_and_validates_values() {
        let parsed =
            parse_optional_auth_profile_id(" OpenAI.Default_1 ", "model_provider.auth_profile_id")
                .expect("auth profile id should parse");
        assert_eq!(parsed, Some("openai.default_1".to_owned()));

        let cleared = parse_optional_auth_profile_id("   ", "model_provider.auth_profile_id")
            .expect("empty auth profile id should clear value");
        assert!(cleared.is_none());

        let invalid =
            parse_optional_auth_profile_id("bad profile", "model_provider.auth_profile_id");
        assert!(invalid.is_err(), "invalid auth profile id should fail validation");
    }

    #[test]
    fn parse_model_provider_auth_provider_kind_accepts_aliases() {
        assert_eq!(
            parse_model_provider_auth_provider_kind("openai", "model_provider.auth_provider_kind",)
                .expect("openai kind should parse"),
            ModelProviderAuthProviderKind::Openai
        );
        assert_eq!(
            parse_model_provider_auth_provider_kind(
                "openai_compatible",
                "model_provider.auth_provider_kind",
            )
            .expect("openai_compatible kind should parse"),
            ModelProviderAuthProviderKind::Openai
        );
        assert_eq!(
            parse_model_provider_auth_provider_kind(
                "anthropic",
                "model_provider.auth_provider_kind",
            )
            .expect("anthropic kind should parse"),
            ModelProviderAuthProviderKind::Anthropic
        );
    }

    #[test]
    fn parse_model_provider_auth_provider_kind_rejects_unknown_values() {
        let result = parse_model_provider_auth_provider_kind(
            "unsupported_provider",
            "model_provider.auth_provider_kind",
        );
        assert!(result.is_err(), "unsupported auth provider kind should fail validation");
    }

    #[test]
    fn parse_model_provider_registry_entry_inherits_model_provider_defaults() {
        let defaults = ModelProviderConfig {
            request_timeout_ms: 9_000,
            max_retries: 4,
            retry_backoff_ms: 275,
            circuit_breaker_failure_threshold: 5,
            circuit_breaker_cooldown_ms: 45_000,
            ..ModelProviderConfig::default()
        };
        let entry = parse_model_provider_registry_entry(
            FileModelProviderRegistryEntry {
                provider_id: Some("OpenAI.Primary".to_owned()),
                display_name: Some(" OpenAI Primary ".to_owned()),
                kind: Some("openai_compatible".to_owned()),
                base_url: Some("https://api.openai.com/v1".to_owned()),
                allow_private_base_url: None,
                enabled: Some(true),
                auth_profile_id: Some("OpenAI.Default".to_owned()),
                auth_provider_kind: None,
                api_key: None,
                api_key_secret_ref: None,
                api_key_vault_ref: Some("GLOBAL/openai_api_key".to_owned()),
                request_timeout_ms: None,
                max_retries: None,
                retry_backoff_ms: None,
                circuit_breaker_failure_threshold: None,
                circuit_breaker_cooldown_ms: None,
            },
            0,
            &defaults,
        )
        .expect("provider entry should parse");

        assert_eq!(entry.provider_id, "openai.primary");
        assert_eq!(entry.display_name.as_deref(), Some("OpenAI Primary"));
        assert_eq!(entry.auth_profile_id.as_deref(), Some("openai.default"));
        assert_eq!(entry.auth_profile_provider_kind, Some(ModelProviderAuthProviderKind::Openai));
        assert!(entry.api_key_secret_ref.is_some());
        assert_eq!(entry.api_key_vault_ref.as_deref(), Some("global/openai_api_key"));
        assert_eq!(entry.request_timeout_ms, 9_000);
        assert_eq!(entry.max_retries, 4);
        assert_eq!(entry.retry_backoff_ms, 275);
        assert_eq!(entry.circuit_breaker_failure_threshold, 5);
        assert_eq!(entry.circuit_breaker_cooldown_ms, 45_000);
    }

    #[test]
    fn parse_model_provider_registry_model_uses_provider_kind_defaults() {
        let providers = vec![parse_model_provider_registry_entry(
            FileModelProviderRegistryEntry {
                provider_id: Some("anthropic-primary".to_owned()),
                display_name: Some("Anthropic".to_owned()),
                kind: Some("anthropic".to_owned()),
                base_url: Some("https://api.anthropic.com".to_owned()),
                allow_private_base_url: Some(false),
                enabled: Some(true),
                auth_profile_id: None,
                auth_provider_kind: None,
                api_key: None,
                api_key_secret_ref: None,
                api_key_vault_ref: None,
                request_timeout_ms: None,
                max_retries: None,
                retry_backoff_ms: None,
                circuit_breaker_failure_threshold: None,
                circuit_breaker_cooldown_ms: None,
            },
            0,
            &ModelProviderConfig::default(),
        )
        .expect("provider entry should parse")];

        let model = parse_model_provider_registry_model(
            FileModelProviderRegistryModel {
                model_id: Some("claude-3-5-sonnet".to_owned()),
                provider_id: Some("anthropic-primary".to_owned()),
                role: Some("chat".to_owned()),
                enabled: Some(true),
                metadata_source: None,
                operator_override: None,
                tool_calls: None,
                json_mode: None,
                vision: None,
                audio_transcribe: None,
                embeddings: None,
                max_context_tokens: None,
                cost_tier: None,
                latency_tier: None,
                recommended_use_cases: None,
                known_limitations: None,
            },
            0,
            &providers,
        )
        .expect("model entry should parse");

        assert_eq!(model.role, ProviderModelRole::Chat);
        assert_eq!(model.metadata_source, ProviderMetadataSource::Static);
        assert!(model.capabilities.tool_calls);
        assert!(model.capabilities.vision);
        assert!(!model.capabilities.embeddings);
        assert_eq!(model.capabilities.max_context_tokens, Some(200_000));
        assert_eq!(model.capabilities.cost_tier, "premium");
        assert_eq!(model.capabilities.metadata_source, "static");
    }

    #[test]
    fn parse_tool_allowlist_normalizes_and_deduplicates_values() {
        let parsed = parse_tool_allowlist(
            "palyra.echo, palyra.sleep ,palyra.echo,,",
            "PALYRA_TOOL_CALL_ALLOWED_TOOLS",
        )
        .expect("allowlist should parse");
        assert_eq!(parsed, vec!["palyra.echo".to_owned(), "palyra.sleep".to_owned()]);
    }

    #[test]
    fn parse_tool_allowlist_rejects_invalid_characters() {
        let result = parse_tool_allowlist("palyra.echo,../shell", "tool_call.allowed_tools");
        assert!(result.is_err(), "allowlist parser must reject invalid tool names");
    }

    #[test]
    fn parse_vault_ref_allowlist_normalizes_and_deduplicates_values() {
        let parsed = parse_vault_ref_allowlist(
            "GLOBAL/openai_api_key,global/openai_api_key,principal:user/openai_api_key",
            "gateway.vault_get_approval_required_refs",
        )
        .expect("vault ref allowlist should parse");
        assert_eq!(
            parsed,
            vec!["global/openai_api_key".to_owned(), "principal:user/openai_api_key".to_owned(),]
        );
    }

    #[test]
    fn parse_vault_ref_allowlist_rejects_invalid_entries() {
        let result = parse_vault_ref_allowlist(
            "global/not valid",
            "gateway.vault_get_approval_required_refs",
        );
        assert!(result.is_err(), "vault ref allowlist must reject invalid entries");
    }

    #[test]
    fn parse_optional_vault_ref_field_requires_single_entry_when_set() {
        let parsed = parse_optional_vault_ref_field(
            "GLOBAL/browserd_state_key",
            "tool_call.browser_service.state_key_vault_ref",
        )
        .expect("single optional vault ref should parse");
        assert_eq!(parsed, Some("global/browserd_state_key".to_owned()));

        let empty =
            parse_optional_vault_ref_field("   ", "tool_call.browser_service.state_key_vault_ref")
                .expect("empty optional vault ref should clear value");
        assert!(empty.is_none());

        let multiple = parse_optional_vault_ref_field(
            "global/key_a,global/key_b",
            "tool_call.browser_service.state_key_vault_ref",
        );
        assert!(multiple.is_err(), "multiple refs must be rejected for single-value field");
    }

    #[test]
    fn parse_optional_browser_state_dir_accepts_empty_and_rejects_nul() {
        let parsed = parse_optional_browser_state_dir(
            "data/browserd-state",
            "tool_call.browser_service.state_dir",
        )
        .expect("browser state dir should parse");
        assert_eq!(parsed, Some(PathBuf::from("data/browserd-state")));

        let empty = parse_optional_browser_state_dir("   ", "tool_call.browser_service.state_dir")
            .expect("empty browser state dir should clear value");
        assert!(empty.is_none());

        let invalid =
            parse_optional_browser_state_dir("state\0dir", "tool_call.browser_service.state_dir");
        assert!(invalid.is_err(), "embedded NUL must be rejected");
    }

    #[test]
    fn parse_process_executable_allowlist_normalizes_and_deduplicates_values() {
        let parsed = parse_process_executable_allowlist(
            "rustc, cargo ,rustc,,",
            "tool_call.process_runner.allowed_executables",
        )
        .expect("allowlist should parse");
        assert_eq!(parsed, vec!["rustc".to_owned(), "cargo".to_owned()]);
    }

    #[test]
    fn parse_process_runner_tier_accepts_supported_values() {
        assert_eq!(
            parse_process_runner_tier("b", "tool_call.process_runner.tier")
                .expect("tier b should parse"),
            SandboxProcessRunnerTier::B
        );
        assert_eq!(
            parse_process_runner_tier("tier_b", "tool_call.process_runner.tier")
                .expect("tier_b alias should parse"),
            SandboxProcessRunnerTier::B
        );
        assert_eq!(
            parse_process_runner_tier("c", "tool_call.process_runner.tier")
                .expect("tier c should parse"),
            SandboxProcessRunnerTier::C
        );
        assert_eq!(
            parse_process_runner_tier("tier_c", "tool_call.process_runner.tier")
                .expect("tier_c alias should parse"),
            SandboxProcessRunnerTier::C
        );
    }

    #[test]
    fn parse_process_runner_tier_rejects_unknown_values() {
        let result = parse_process_runner_tier("strict", "tool_call.process_runner.tier");
        assert!(result.is_err(), "unsupported process runner tier must fail parsing");
    }

    #[test]
    fn parse_process_runner_egress_enforcement_mode_accepts_supported_values() {
        assert_eq!(
            parse_process_runner_egress_enforcement_mode(
                "none",
                "tool_call.process_runner.egress_enforcement_mode",
            )
            .expect("none mode should parse"),
            EgressEnforcementMode::None
        );
        assert_eq!(
            parse_process_runner_egress_enforcement_mode(
                "preflight",
                "tool_call.process_runner.egress_enforcement_mode",
            )
            .expect("preflight mode should parse"),
            EgressEnforcementMode::Preflight
        );
        assert_eq!(
            parse_process_runner_egress_enforcement_mode(
                "strict",
                "tool_call.process_runner.egress_enforcement_mode",
            )
            .expect("strict mode should parse"),
            EgressEnforcementMode::Strict
        );
    }

    #[test]
    fn parse_process_runner_egress_enforcement_mode_rejects_unknown_values() {
        let result = parse_process_runner_egress_enforcement_mode(
            "best_effort",
            "tool_call.process_runner.egress_enforcement_mode",
        );
        assert!(result.is_err(), "unsupported egress enforcement mode must fail parsing");
    }

    #[test]
    fn parse_cron_timezone_mode_accepts_supported_values() {
        assert_eq!(
            parse_cron_timezone_mode("utc", "cron.timezone").expect("utc should parse"),
            crate::cron::CronTimezoneMode::Utc
        );
        assert_eq!(
            parse_cron_timezone_mode("local", "cron.timezone").expect("local should parse"),
            crate::cron::CronTimezoneMode::Local
        );
    }

    #[test]
    fn parse_cron_timezone_mode_rejects_unknown_values() {
        let result = parse_cron_timezone_mode("Europe/Prague", "cron.timezone");
        assert!(result.is_err(), "unsupported cron timezone mode must be rejected");
    }

    #[test]
    fn parse_host_allowlist_normalizes_and_deduplicates_values() {
        let parsed = parse_host_allowlist(
            "EXAMPLE.COM, api.example.com.,example.com",
            "tool_call.process_runner.allowed_egress_hosts",
        )
        .expect("host allowlist should parse");
        assert_eq!(parsed, vec!["example.com".to_owned(), "api.example.com".to_owned()]);
    }

    #[test]
    fn parse_dns_suffix_allowlist_normalizes_and_deduplicates_values() {
        let parsed = parse_dns_suffix_allowlist(
            "example.com,.corp.local,example.com",
            "tool_call.process_runner.allowed_dns_suffixes",
        )
        .expect("dns suffix allowlist should parse");
        assert_eq!(parsed, vec![".example.com".to_owned(), ".corp.local".to_owned()]);
    }

    #[test]
    fn parse_dns_suffix_allowlist_rejects_invalid_values() {
        let result = parse_dns_suffix_allowlist(
            "..example.com",
            "tool_call.process_runner.allowed_dns_suffixes",
        );
        assert!(result.is_err(), "dns suffix allowlist must reject malformed entries");
    }

    #[test]
    fn parse_content_type_allowlist_normalizes_and_deduplicates_values() {
        let parsed = parse_content_type_allowlist(
            "TEXT/HTML, application/json,text/html",
            "tool_call.http_fetch.allowed_content_types",
        )
        .expect("content-type allowlist should parse");
        assert_eq!(parsed, vec!["text/html".to_owned(), "application/json".to_owned()]);
    }

    #[test]
    fn parse_content_type_allowlist_rejects_parameters_and_whitespace() {
        let result = parse_content_type_allowlist(
            "text/html; charset=utf-8",
            "tool_call.http_fetch.allowed_content_types",
        );
        assert!(result.is_err(), "content-type allowlist must reject parameters");
    }

    #[test]
    fn parse_http_header_allowlist_normalizes_and_deduplicates_values() {
        let parsed = parse_http_header_allowlist(
            "User-Agent,accept,user-agent",
            "tool_call.http_fetch.allowed_request_headers",
        )
        .expect("header allowlist should parse");
        assert_eq!(parsed, vec!["user-agent".to_owned(), "accept".to_owned()]);
    }

    #[test]
    fn parse_http_header_allowlist_rejects_invalid_header_names() {
        let result = parse_http_header_allowlist(
            "x-custom, bad header",
            "tool_call.http_fetch.allowed_request_headers",
        );
        assert!(result.is_err(), "header allowlist must reject invalid header names");
    }

    #[test]
    fn parse_browser_service_endpoint_requires_http_or_https_without_path() {
        assert!(
            parse_browser_service_endpoint(
                "grpc://127.0.0.1:7543",
                "tool_call.browser_service.endpoint",
            )
            .is_err(),
            "unsupported scheme must fail"
        );
        assert!(
            parse_browser_service_endpoint(
                "http://127.0.0.1:7543/browser",
                "tool_call.browser_service.endpoint",
            )
            .is_err(),
            "path segments must fail"
        );
        let parsed = parse_browser_service_endpoint(
            "https://browserd.internal:7443/",
            "tool_call.browser_service.endpoint",
        )
        .expect("valid endpoint should parse");
        assert_eq!(parsed, "https://browserd.internal:7443");
    }

    #[test]
    fn parse_canvas_host_public_base_url_requires_http_or_https_without_query_or_fragment() {
        assert!(
            parse_canvas_host_public_base_url(
                "grpc://127.0.0.1:7142",
                "canvas_host.public_base_url",
            )
            .is_err(),
            "unsupported scheme must fail"
        );
        assert!(
            parse_canvas_host_public_base_url(
                "https://console.example.com/base?debug=true",
                "canvas_host.public_base_url",
            )
            .is_err(),
            "query component must fail"
        );
        let parsed = parse_canvas_host_public_base_url(
            "https://console.example.com/base/",
            "canvas_host.public_base_url",
        )
        .expect("valid base URL should parse");
        assert_eq!(parsed, "https://console.example.com/base");
    }

    #[test]
    fn parse_storage_prefix_allowlist_normalizes_and_deduplicates_values() {
        let parsed = parse_storage_prefix_allowlist(
            "plugins/cache, plugins/cache ,plugins/artifacts",
            "tool_call.wasm_runtime.allowed_storage_prefixes",
        )
        .expect("storage prefix allowlist should parse");
        assert_eq!(parsed, vec!["plugins/cache".to_owned(), "plugins/artifacts".to_owned()]);
    }

    #[test]
    fn parse_storage_prefix_allowlist_rejects_parent_traversal() {
        let result = parse_storage_prefix_allowlist(
            "plugins/../escape",
            "tool_call.wasm_runtime.allowed_storage_prefixes",
        );
        assert!(result.is_err(), "storage prefix allowlist must reject parent traversal");
    }

    #[test]
    fn parse_positive_usize_rejects_zero() {
        let result = parse_positive_usize(0, "gateway.max_tape_entries_per_response");
        assert!(result.is_err(), "zero should not be accepted for positive usize fields");
    }

    #[test]
    fn parse_positive_u32_rejects_zero() {
        let result = parse_positive_u32(0, "memory.retention.ttl_days");
        assert!(result.is_err(), "zero should not be accepted for positive u32 fields");
    }

    #[test]
    fn parse_default_memory_ttl_zero_disables_default_ttl() {
        let parsed =
            parse_default_memory_ttl_ms(0, "memory.default_ttl_ms").expect("ttl should parse");
        assert_eq!(parsed, None);
    }

    #[test]
    fn parse_default_memory_ttl_rejects_negative_values() {
        let result = parse_default_memory_ttl_ms(-1, "memory.default_ttl_ms");
        assert!(result.is_err(), "negative ttl should be rejected");
    }

    #[test]
    fn parse_memory_retention_vacuum_schedule_normalizes_whitespace() {
        let parsed = parse_memory_retention_vacuum_schedule(
            "  0   2   *  *   0  ",
            "memory.retention.vacuum_schedule",
        )
        .expect("schedule should parse");
        assert_eq!(parsed, "0 2 * * 0");
    }

    #[test]
    fn parse_memory_retention_vacuum_schedule_rejects_invalid_field_count() {
        let error =
            parse_memory_retention_vacuum_schedule("* * * *", "memory.retention.vacuum_schedule")
                .expect_err("invalid cron field count should fail");
        assert!(
            error.to_string().contains("5 or 6 fields"),
            "error should explain expected cron field count: {error}"
        );
    }
}
