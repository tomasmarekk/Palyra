export type JsonValue =
  | string
  | number
  | boolean
  | null
  | { [key: string]: JsonValue }
  | JsonValue[];

export interface ChatSessionRecord {
  session_id: string;
  session_key: string;
  session_label?: string;
  principal: string;
  device_id: string;
  channel?: string;
  created_at_unix_ms: number;
  updated_at_unix_ms: number;
  last_run_id?: string;
}

export interface ChatRunStatusRecord {
  run_id: string;
  session_id: string;
  state: string;
  cancel_requested: boolean;
  cancel_reason?: string;
  principal: string;
  device_id: string;
  channel?: string;
  prompt_tokens: number;
  completion_tokens: number;
  total_tokens: number;
  created_at_unix_ms: number;
  started_at_unix_ms: number;
  completed_at_unix_ms?: number;
  updated_at_unix_ms: number;
  last_error?: string;
  tape_events: number;
}

export interface ChatRunTapeRecord {
  seq: number;
  event_type: string;
  payload_json: string;
}

export interface ChatRunTapeSnapshot {
  run_id: string;
  requested_after_seq?: number;
  limit: number;
  max_response_bytes: number;
  returned_bytes: number;
  next_after_seq?: number;
  events: ChatRunTapeRecord[];
}

export interface ChatStreamMetaLine {
  type: "meta";
  run_id: string;
  session_id: string;
}

export interface ChatStreamEventEnvelope {
  run_id: string;
  event_type: string;
  [key: string]: JsonValue;
}

export interface ChatStreamEventLine {
  type: "event";
  event: ChatStreamEventEnvelope;
}

export interface ChatStreamErrorLine {
  type: "error";
  run_id?: string;
  error: string;
}

export interface ChatStreamCompleteLine {
  type: "complete";
  run_id: string;
  status: string;
}

export type ChatStreamLine =
  | ChatStreamMetaLine
  | ChatStreamEventLine
  | ChatStreamErrorLine
  | ChatStreamCompleteLine;

export interface ConsoleSession {
  principal: string;
  device_id: string;
  channel?: string;
  csrf_token: string;
  issued_at_unix_ms: number;
  expires_at_unix_ms: number;
}

export interface ConsoleDiagnosticsSnapshot {
  generated_at_unix_ms: number;
  model_provider: JsonValue;
  rate_limits: JsonValue;
  auth_profiles: JsonValue;
  browserd: JsonValue;
  memory?: JsonValue;
}

export interface ContractDescriptor {
  contract_version: string;
}

export interface PageInfo {
  limit: number;
  returned: number;
  has_more: boolean;
  next_cursor?: string;
}

export type ErrorCategory =
  | "auth"
  | "validation"
  | "policy"
  | "not_found"
  | "conflict"
  | "dependency"
  | "availability"
  | "internal";

export interface ValidationIssue {
  field: string;
  code: string;
  message: string;
}

export interface ErrorEnvelope {
  error?: string;
  code?: string;
  category?: ErrorCategory;
  retryable?: boolean;
  redacted?: boolean;
  validation_errors?: ValidationIssue[];
}

export interface CapabilityEntry {
  id: string;
  domain: string;
  title: string;
  owner: string;
  surfaces: string[];
  execution_mode: string;
  mutation_classes: string[];
  test_refs: string[];
  contract_paths: string[];
  notes?: string;
}

export interface CapabilityMigrationNote {
  id: string;
  message: string;
}

export interface CapabilityCatalog {
  contract: ContractDescriptor;
  version: string;
  generated_at_unix_ms: number;
  capabilities: CapabilityEntry[];
  migration_notes: CapabilityMigrationNote[];
}

export interface DeploymentPostureSummary {
  contract: ContractDescriptor;
  mode: string;
  bind_profile: string;
  bind_addresses: {
    admin: string;
    grpc: string;
    quic: string;
  };
  tls: {
    gateway_enabled: boolean;
  };
  admin_auth_required: boolean;
  dangerous_remote_bind_ack: {
    config: boolean;
    env: boolean;
    env_name: string;
  };
  remote_bind_detected: boolean;
  last_remote_admin_access_attempt?: {
    observed_at_unix_ms: number;
    remote_ip_fingerprint: string;
    method: string;
    path: string;
    status_code: number;
    outcome: string;
  };
  warnings: string[];
}

export interface AuthProfileProvider {
  kind: string;
  custom_name?: string;
}

export interface AuthProfileScope {
  kind: string;
  agent_id?: string;
}

export type AuthCredentialView =
  | {
      type: "api_key";
      api_key_vault_ref: string;
    }
  | {
      type: "oauth";
      access_token_vault_ref: string;
      refresh_token_vault_ref: string;
      token_endpoint: string;
      client_id?: string;
      client_secret_vault_ref?: string;
      scopes: string[];
      expires_at_unix_ms?: number;
      refresh_state: JsonValue;
    };

export interface AuthProfileView {
  profile_id: string;
  provider: AuthProfileProvider;
  profile_name: string;
  scope: AuthProfileScope;
  credential: AuthCredentialView;
  created_at_unix_ms: number;
  updated_at_unix_ms: number;
}

export interface AuthProfileListEnvelope {
  contract: ContractDescriptor;
  profiles: AuthProfileView[];
  page: PageInfo;
}

export interface AuthProfileEnvelope {
  contract: ContractDescriptor;
  profile: AuthProfileView;
}

export interface AuthProfileDeleteEnvelope {
  contract: ContractDescriptor;
  profile_id: string;
  deleted: boolean;
}

export interface AuthHealthEnvelope {
  contract: ContractDescriptor;
  summary: JsonValue;
  expiry_distribution: JsonValue;
  profiles: JsonValue[];
  refresh_metrics: JsonValue;
}

export interface ProviderAuthStateEnvelope {
  contract: ContractDescriptor;
  provider: string;
  oauth_supported: boolean;
  bootstrap_supported: boolean;
  callback_supported: boolean;
  reconnect_supported: boolean;
  revoke_supported: boolean;
  default_selection_supported: boolean;
  default_profile_id?: string;
  available_profile_ids: string[];
  state: string;
  note?: string;
}

export interface ProviderAuthActionEnvelope {
  contract: ContractDescriptor;
  provider: string;
  action: string;
  state: string;
  message: string;
  profile_id?: string;
}

export interface ConfigBackupRecord {
  index: number;
  path: string;
  exists: boolean;
}

export interface ConfigDocumentSnapshot {
  contract: ContractDescriptor;
  source_path: string;
  config_version: number;
  migrated_from_version?: number;
  redacted: boolean;
  document_toml: string;
  backups: ConfigBackupRecord[];
}

export interface ConfigValidationEnvelope {
  contract: ContractDescriptor;
  source_path: string;
  valid: boolean;
  config_version: number;
  migrated_from_version?: number;
}

export interface ConfigMutationEnvelope {
  contract: ContractDescriptor;
  operation: string;
  source_path: string;
  backups_retained: number;
  config_version: number;
  migrated_from_version?: number;
  changed_key?: string;
}

export interface SecretMetadata {
  scope: string;
  key: string;
  created_at_unix_ms: number;
  updated_at_unix_ms: number;
  value_bytes: number;
}

export interface SecretMetadataList {
  contract: ContractDescriptor;
  scope: string;
  secrets: SecretMetadata[];
  page: PageInfo;
}

export interface SecretMetadataEnvelope {
  contract: ContractDescriptor;
  secret: SecretMetadata;
}

export interface SecretRevealEnvelope {
  contract: ContractDescriptor;
  scope: string;
  key: string;
  value_bytes: number;
  value_base64: string;
  value_utf8?: string;
}

export interface PairingCodeRecord {
  code: string;
  channel: string;
  issued_by: string;
  created_at_unix_ms: number;
  expires_at_unix_ms: number;
}

export interface PairingPendingRecord {
  channel: string;
  sender_identity: string;
  code: string;
  requested_at_unix_ms: number;
  expires_at_unix_ms: number;
  approval_id?: string;
}

export interface PairingGrantRecord {
  channel: string;
  sender_identity: string;
  approved_at_unix_ms: number;
  expires_at_unix_ms?: number;
  approval_id?: string;
}

export interface PairingChannelSnapshot {
  channel: string;
  pending: PairingPendingRecord[];
  paired: PairingGrantRecord[];
  active_codes: PairingCodeRecord[];
}

export interface PairingSummaryEnvelope {
  contract: ContractDescriptor;
  channels: PairingChannelSnapshot[];
}

export interface SupportBundleJob {
  job_id: string;
  state: "queued" | "running" | "succeeded" | "failed";
  requested_at_unix_ms: number;
  started_at_unix_ms?: number;
  completed_at_unix_ms?: number;
  output_path?: string;
  command_output: string;
  error?: string;
}

export interface SupportBundleJobEnvelope {
  contract: ContractDescriptor;
  job: SupportBundleJob;
}

export interface SupportBundleJobListEnvelope {
  contract: ContractDescriptor;
  jobs: SupportBundleJob[];
  page: PageInfo;
}

export class ControlPlaneApiError extends Error {
  readonly status: number;
  readonly code?: string;
  readonly category?: ErrorCategory;
  readonly retryable: boolean;
  readonly redacted: boolean;
  readonly validationErrors: ValidationIssue[];

  constructor(
    message: string,
    options: {
      status: number;
      code?: string;
      category?: ErrorCategory;
      retryable?: boolean;
      redacted?: boolean;
      validationErrors?: ValidationIssue[];
      cause?: unknown;
    }
  ) {
    super(message, { cause: options.cause });
    this.name = "ControlPlaneApiError";
    this.status = options.status;
    this.code = options.code;
    this.category = options.category;
    this.retryable = options.retryable ?? false;
    this.redacted = options.redacted ?? false;
    this.validationErrors = options.validationErrors ?? [];
  }
}

interface RequestOptions {
  csrf?: boolean;
  timeoutMs?: number;
}

const DEFAULT_REQUEST_TIMEOUT_MS = 10_000;
const DEFAULT_SAFE_READ_RETRIES = 1;

function buildPathWithQuery(path: string, params?: URLSearchParams): string {
  if (params === undefined || params.size === 0) {
    return path;
  }
  return `${path}?${params.toString()}`;
}

export class ConsoleApiClient {
  private csrfToken: string | null = null;

  constructor(
    private readonly basePath = "",
    private readonly fetcher: typeof fetch = fetch
  ) {}

  async getSession(): Promise<ConsoleSession> {
    const session = await this.request<ConsoleSession>("/console/v1/auth/session");
    this.csrfToken = session.csrf_token;
    return session;
  }

  async login(payload: {
    admin_token: string;
    principal: string;
    device_id: string;
    channel?: string;
  }): Promise<ConsoleSession> {
    const session = await this.request<ConsoleSession>(
      "/console/v1/auth/login",
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: false }
    );
    this.csrfToken = session.csrf_token;
    return session;
  }

  async logout(): Promise<void> {
    await this.request<{ signed_out: boolean }>(
      "/console/v1/auth/logout",
      { method: "POST" },
      { csrf: true }
    );
    this.csrfToken = null;
  }

  async getDiagnostics(): Promise<ConsoleDiagnosticsSnapshot> {
    return this.request("/console/v1/diagnostics");
  }

  async getCapabilityCatalog(): Promise<CapabilityCatalog> {
    return this.request("/console/v1/control-plane/capabilities");
  }

  async getDeploymentPosture(): Promise<DeploymentPostureSummary> {
    return this.request("/console/v1/deployment/posture");
  }

  async listAuthProfiles(params?: URLSearchParams): Promise<AuthProfileListEnvelope> {
    return this.request(buildPathWithQuery("/console/v1/auth/profiles", params));
  }

  async getAuthProfile(profileId: string): Promise<AuthProfileEnvelope> {
    return this.request(`/console/v1/auth/profiles/${encodeURIComponent(profileId)}`);
  }

  async upsertAuthProfile(profile: AuthProfileView): Promise<AuthProfileEnvelope> {
    return this.request(
      "/console/v1/auth/profiles",
      {
        method: "POST",
        body: JSON.stringify(profile)
      },
      { csrf: true }
    );
  }

  async deleteAuthProfile(profileId: string): Promise<AuthProfileDeleteEnvelope> {
    return this.request(
      `/console/v1/auth/profiles/${encodeURIComponent(profileId)}/delete`,
      {
        method: "POST",
        body: JSON.stringify({})
      },
      { csrf: true }
    );
  }

  async getAuthHealth(params?: URLSearchParams): Promise<AuthHealthEnvelope> {
    return this.request(buildPathWithQuery("/console/v1/auth/health", params));
  }

  async getOpenAiProviderState(): Promise<ProviderAuthStateEnvelope> {
    return this.request("/console/v1/auth/providers/openai");
  }

  async startOpenAiProviderBootstrap(payload: { profile_id?: string } = {}): Promise<ProviderAuthActionEnvelope> {
    return this.request(
      "/console/v1/auth/providers/openai/bootstrap",
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: true }
    );
  }

  async getOpenAiProviderCallbackState(): Promise<ProviderAuthStateEnvelope> {
    return this.request("/console/v1/auth/providers/openai/callback-state");
  }

  async reconnectOpenAiProvider(payload: { profile_id?: string } = {}): Promise<ProviderAuthActionEnvelope> {
    return this.request(
      "/console/v1/auth/providers/openai/reconnect",
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: true }
    );
  }

  async revokeOpenAiProvider(payload: { profile_id?: string } = {}): Promise<ProviderAuthActionEnvelope> {
    return this.request(
      "/console/v1/auth/providers/openai/revoke",
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: true }
    );
  }

  async setOpenAiDefaultProfile(payload: { profile_id?: string } = {}): Promise<ProviderAuthActionEnvelope> {
    return this.request(
      "/console/v1/auth/providers/openai/default-profile",
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: true }
    );
  }

  async inspectConfig(payload: {
    path?: string;
    show_secrets?: boolean;
    backups?: number;
  }): Promise<ConfigDocumentSnapshot> {
    return this.request(
      "/console/v1/config/inspect",
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: false }
    );
  }

  async validateConfig(payload: { path?: string }): Promise<ConfigValidationEnvelope> {
    return this.request(
      "/console/v1/config/validate",
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: false }
    );
  }

  async mutateConfig(payload: {
    path?: string;
    key: string;
    value?: string;
    backups?: number;
  }): Promise<ConfigMutationEnvelope> {
    return this.request(
      "/console/v1/config/mutate",
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: true }
    );
  }

  async migrateConfig(payload: {
    path?: string;
    show_secrets?: boolean;
    backups?: number;
  }): Promise<ConfigMutationEnvelope> {
    return this.request(
      "/console/v1/config/migrate",
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: true }
    );
  }

  async recoverConfig(payload: {
    path?: string;
    backup?: number;
    backups?: number;
  }): Promise<ConfigMutationEnvelope> {
    return this.request(
      "/console/v1/config/recover",
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: true }
    );
  }

  async listSecrets(scope: string): Promise<SecretMetadataList> {
    return this.request(`/console/v1/secrets?scope=${encodeURIComponent(scope)}`);
  }

  async getSecretMetadata(scope: string, key: string): Promise<SecretMetadataEnvelope> {
    return this.request(
      `/console/v1/secrets/metadata?scope=${encodeURIComponent(scope)}&key=${encodeURIComponent(key)}`
    );
  }

  async setSecret(payload: {
    scope: string;
    key: string;
    value_base64: string;
  }): Promise<SecretMetadataEnvelope> {
    return this.request(
      "/console/v1/secrets",
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: true }
    );
  }

  async revealSecret(payload: {
    scope: string;
    key: string;
    reveal: true;
  }): Promise<SecretRevealEnvelope> {
    return this.request(
      "/console/v1/secrets/reveal",
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: true }
    );
  }

  async deleteSecret(payload: { scope: string; key: string }): Promise<SecretMetadataEnvelope> {
    return this.request(
      "/console/v1/secrets/delete",
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: true }
    );
  }

  async getPairingSummary(): Promise<PairingSummaryEnvelope> {
    return this.request("/console/v1/pairing");
  }

  async mintPairingCode(payload: {
    channel: string;
    issued_by?: string;
    ttl_ms?: number;
  }): Promise<PairingSummaryEnvelope> {
    return this.request(
      "/console/v1/pairing/codes",
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: true }
    );
  }

  async listSupportBundleJobs(params?: URLSearchParams): Promise<SupportBundleJobListEnvelope> {
    return this.request(buildPathWithQuery("/console/v1/support-bundle/jobs", params));
  }

  async createSupportBundleJob(payload: { retain_jobs?: number } = {}): Promise<SupportBundleJobEnvelope> {
    return this.request(
      "/console/v1/support-bundle/jobs",
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: true }
    );
  }

  async getSupportBundleJob(jobId: string): Promise<SupportBundleJobEnvelope> {
    return this.request(`/console/v1/support-bundle/jobs/${encodeURIComponent(jobId)}`);
  }

  async listApprovals(params?: URLSearchParams): Promise<{ approvals: JsonValue[] }> {
    return this.request(buildPathWithQuery("/console/v1/approvals", params));
  }

  async listChatSessions(params?: URLSearchParams): Promise<{
    sessions: ChatSessionRecord[];
    next_after_session_key?: string;
  }> {
    return this.request(buildPathWithQuery("/console/v1/chat/sessions", params));
  }

  async resolveChatSession(payload: {
    session_id?: string;
    session_key?: string;
    session_label?: string;
    require_existing?: boolean;
    reset_session?: boolean;
  }): Promise<{ session: ChatSessionRecord; created: boolean; reset_applied: boolean }> {
    return this.request(
      "/console/v1/chat/sessions",
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: true }
    );
  }

  async renameChatSession(
    sessionId: string,
    payload: { session_label: string }
  ): Promise<{ session: ChatSessionRecord; created: boolean; reset_applied: boolean }> {
    return this.request(
      `/console/v1/chat/sessions/${encodeURIComponent(sessionId)}/rename`,
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: true }
    );
  }

  async resetChatSession(
    sessionId: string
  ): Promise<{ session: ChatSessionRecord; created: boolean; reset_applied: boolean }> {
    return this.request(
      `/console/v1/chat/sessions/${encodeURIComponent(sessionId)}/reset`,
      { method: "POST" },
      { csrf: true }
    );
  }

  async chatRunStatus(runId: string): Promise<{ run: ChatRunStatusRecord }> {
    return this.request(`/console/v1/chat/runs/${encodeURIComponent(runId)}/status`);
  }

  async chatRunEvents(
    runId: string,
    params?: URLSearchParams
  ): Promise<{ run: ChatRunStatusRecord; tape: ChatRunTapeSnapshot }> {
    return this.request(
      buildPathWithQuery(`/console/v1/chat/runs/${encodeURIComponent(runId)}/events`, params)
    );
  }

  async streamChatMessage(
    sessionId: string,
    payload: {
      text: string;
      allow_sensitive_tools?: boolean;
      session_label?: string;
    },
    options: {
      signal?: AbortSignal;
      onLine: (line: ChatStreamLine) => void;
    }
  ): Promise<void> {
    const path = `/console/v1/chat/sessions/${encodeURIComponent(sessionId)}/messages/stream`;
    const headers = new Headers();
    headers.set("Content-Type", "application/json");
    if (this.csrfToken === null) {
      throw new Error("Missing CSRF token. Please sign in again.");
    }
    headers.set("x-palyra-csrf-token", this.csrfToken);
    const response = await this.fetcher(`${this.basePath}${path}`, {
      method: "POST",
      headers,
      body: JSON.stringify(payload),
      credentials: "include",
      signal: options.signal
    });
    if (!response.ok) {
      throw await buildRequestError(response);
    }
    if (response.body === null) {
      throw new Error("Chat stream response body is missing.");
    }

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffered = "";
    while (true) {
      const chunk = await reader.read();
      if (chunk.done) {
        break;
      }
      buffered += decoder.decode(chunk.value, { stream: true });
      buffered = flushNdjsonBuffer(buffered, options.onLine);
    }
    buffered += decoder.decode();
    flushNdjsonBuffer(buffered, options.onLine, true);
  }

  async getApproval(approvalId: string): Promise<{ approval: JsonValue }> {
    return this.request(`/console/v1/approvals/${encodeURIComponent(approvalId)}`);
  }

  async decideApproval(
    approvalId: string,
    payload: {
      approved: boolean;
      reason?: string;
      decision_scope?: "once" | "session" | "timeboxed";
      decision_scope_ttl_ms?: number;
    }
  ): Promise<{ approval: JsonValue }> {
    return this.request(
      `/console/v1/approvals/${encodeURIComponent(approvalId)}/decision`,
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: true }
    );
  }

  async listCronJobs(params?: URLSearchParams): Promise<{ jobs: JsonValue[] }> {
    return this.request(buildPathWithQuery("/console/v1/cron/jobs", params));
  }

  async createCronJob(payload: {
    name: string;
    prompt: string;
    schedule_type: "cron" | "every" | "at";
    cron_expression?: string;
    every_interval_ms?: number;
    at_timestamp_rfc3339?: string;
    enabled?: boolean;
    channel?: string;
  }): Promise<{ job: JsonValue }> {
    return this.request(
      "/console/v1/cron/jobs",
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: true }
    );
  }

  async setCronJobEnabled(jobId: string, enabled: boolean): Promise<{ job: JsonValue }> {
    return this.request(
      `/console/v1/cron/jobs/${encodeURIComponent(jobId)}/enabled`,
      {
        method: "POST",
        body: JSON.stringify({ enabled })
      },
      { csrf: true }
    );
  }

  async runCronJobNow(jobId: string): Promise<{ run_id?: string; status: string; message: string }> {
    return this.request(
      `/console/v1/cron/jobs/${encodeURIComponent(jobId)}/run-now`,
      { method: "POST" },
      { csrf: true }
    );
  }

  async listCronRuns(jobId: string, params?: URLSearchParams): Promise<{ runs: JsonValue[] }> {
    return this.request(buildPathWithQuery(`/console/v1/cron/jobs/${encodeURIComponent(jobId)}/runs`, params));
  }

  async listChannels(): Promise<{ connectors: JsonValue[] }> {
    return this.request("/console/v1/channels");
  }

  async getChannelStatus(connectorId: string): Promise<{ connector: JsonValue }> {
    return this.request(`/console/v1/channels/${encodeURIComponent(connectorId)}`);
  }

  async setChannelEnabled(
    connectorId: string,
    enabled: boolean
  ): Promise<{ connector: JsonValue }> {
    return this.request(
      `/console/v1/channels/${encodeURIComponent(connectorId)}/enabled`,
      {
        method: "POST",
        body: JSON.stringify({ enabled })
      },
      { csrf: true }
    );
  }

  async listChannelLogs(
    connectorId: string,
    params?: URLSearchParams
  ): Promise<{ events: JsonValue[]; dead_letters: JsonValue[] }> {
    return this.request(
      buildPathWithQuery(`/console/v1/channels/${encodeURIComponent(connectorId)}/logs`, params)
    );
  }

  async sendChannelTestMessage(
    connectorId: string,
    payload: {
      text: string;
      conversation_id?: string;
      sender_id?: string;
      sender_display?: string;
      simulate_crash_once?: boolean;
      is_direct_message?: boolean;
      requested_broadcast?: boolean;
    }
  ): Promise<{ ingest: JsonValue; status: JsonValue }> {
    return this.request(
      `/console/v1/channels/${encodeURIComponent(connectorId)}/test`,
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: true }
    );
  }

  async sendChannelDiscordTestSend(
    connectorId: string,
    payload: {
      target: string;
      text?: string;
      confirm: boolean;
      auto_reaction?: string;
      thread_id?: string;
    }
  ): Promise<{ dispatch: JsonValue; status: JsonValue; runtime?: JsonValue }> {
    return this.request(
      `/console/v1/channels/${encodeURIComponent(connectorId)}/test-send`,
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: true }
    );
  }

  async getChannelRouterRules(): Promise<{ config: JsonValue; config_hash: string }> {
    return this.request("/console/v1/channels/router/rules");
  }

  async getChannelRouterWarnings(): Promise<{ warnings: JsonValue[]; config_hash: string }> {
    return this.request("/console/v1/channels/router/warnings");
  }

  async previewChannelRoute(
    payload: {
      channel: string;
      text: string;
      conversation_id?: string;
      sender_identity?: string;
      sender_display?: string;
      sender_verified?: boolean;
      is_direct_message?: boolean;
      requested_broadcast?: boolean;
      adapter_message_id?: string;
      adapter_thread_id?: string;
      max_payload_bytes?: number;
    }
  ): Promise<{ preview: JsonValue }> {
    return this.request(
      "/console/v1/channels/router/preview",
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: true }
    );
  }

  async listChannelRouterPairings(
    params?: URLSearchParams
  ): Promise<{ pairings: JsonValue[]; config_hash: string }> {
    return this.request(buildPathWithQuery("/console/v1/channels/router/pairings", params));
  }

  async mintChannelRouterPairingCode(
    payload: { channel: string; issued_by?: string; ttl_ms?: number }
  ): Promise<{ code: JsonValue; config_hash: string }> {
    return this.request(
      "/console/v1/channels/router/pairing-codes",
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: true }
    );
  }

  async probeDiscordOnboarding(
    payload: {
      account_id?: string;
      token: string;
      mode?: "local" | "remote_vps";
      inbound_scope?: "dm_only" | "allowlisted_guild_channels" | "open_guild_channels";
      allow_from?: string[];
      deny_from?: string[];
      require_mention?: boolean;
      mention_patterns?: string[];
      concurrency_limit?: number;
      broadcast_strategy?: "deny" | "mention_only" | "allow";
      confirm_open_guild_channels?: boolean;
      verify_channel_id?: string;
    }
  ): Promise<{ [key: string]: JsonValue }> {
    return this.request(
      "/console/v1/channels/discord/onboarding/probe",
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: true }
    );
  }

  async applyDiscordOnboarding(
    payload: {
      account_id?: string;
      token: string;
      mode?: "local" | "remote_vps";
      inbound_scope?: "dm_only" | "allowlisted_guild_channels" | "open_guild_channels";
      allow_from?: string[];
      deny_from?: string[];
      require_mention?: boolean;
      mention_patterns?: string[];
      concurrency_limit?: number;
      broadcast_strategy?: "deny" | "mention_only" | "allow";
      confirm_open_guild_channels?: boolean;
      verify_channel_id?: string;
    }
  ): Promise<{ [key: string]: JsonValue }> {
    return this.request(
      "/console/v1/channels/discord/onboarding/apply",
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: true }
    );
  }

  async searchMemory(params?: URLSearchParams): Promise<{ hits: JsonValue[] }> {
    return this.request(buildPathWithQuery("/console/v1/memory/search", params));
  }

  async getMemoryStatus(): Promise<{
    usage: JsonValue;
    retention: JsonValue;
    maintenance: JsonValue;
  }> {
    return this.request("/console/v1/memory/status");
  }

  async purgeMemory(payload: {
    channel?: string;
    session_id?: string;
    purge_all_principal?: boolean;
  }): Promise<{ deleted_count: number }> {
    return this.request(
      "/console/v1/memory/purge",
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: true }
    );
  }

  async listSkills(params?: URLSearchParams): Promise<{ entries: JsonValue[] }> {
    return this.request(buildPathWithQuery("/console/v1/skills", params));
  }

  async installSkill(payload: {
    artifact_path: string;
    allow_tofu?: boolean;
    allow_untrusted?: boolean;
  }): Promise<{ record: JsonValue }> {
    return this.request(
      "/console/v1/skills/install",
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: true }
    );
  }

  async verifySkill(
    skillId: string,
    payload: { version?: string; allow_tofu?: boolean }
  ): Promise<{ report: JsonValue }> {
    return this.request(
      `/console/v1/skills/${encodeURIComponent(skillId)}/verify`,
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: true }
    );
  }

  async auditSkill(
    skillId: string,
    payload: { version?: string; allow_tofu?: boolean; quarantine_on_fail?: boolean }
  ): Promise<{ report: JsonValue; quarantined: boolean }> {
    return this.request(
      `/console/v1/skills/${encodeURIComponent(skillId)}/audit`,
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: true }
    );
  }

  async quarantineSkill(payload: {
    skill_id: string;
    version: string;
    reason?: string;
  }): Promise<JsonValue> {
    return this.request(
      `/console/v1/skills/${encodeURIComponent(payload.skill_id)}/quarantine`,
      {
        method: "POST",
        body: JSON.stringify({
          version: payload.version,
          reason: payload.reason
        })
      },
      { csrf: true }
    );
  }

  async enableSkill(payload: {
    skill_id: string;
    version: string;
    reason?: string;
  }): Promise<JsonValue> {
    return this.request(
      `/console/v1/skills/${encodeURIComponent(payload.skill_id)}/enable`,
      {
        method: "POST",
        body: JSON.stringify({
          version: payload.version,
          reason: payload.reason,
          override: true
        })
      },
      { csrf: true }
    );
  }

  async listAuditEvents(params?: URLSearchParams): Promise<{ events: JsonValue[] }> {
    return this.request(buildPathWithQuery("/console/v1/audit/events", params));
  }

  async listBrowserProfiles(params?: URLSearchParams): Promise<{
    principal: string;
    active_profile_id?: string;
    profiles: JsonValue[];
  }> {
    return this.request(buildPathWithQuery("/console/v1/browser/profiles", params));
  }

  async createBrowserProfile(payload: {
    principal?: string;
    name: string;
    theme_color?: string;
    persistence_enabled?: boolean;
    private_profile?: boolean;
  }): Promise<{ profile: JsonValue }> {
    return this.request(
      "/console/v1/browser/profiles/create",
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: true }
    );
  }

  async renameBrowserProfile(
    profileId: string,
    payload: { principal?: string; name: string }
  ): Promise<{ profile: JsonValue }> {
    return this.request(
      `/console/v1/browser/profiles/${encodeURIComponent(profileId)}/rename`,
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: true }
    );
  }

  async deleteBrowserProfile(
    profileId: string,
    payload: { principal?: string } = {}
  ): Promise<{ deleted: boolean; active_profile_id?: string }> {
    return this.request(
      `/console/v1/browser/profiles/${encodeURIComponent(profileId)}/delete`,
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: true }
    );
  }

  async activateBrowserProfile(
    profileId: string,
    payload: { principal?: string } = {}
  ): Promise<{ profile: JsonValue }> {
    return this.request(
      `/console/v1/browser/profiles/${encodeURIComponent(profileId)}/activate`,
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: true }
    );
  }

  async listBrowserDownloads(params?: URLSearchParams): Promise<{
    artifacts: JsonValue[];
    truncated: boolean;
    error: string;
  }> {
    return this.request(buildPathWithQuery("/console/v1/browser/downloads", params));
  }

  async mintBrowserRelayToken(payload: {
    session_id: string;
    extension_id: string;
    ttl_ms?: number;
  }): Promise<{
    relay_token: string;
    session_id: string;
    extension_id: string;
    issued_at_unix_ms: number;
    expires_at_unix_ms: number;
    token_ttl_ms: number;
    warning: string;
  }> {
    return this.request(
      "/console/v1/browser/relay/tokens",
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      { csrf: true }
    );
  }

  async relayBrowserAction(
    payload: {
      session_id: string;
      extension_id: string;
      action: "open_tab" | "capture_selection" | "send_page_snapshot";
      open_tab?: { url: string; activate?: boolean; timeout_ms?: number };
      capture_selection?: { selector: string; max_selection_bytes?: number };
      page_snapshot?: {
        include_dom_snapshot?: boolean;
        include_visible_text?: boolean;
        max_dom_snapshot_bytes?: number;
        max_visible_text_bytes?: number;
      };
      max_payload_bytes?: number;
    },
    relayToken?: string
  ): Promise<{
    success: boolean;
    action: string;
    error: string;
    result: JsonValue;
  }> {
    const headers = relayToken !== undefined && relayToken.trim().length > 0
      ? { authorization: `Bearer ${relayToken.trim()}` }
      : undefined;
    return this.request(
      "/console/v1/browser/relay/actions",
      {
        method: "POST",
        headers,
        body: JSON.stringify(payload)
      },
      { csrf: false }
    );
  }

  private async request<T>(
    path: string,
    init: RequestInit = {},
    options: RequestOptions = {}
  ): Promise<T> {
    const headers = new Headers(init.headers);
    if (init.body !== undefined && !headers.has("Content-Type")) {
      headers.set("Content-Type", "application/json");
    }

    const method = (init.method ?? "GET").toUpperCase();
    const requiresCsrf = options.csrf ?? (method !== "GET");
    if (requiresCsrf) {
      if (this.csrfToken === null) {
        throw new Error("Missing CSRF token. Please sign in again.");
      }
      headers.set("x-palyra-csrf-token", this.csrfToken);
    }

    const timeoutMs = normalizeRequestTimeoutMs(options.timeoutMs);
    const maxAttempts = method === "GET" ? DEFAULT_SAFE_READ_RETRIES + 1 : 1;
    let attempt = 0;

    while (true) {
      attempt += 1;
      const requestController = new AbortController();
      let timedOut = false;
      const releaseCallerSignal = forwardAbortSignal(init.signal, requestController);
      const timeoutHandle = setTimeout(() => {
        timedOut = true;
        requestController.abort();
      }, timeoutMs);

      try {
        const response = await this.fetcher(`${this.basePath}${path}`, {
          ...init,
          headers,
          credentials: "include",
          signal: requestController.signal
        });
        return (await parseJsonResponse<T>(response)) as T;
      } catch (error) {
        if (isAbortError(error)) {
          if (timedOut) {
            throw new Error(`Request timed out after ${timeoutMs} ms.`, { cause: error });
          }
          if (init.signal?.aborted === true) {
            throw new Error("Request canceled.", { cause: error });
          }
        }
        if (!shouldRetrySafeRead(method, attempt, maxAttempts, error)) {
          throw error;
        }
      } finally {
        clearTimeout(timeoutHandle);
        releaseCallerSignal();
      }
    }
  }
}

function normalizeRequestTimeoutMs(timeoutMs: number | undefined): number {
  if (timeoutMs === undefined || !Number.isFinite(timeoutMs) || timeoutMs <= 0) {
    return DEFAULT_REQUEST_TIMEOUT_MS;
  }
  return Math.floor(timeoutMs);
}

function forwardAbortSignal(signal: AbortSignal | null | undefined, controller: AbortController): () => void {
  if (signal === undefined || signal === null) {
    return () => {};
  }
  if (signal.aborted) {
    controller.abort();
    return () => {};
  }
  const onAbort = () => {
    controller.abort();
  };
  signal.addEventListener("abort", onAbort, { once: true });
  return () => {
    signal.removeEventListener("abort", onAbort);
  };
}

function isAbortError(error: unknown): boolean {
  if (error instanceof Error && error.name === "AbortError") {
    return true;
  }
  if (typeof DOMException !== "undefined" && error instanceof DOMException) {
    return error.name === "AbortError";
  }
  return false;
}

function shouldRetrySafeRead(
  method: string,
  attempt: number,
  maxAttempts: number,
  error: unknown
): boolean {
  if (method !== "GET" || attempt >= maxAttempts) {
    return false;
  }
  if (error instanceof ControlPlaneApiError) {
    return error.retryable;
  }
  return !isAbortError(error);
}

function parseErrorEnvelope(payload: JsonValue): ErrorEnvelope | null {
  if (payload !== null && typeof payload === "object" && !Array.isArray(payload)) {
    const envelope = payload as ErrorEnvelope;
    if (typeof envelope.error === "string" && envelope.error.trim().length > 0) {
      return envelope;
    }
  }
  return null;
}

function buildControlPlaneApiError(payload: JsonValue, status: number): ControlPlaneApiError {
  const envelope = parseErrorEnvelope(payload);
  return new ControlPlaneApiError(
    envelope?.error?.trim().length ? envelope.error : `Request failed with HTTP ${status}.`,
    {
      status,
      code: envelope?.code,
      category: envelope?.category,
      retryable: envelope?.retryable,
      redacted: envelope?.redacted,
      validationErrors: envelope?.validation_errors
    }
  );
}

async function buildRequestError(response: Response): Promise<ControlPlaneApiError> {
  const contentType = response.headers.get("content-type") ?? "";
  const payload = contentType.includes("application/json")
    ? ((await response.json()) as JsonValue)
    : ((await response.text()) as unknown as JsonValue);
  return buildControlPlaneApiError(payload, response.status);
}

async function parseJsonResponse<T>(response: Response): Promise<T> {
  const contentType = response.headers.get("content-type") ?? "";
  const isJson = contentType.includes("application/json");
  const payload = isJson
    ? ((await response.json()) as JsonValue)
    : ((await response.text()) as unknown as JsonValue);

  if (!response.ok) {
    throw buildControlPlaneApiError(payload, response.status);
  }
  return payload as T;
}

function flushNdjsonBuffer(
  buffer: string,
  onLine: (line: ChatStreamLine) => void,
  flushRemainder = false
): string {
  let remainder = buffer;
  while (true) {
    const newline = remainder.indexOf("\n");
    if (newline === -1) {
      break;
    }
    const line = remainder.slice(0, newline).trim();
    remainder = remainder.slice(newline + 1);
    if (line.length > 0) {
      onLine(parseChatStreamLine(line));
    }
  }
  if (flushRemainder) {
    const tail = remainder.trim();
    if (tail.length > 0) {
      onLine(parseChatStreamLine(tail));
    }
    return "";
  }
  return remainder;
}

function parseChatStreamLine(line: string): ChatStreamLine {
  let parsed: unknown;
  try {
    parsed = JSON.parse(line);
  } catch {
    throw new Error("Chat stream emitted malformed JSON line.");
  }
  if (!isRecord(parsed) || typeof parsed.type !== "string") {
    throw new Error("Chat stream emitted an invalid line envelope.");
  }
  if (parsed.type === "meta") {
    if (typeof parsed.run_id !== "string" || typeof parsed.session_id !== "string") {
      throw new Error("Chat stream meta line is missing run_id/session_id.");
    }
    return {
      type: "meta",
      run_id: parsed.run_id,
      session_id: parsed.session_id
    };
  }
  if (parsed.type === "event") {
    if (!isRecord(parsed.event)) {
      throw new Error("Chat stream event line is missing event payload.");
    }
    const eventType = parsed.event.event_type;
    const runId = parsed.event.run_id;
    if (typeof eventType !== "string" || typeof runId !== "string") {
      throw new Error("Chat stream event payload is missing run_id/event_type.");
    }
    return {
      type: "event",
      event: parsed.event as ChatStreamEventEnvelope
    };
  }
  if (parsed.type === "error") {
    if (typeof parsed.error !== "string") {
      throw new Error("Chat stream error line is missing error text.");
    }
    return {
      type: "error",
      run_id: typeof parsed.run_id === "string" ? parsed.run_id : undefined,
      error: parsed.error
    };
  }
  if (parsed.type === "complete") {
    if (typeof parsed.run_id !== "string" || typeof parsed.status !== "string") {
      throw new Error("Chat stream complete line is missing run_id/status.");
    }
    return {
      type: "complete",
      run_id: parsed.run_id,
      status: parsed.status
    };
  }
  throw new Error(`Unsupported chat stream line type '${parsed.type}'.`);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
