import type { JsonValue } from "../consoleApi";
import {
  readBool,
  readNumber,
  readObject,
  readString,
  readStringList,
  toJsonObjectArray,
  type JsonObject,
} from "./shared";

export type ProviderRegistryProviderView = {
  providerId: string;
  credentialId?: string;
  displayName: string;
  kind: string;
  enabled: boolean;
  endpointBaseUrl?: string;
  authProfileId?: string;
  credentialSource?: string;
  healthState: string;
  healthMessage?: string;
  discoveryStatus: string;
  discoveredModelIds: string[];
  errorRateBps: number;
  avgLatencyMs: number;
  lastUsedAtUnixMs?: number;
  lastSuccessAtUnixMs?: number;
  lastErrorAtUnixMs?: number;
  lastError?: ProviderFailureView;
  circuitOpen: boolean;
};

export type ProviderFailureView = {
  class: string;
  recommendedAction?: string;
  statusCode?: number;
  providerDetail?: string;
  message?: string;
};

export type ProviderRegistryCredentialView = {
  credentialId: string;
  providerId: string;
  providerKind: string;
  authProfileId?: string;
  authProfileProviderKind?: string;
  credentialSource?: string;
  availabilityState: string;
  capabilitySummary: string[];
  healthState: string;
  healthMessage?: string;
  errorRateBps: number;
  avgLatencyMs: number;
  lastUsedAtUnixMs?: number;
  lastSuccessAtUnixMs?: number;
  lastErrorAtUnixMs?: number;
  lastError?: ProviderFailureView;
};

export type ProviderRegistryModelView = {
  modelId: string;
  providerId: string;
  role: string;
  enabled: boolean;
  toolCalls: boolean;
  jsonMode: boolean;
  vision: boolean;
  audioTranscribe: boolean;
  embeddings: boolean;
  maxContextTokens?: number;
  costTier?: string;
  latencyTier?: string;
  recommendedUseCases: string[];
  knownLimitations: string[];
};

export type ProviderRegistrySummaryView = {
  providerState: string;
  providerKind: string;
  providerId?: string;
  modelId?: string;
  credentialId?: string;
  credentialSource?: string;
  apiKeyConfigured: boolean;
  defaultChatModelId?: string;
  defaultEmbeddingsModelId?: string;
  failoverEnabled: boolean;
  responseCacheEnabled: boolean;
  providers: ProviderRegistryProviderView[];
  credentials: ProviderRegistryCredentialView[];
  models: ProviderRegistryModelView[];
};

export type RoutingExplanationView = {
  explanation: string[];
  budgetOutcome?: string;
  reasonCodes: string[];
  taskClass?: string;
  routingAction?: string;
  credentialId?: string;
  deferred?: boolean;
  lease?: {
    state?: string;
    priority?: string;
    estimatedWaitMs?: number;
    reason?: string;
  };
};

export function readProviderRegistrySummary(
  snapshot: JsonObject | null | undefined,
): ProviderRegistrySummaryView | null {
  const root = snapshot ?? null;
  if (root === null) {
    return null;
  }
  const modelProvider = readObject(root, "model_provider");
  if (modelProvider === null) {
    return null;
  }
  const registry = readObject(modelProvider, "registry");
  const providers = toJsonObjectArray(
    Array.isArray(registry?.providers) ? registry.providers : [],
  ).map(readProviderRegistryProvider);
  const credentials = toJsonObjectArray(
    Array.isArray(registry?.credentials) ? registry.credentials : [],
  ).map(readProviderRegistryCredential);
  const models = toJsonObjectArray(Array.isArray(registry?.models) ? registry.models : []).map(
    readProviderRegistryModel,
  );
  return {
    providerState:
      readString(readObject(modelProvider, "health") ?? {}, "state") ??
      readString(readObject(modelProvider, "health") ?? {}, "status") ??
      readString(modelProvider, "kind") ??
      "unknown",
    providerKind: readString(modelProvider, "kind") ?? "unknown",
    providerId: readString(modelProvider, "provider_id") ?? undefined,
    modelId: readString(modelProvider, "model_id") ?? undefined,
    credentialId: readString(modelProvider, "credential_id") ?? undefined,
    credentialSource: readString(modelProvider, "credential_source") ?? undefined,
    apiKeyConfigured: readBool(modelProvider, "api_key_configured"),
    defaultChatModelId: readString(registry ?? {}, "default_chat_model_id") ?? undefined,
    defaultEmbeddingsModelId:
      readString(registry ?? {}, "default_embeddings_model_id") ?? undefined,
    failoverEnabled: readBool(registry ?? {}, "failover_enabled"),
    responseCacheEnabled: readBool(registry ?? {}, "response_cache_enabled"),
    providers,
    credentials,
    models,
  };
}

export function parseRoutingExplanation(value: string | undefined): RoutingExplanationView {
  if (value === undefined || value.trim().length === 0) {
    return { explanation: [], reasonCodes: [] };
  }
  try {
    const parsed = JSON.parse(value) as JsonValue;
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
      return { explanation: [], reasonCodes: [] };
    }
    const record = parsed as JsonObject;
    return {
      explanation: readStringList(record, "explanation"),
      budgetOutcome: readString(record, "budget_outcome") ?? undefined,
      reasonCodes: readStringList(record, "reason_codes"),
      taskClass: readString(record, "task_class") ?? undefined,
      routingAction: readString(record, "routing_action") ?? undefined,
      credentialId: readString(record, "credential_id") ?? undefined,
      deferred: readBool(record, "deferred") || undefined,
      lease: readObject(record, "lease")
        ? {
            state: readString(readObject(record, "lease") ?? {}, "state") ?? undefined,
            priority: readString(readObject(record, "lease") ?? {}, "priority") ?? undefined,
            estimatedWaitMs:
              readNumber(readObject(record, "lease") ?? {}, "estimated_wait_ms") ?? undefined,
            reason: readString(readObject(record, "lease") ?? {}, "reason") ?? undefined,
          }
        : undefined,
    };
  } catch {
    return { explanation: [], reasonCodes: [] };
  }
}

function readProviderRegistryProvider(entry: JsonObject): ProviderRegistryProviderView {
  const health = readObject(entry, "health") ?? {};
  const discovery = readObject(entry, "discovery") ?? {};
  const runtimeMetrics = readObject(entry, "runtime_metrics") ?? {};
  const circuitBreaker = readObject(entry, "circuit_breaker") ?? {};
  return {
    providerId: readString(entry, "provider_id") ?? "unknown",
    credentialId: readString(entry, "credential_id") ?? undefined,
    displayName: readString(entry, "display_name") ?? readString(entry, "provider_id") ?? "unknown",
    kind: readString(entry, "kind") ?? "unknown",
    enabled: readBool(entry, "enabled"),
    endpointBaseUrl: readString(entry, "endpoint_base_url") ?? undefined,
    authProfileId: readString(entry, "auth_profile_id") ?? undefined,
    credentialSource: readString(entry, "credential_source") ?? undefined,
    healthState: readString(health, "state") ?? readString(health, "status") ?? "unknown",
    healthMessage: readString(health, "message") ?? undefined,
    discoveryStatus: readString(discovery, "status") ?? "unknown",
    discoveredModelIds: readStringList(discovery, "discovered_model_ids"),
    errorRateBps: readNumber(runtimeMetrics, "error_rate_bps") ?? 0,
    avgLatencyMs: readNumber(runtimeMetrics, "avg_latency_ms") ?? 0,
    lastUsedAtUnixMs: readNumber(runtimeMetrics, "last_used_at_unix_ms") ?? undefined,
    lastSuccessAtUnixMs: readNumber(runtimeMetrics, "last_success_at_unix_ms") ?? undefined,
    lastErrorAtUnixMs: readNumber(runtimeMetrics, "last_error_at_unix_ms") ?? undefined,
    lastError: readProviderFailure(readObject(runtimeMetrics, "last_error")),
    circuitOpen: readBool(circuitBreaker, "open"),
  };
}

function readProviderRegistryCredential(entry: JsonObject): ProviderRegistryCredentialView {
  const health = readObject(entry, "health") ?? {};
  const runtimeMetrics = readObject(entry, "runtime_metrics") ?? {};
  const capabilitySummary = readObject(entry, "capability_summary") ?? {};
  const capabilities: string[] = [];
  if (readBool(capabilitySummary, "chat")) {
    capabilities.push("chat");
  }
  if (readBool(capabilitySummary, "embeddings")) {
    capabilities.push("embeddings");
  }
  if (readBool(capabilitySummary, "audio_transcription")) {
    capabilities.push("audio");
  }
  if (readBool(capabilitySummary, "vision")) {
    capabilities.push("vision");
  }
  const maxContextTokens = readNumber(capabilitySummary, "max_context_tokens");
  if (maxContextTokens !== undefined) {
    capabilities.push(`ctx:${maxContextTokens}`);
  }
  return {
    credentialId: readString(entry, "credential_id") ?? "unknown",
    providerId: readString(entry, "provider_id") ?? "unknown",
    providerKind: readString(entry, "provider_kind") ?? "unknown",
    authProfileId: readString(entry, "auth_profile_id") ?? undefined,
    authProfileProviderKind: readString(entry, "auth_profile_provider_kind") ?? undefined,
    credentialSource: readString(entry, "credential_source") ?? undefined,
    availabilityState: readString(entry, "availability_state") ?? "unknown",
    capabilitySummary: capabilities,
    healthState: readString(health, "state") ?? readString(health, "status") ?? "unknown",
    healthMessage: readString(health, "message") ?? undefined,
    errorRateBps: readNumber(runtimeMetrics, "error_rate_bps") ?? 0,
    avgLatencyMs: readNumber(runtimeMetrics, "avg_latency_ms") ?? 0,
    lastUsedAtUnixMs: readNumber(runtimeMetrics, "last_used_at_unix_ms") ?? undefined,
    lastSuccessAtUnixMs: readNumber(runtimeMetrics, "last_success_at_unix_ms") ?? undefined,
    lastErrorAtUnixMs: readNumber(runtimeMetrics, "last_error_at_unix_ms") ?? undefined,
    lastError: readProviderFailure(readObject(runtimeMetrics, "last_error")),
  };
}

function readProviderRegistryModel(entry: JsonObject): ProviderRegistryModelView {
  const capabilities = readObject(entry, "capabilities") ?? {};
  return {
    modelId: readString(entry, "model_id") ?? "unknown",
    providerId: readString(entry, "provider_id") ?? "unknown",
    role: readString(entry, "role") ?? "unknown",
    enabled: readBool(entry, "enabled"),
    toolCalls: readBool(capabilities, "tool_calls"),
    jsonMode: readBool(capabilities, "json_mode"),
    vision: readBool(capabilities, "vision"),
    audioTranscribe: readBool(capabilities, "audio_transcribe"),
    embeddings: readBool(capabilities, "embeddings"),
    maxContextTokens: readNumber(capabilities, "max_context_tokens") ?? undefined,
    costTier: readString(capabilities, "cost_tier") ?? undefined,
    latencyTier: readString(capabilities, "latency_tier") ?? undefined,
    recommendedUseCases: readStringList(capabilities, "recommended_use_cases"),
    knownLimitations: readStringList(capabilities, "known_limitations"),
  };
}

function readProviderFailure(entry: JsonObject | null): ProviderFailureView | undefined {
  if (entry === null) {
    return undefined;
  }
  const className = readString(entry, "class");
  if (className === null) {
    return undefined;
  }
  return {
    class: className,
    recommendedAction: readString(entry, "recommended_action") ?? undefined,
    statusCode: readNumber(entry, "status_code") ?? undefined,
    providerDetail: readString(entry, "provider_detail") ?? undefined,
    message: readString(entry, "message") ?? undefined,
  };
}
