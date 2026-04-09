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
  circuitOpen: boolean;
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
  credentialSource?: string;
  apiKeyConfigured: boolean;
  defaultChatModelId?: string;
  defaultEmbeddingsModelId?: string;
  failoverEnabled: boolean;
  responseCacheEnabled: boolean;
  providers: ProviderRegistryProviderView[];
  models: ProviderRegistryModelView[];
};

export type RoutingExplanationView = {
  explanation: string[];
  budgetOutcome?: string;
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
  const models = toJsonObjectArray(Array.isArray(registry?.models) ? registry.models : []).map(
    readProviderRegistryModel,
  );
  return {
    providerState: readString(readObject(modelProvider, "health") ?? {}, "status")
      ?? readString(modelProvider, "kind")
      ?? "unknown",
    providerKind: readString(modelProvider, "kind") ?? "unknown",
    providerId: readString(modelProvider, "provider_id") ?? undefined,
    modelId: readString(modelProvider, "model_id") ?? undefined,
    credentialSource: readString(modelProvider, "credential_source") ?? undefined,
    apiKeyConfigured: readBool(modelProvider, "api_key_configured"),
    defaultChatModelId: readString(registry ?? {}, "default_chat_model_id") ?? undefined,
    defaultEmbeddingsModelId:
      readString(registry ?? {}, "default_embeddings_model_id") ?? undefined,
    failoverEnabled: readBool(registry ?? {}, "failover_enabled"),
    responseCacheEnabled: readBool(registry ?? {}, "response_cache_enabled"),
    providers,
    models,
  };
}

export function parseRoutingExplanation(value: string | undefined): RoutingExplanationView {
  if (value === undefined || value.trim().length === 0) {
    return { explanation: [] };
  }
  try {
    const parsed = JSON.parse(value) as JsonValue;
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
      return { explanation: [] };
    }
    const record = parsed as JsonObject;
    return {
      explanation: readStringList(record, "explanation"),
      budgetOutcome: readString(record, "budget_outcome") ?? undefined,
    };
  } catch {
    return { explanation: [] };
  }
}

function readProviderRegistryProvider(entry: JsonObject): ProviderRegistryProviderView {
  const health = readObject(entry, "health") ?? {};
  const discovery = readObject(entry, "discovery") ?? {};
  const runtimeMetrics = readObject(entry, "runtime_metrics") ?? {};
  const circuitBreaker = readObject(entry, "circuit_breaker") ?? {};
  return {
    providerId: readString(entry, "provider_id") ?? "unknown",
    displayName: readString(entry, "display_name") ?? readString(entry, "provider_id") ?? "unknown",
    kind: readString(entry, "kind") ?? "unknown",
    enabled: readBool(entry, "enabled"),
    endpointBaseUrl: readString(entry, "endpoint_base_url") ?? undefined,
    authProfileId: readString(entry, "auth_profile_id") ?? undefined,
    credentialSource: readString(entry, "credential_source") ?? undefined,
    healthState: readString(health, "status") ?? "unknown",
    healthMessage: readString(health, "message") ?? undefined,
    discoveryStatus: readString(discovery, "status") ?? "unknown",
    discoveredModelIds: readStringList(discovery, "discovered_model_ids"),
    errorRateBps: readNumber(runtimeMetrics, "error_rate_bps") ?? 0,
    avgLatencyMs: readNumber(runtimeMetrics, "avg_latency_ms") ?? 0,
    circuitOpen: readBool(circuitBreaker, "open"),
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
