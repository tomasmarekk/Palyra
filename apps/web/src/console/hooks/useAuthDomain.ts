import { useEffect, useRef, useState } from "react";

import type {
  AuthHealthEnvelope,
  AuthProfileScope,
  AuthProfileView,
  ConsoleApiClient,
  OpenAiOAuthBootstrapEnvelope,
  OpenAiOAuthCallbackStateEnvelope,
  ProviderProbeResult,
  ProviderAuthStateEnvelope,
} from "../../consoleApi";
import { emptyToUndefined, toErrorMessage } from "../shared";

const DEFAULT_OPENAI_OAUTH_SCOPES = "openid profile email offline_access";
const DEFAULT_MINIMAX_OAUTH_SCOPES = "group_id profile model.completion";
const OPENAI_OAUTH_CALLBACK_EVENT_TYPE = "palyra-openai-oauth-complete";
const OPENAI_OAUTH_POLL_INTERVAL_MS = 1_500;

type UseAuthDomainArgs = {
  api: ConsoleApiClient;
  setError: (message: string | null) => void;
  setNotice: (message: string | null) => void;
};

export type OpenAiScopeKind = "global" | "agent";
export type AuthProviderKind = "openai" | "anthropic" | "minimax";

export type AuthProviderConfig = {
  key: AuthProviderKind;
  label: string;
  oauthSupported: boolean;
  oauthRequiresClientId: boolean;
  oauthRequiresClientSecret: boolean;
  defaultOAuthScopes: string;
};

export const AUTH_PROVIDER_CONFIGS: AuthProviderConfig[] = [
  {
    key: "openai",
    label: "OpenAI",
    oauthSupported: true,
    oauthRequiresClientId: true,
    oauthRequiresClientSecret: true,
    defaultOAuthScopes: DEFAULT_OPENAI_OAUTH_SCOPES,
  },
  {
    key: "anthropic",
    label: "Anthropic",
    oauthSupported: false,
    oauthRequiresClientId: false,
    oauthRequiresClientSecret: false,
    defaultOAuthScopes: "",
  },
  {
    key: "minimax",
    label: "MiniMax",
    oauthSupported: true,
    oauthRequiresClientId: false,
    oauthRequiresClientSecret: false,
    defaultOAuthScopes: DEFAULT_MINIMAX_OAUTH_SCOPES,
  },
];

export type AuthApiKeyDraft = {
  provider: AuthProviderKind;
  profileId: string;
  profileName: string;
  scopeKind: OpenAiScopeKind;
  agentId: string;
  apiKey: string;
  setDefault: boolean;
};

export type AuthOAuthDraft = {
  provider: AuthProviderKind;
  profileName: string;
  scopeKind: OpenAiScopeKind;
  agentId: string;
  clientId: string;
  clientSecret: string;
  scopes: string;
  setDefault: boolean;
};

export function useAuthDomain({ api, setError, setNotice }: UseAuthDomainArgs) {
  const [authBusy, setAuthBusy] = useState(false);
  const [authPolling, setAuthPolling] = useState(false);
  const [authProfiles, setAuthProfiles] = useState<AuthProfileView[]>([]);
  const [authHealth, setAuthHealth] = useState<AuthHealthEnvelope | null>(null);
  const [authProviderState, setAuthProviderState] = useState<ProviderAuthStateEnvelope | null>(
    null,
  );
  const [authProviderStates, setAuthProviderStates] = useState<
    Record<string, ProviderAuthStateEnvelope>
  >({});
  const [authProviderProbeMode, setAuthProviderProbeMode] = useState<string | null>(null);
  const [authProviderProbeResults, setAuthProviderProbeResults] = useState<
    Record<string, ProviderProbeResult>
  >({});
  const [authApiKeyDraft, setAuthApiKeyDraft] = useState<AuthApiKeyDraft>(createDefaultApiKeyDraft);
  const [authOAuthDraft, setAuthOAuthDraft] = useState<AuthOAuthDraft>(createDefaultOAuthDraft);
  const [authActiveOauthAttempt, setAuthActiveOauthAttempt] =
    useState<OpenAiOAuthBootstrapEnvelope | null>(null);
  const [authOauthCallbackState, setAuthOauthCallbackState] =
    useState<OpenAiOAuthCallbackStateEnvelope | null>(null);

  const oauthAttemptIdRef = useRef<string | null>(null);
  const oauthPopupRef = useRef<Window | null>(null);
  const oauthPollTimerRef = useRef<number | null>(null);

  useEffect(() => {
    if (typeof window === "undefined") {
      return undefined;
    }

    const onMessage = (event: MessageEvent) => {
      if (event.origin !== window.location.origin || !isOauthCallbackMessage(event.data)) {
        return;
      }
      if (oauthAttemptIdRef.current !== event.data.attempt_id) {
        return;
      }
      void checkOpenAiCallbackState(event.data.attempt_id);
    };

    window.addEventListener("message", onMessage);
    return () => {
      window.removeEventListener("message", onMessage);
      clearOauthPolling();
    };
  }, []);

  async function loadAuthState(): Promise<void> {
    const healthParams = new URLSearchParams();
    healthParams.set("include_profiles", "true");

    const [profilesResponse, healthResponse] = await Promise.all([
      api.listAuthProfiles(),
      api.getAuthHealth(healthParams),
    ]);
    const providerKeys = providerStateKeysForProfiles(profilesResponse.profiles);
    const providerResponses = await Promise.all(
      providerKeys.map((provider) => api.getProviderState(provider)),
    );

    setAuthProfiles(profilesResponse.profiles);
    setAuthHealth(healthResponse);
    const providerStates = Object.fromEntries(
      providerKeys.map((provider, index) => [provider, providerResponses[index]]),
    ) as Record<string, ProviderAuthStateEnvelope>;
    setAuthProviderStates(providerStates);
    setAuthProviderState(resolvePrimaryProviderState(providerStates, profilesResponse.profiles));
  }

  async function refreshAuth(options?: { clearError?: boolean }): Promise<void> {
    setAuthBusy(true);
    if (options?.clearError !== false) {
      setError(null);
    }
    try {
      await loadAuthState();
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setAuthBusy(false);
    }
  }

  async function connectApiKeyProfile(): Promise<void> {
    const profileName = authApiKeyDraft.profileName.trim();
    const apiKey = authApiKeyDraft.apiKey.trim();
    const providerLabel = providerConfig(authApiKeyDraft.provider).label;
    if (profileName.length === 0) {
      setError(`${providerLabel} API key connect requires a profile name.`);
      return;
    }
    if (apiKey.length === 0) {
      setError(`${providerLabel} API key connect requires an API key.`);
      return;
    }

    setAuthBusy(true);
    setError(null);
    setNotice(null);
    try {
      const request = {
        profile_id: emptyToUndefined(authApiKeyDraft.profileId),
        profile_name: profileName,
        scope: resolveScope(authApiKeyDraft.scopeKind, authApiKeyDraft.agentId),
        api_key: apiKey,
        set_default: authApiKeyDraft.setDefault,
      };
      const response = await api.connectProviderApiKey(authApiKeyDraft.provider, request);
      resetAuthApiKeyDraft();
      setNotice(response.message);
      await loadAuthState();
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setAuthBusy(false);
    }
  }

  async function startProviderOAuth(): Promise<void> {
    const profileName = authOAuthDraft.profileName.trim();
    const provider = providerConfig(authOAuthDraft.provider);
    const clientId = authOAuthDraft.clientId.trim();
    const clientSecret = authOAuthDraft.clientSecret.trim();
    if (profileName.length === 0) {
      setError(`${provider.label} OAuth connect requires a profile name.`);
      return;
    }
    if (provider.oauthRequiresClientId && clientId.length === 0) {
      setError(`${provider.label} OAuth connect requires client_id.`);
      return;
    }
    if (provider.oauthRequiresClientSecret && clientSecret.length === 0) {
      setError(`${provider.label} OAuth connect requires client_secret.`);
      return;
    }

    setAuthBusy(true);
    setError(null);
    setNotice(null);
    try {
      const response = await api.startProviderBootstrap(authOAuthDraft.provider, {
        profile_name: profileName,
        scope: resolveScope(authOAuthDraft.scopeKind, authOAuthDraft.agentId),
        client_id: emptyToUndefined(clientId),
        client_secret: emptyToUndefined(clientSecret),
        scopes: normalizeScopes(authOAuthDraft.scopes, provider.defaultOAuthScopes),
        set_default: authOAuthDraft.setDefault,
      });
      beginOauthAttempt(response);
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setAuthBusy(false);
    }
  }

  async function reconnectProviderProfile(profile: AuthProfileView): Promise<void> {
    setAuthBusy(true);
    setError(null);
    setNotice(null);
    try {
      const response = await api.reconnectProvider(providerKeyForProfile(profile), {
        profile_id: normalizeProfileSelection(profile.profile_id),
      });
      beginOauthAttempt(response);
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setAuthBusy(false);
    }
  }

  async function refreshProviderProfile(profile: AuthProfileView): Promise<void> {
    setAuthBusy(true);
    setError(null);
    setNotice(null);
    try {
      const response = await api.refreshProvider(providerKeyForProfile(profile), {
        profile_id: normalizeProfileSelection(profile.profile_id),
      });
      setNotice(response.message);
      await loadAuthState();
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setAuthBusy(false);
    }
  }

  async function revokeOpenAiProfile(profileId: string): Promise<void> {
    setAuthBusy(true);
    setError(null);
    setNotice(null);
    try {
      const normalizedProfileId = normalizeProfileSelection(profileId);
      const response = await api.revokeOpenAiProvider({
        profile_id: normalizedProfileId,
      });
      setAuthApiKeyDraft((current) =>
        current.profileId === normalizedProfileId ? createDefaultApiKeyDraft() : current,
      );
      if (authActiveOauthAttempt?.profile_id === normalizedProfileId) {
        clearOauthPolling();
        setAuthActiveOauthAttempt(null);
        setAuthOauthCallbackState(null);
      }
      setNotice(response.message);
      await loadAuthState();
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setAuthBusy(false);
    }
  }

  async function revokeProviderProfile(profile: AuthProfileView): Promise<void> {
    setAuthBusy(true);
    setError(null);
    setNotice(null);
    try {
      const normalizedProfileId = normalizeProfileSelection(profile.profile_id);
      const response = await api.revokeProvider(providerKeyForProfile(profile), {
        profile_id: normalizedProfileId,
      });
      setAuthApiKeyDraft((current) =>
        current.profileId === normalizedProfileId ? createDefaultApiKeyDraft() : current,
      );
      if (authActiveOauthAttempt?.profile_id === normalizedProfileId) {
        clearOauthPolling();
        setAuthActiveOauthAttempt(null);
        setAuthOauthCallbackState(null);
      }
      setNotice(response.message);
      await loadAuthState();
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setAuthBusy(false);
    }
  }

  async function setDefaultProviderProfile(profile: AuthProfileView): Promise<void> {
    setAuthBusy(true);
    setError(null);
    setNotice(null);
    try {
      const normalizedProfileId = normalizeProfileSelection(profile.profile_id);
      const response = await api.setProviderDefaultProfile(providerKeyForProfile(profile), {
        profile_id: normalizedProfileId,
      });
      setNotice(response.message);
      await loadAuthState();
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setAuthBusy(false);
    }
  }

  async function probeProvider(providerId: string, discover: boolean): Promise<void> {
    const normalizedProviderId = emptyToUndefined(providerId);
    if (normalizedProviderId === undefined) {
      setError("Provider probe requires a provider id.");
      return;
    }

    setAuthBusy(true);
    setError(null);
    setNotice(null);
    try {
      const response = discover
        ? await api.discoverModelProviderModels({ provider_id: normalizedProviderId })
        : await api.testModelProviderConnection({ provider_id: normalizedProviderId });
      const nextResults = Object.fromEntries(
        response.providers.map(
          (result) => [result.provider_id, result] satisfies [string, ProviderProbeResult],
        ),
      );
      setAuthProviderProbeMode(response.mode);
      setAuthProviderProbeResults((current) => ({ ...current, ...nextResults }));
      const firstResult = response.providers[0];
      if (firstResult !== undefined) {
        setNotice(
          discover
            ? `Discovery for ${firstResult.provider_id}: ${firstResult.message}`
            : `Connection test for ${firstResult.provider_id}: ${firstResult.message}`,
        );
      }
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setAuthBusy(false);
    }
  }

  async function checkOpenAiCallbackState(attemptId?: string): Promise<void> {
    const activeAttemptId = normalizeProfileSelection(
      attemptId ?? oauthAttemptIdRef.current ?? authActiveOauthAttempt?.attempt_id,
    );
    if (activeAttemptId === undefined) {
      setError("No active provider OAuth attempt is waiting for completion.");
      return;
    }
    const provider = authActiveOauthAttempt?.provider ?? "openai";

    setAuthPolling(true);
    try {
      const response = await api.getProviderCallbackState(provider, activeAttemptId);
      setAuthOauthCallbackState(response);
      if (response.state === "pending") {
        scheduleOauthPolling(activeAttemptId);
        return;
      }

      clearOauthPolling();
      closeOauthWindow();
      if (response.state === "succeeded") {
        resetAuthOAuthDraft();
        setNotice(response.message);
      } else {
        setError(response.message);
      }
      await loadAuthState();
    } catch (failure) {
      clearOauthPolling();
      setError(toErrorMessage(failure));
    } finally {
      setAuthPolling(false);
    }
  }

  function openActiveOauthWindow(): void {
    if (authActiveOauthAttempt === null) {
      setError("No active provider OAuth authorization URL is available.");
      return;
    }
    oauthPopupRef.current = openOauthWindow(authActiveOauthAttempt.authorization_url);
    if (oauthPopupRef.current === null) {
      setNotice("Provider OAuth URL is ready, but the browser blocked the pop-up.");
      return;
    }
    setNotice("Provider OAuth window opened.");
  }

  function prepareApiKeyRotation(profile: AuthProfileView): void {
    const provider = providerKeyForProfile(profile);
    setAuthApiKeyDraft({
      provider,
      profileId: profile.profile_id,
      profileName: profile.profile_name,
      scopeKind: profile.scope.kind === "agent" ? "agent" : "global",
      agentId: profile.scope.agent_id ?? "",
      apiKey: "",
      setDefault: authProviderStates[provider]?.default_profile_id === profile.profile_id,
    });
    setError(null);
    setNotice(
      `Editing ${providerConfig(provider).label} API key profile '${profile.profile_name}'. Submit a new key to rotate it.`,
    );
  }

  function cancelApiKeyRotation(): void {
    resetAuthApiKeyDraft();
    setError(null);
    setNotice("API key form reset.");
  }

  function resetAuthDomain(): void {
    clearOauthPolling();
    closeOauthWindow();
    setAuthBusy(false);
    setAuthPolling(false);
    setAuthProfiles([]);
    setAuthHealth(null);
    setAuthProviderState(null);
    setAuthProviderStates({});
    setAuthProviderProbeMode(null);
    setAuthProviderProbeResults({});
    setAuthActiveOauthAttempt(null);
    setAuthOauthCallbackState(null);
    resetAuthApiKeyDraft();
    resetAuthOAuthDraft();
  }

  function resetAuthApiKeyDraft(): void {
    setAuthApiKeyDraft(createDefaultApiKeyDraft());
  }

  function resetAuthOAuthDraft(): void {
    setAuthOAuthDraft(createDefaultOAuthDraft());
  }

  function beginOauthAttempt(response: OpenAiOAuthBootstrapEnvelope): void {
    clearOauthPolling();
    setAuthActiveOauthAttempt(response);
    setAuthOauthCallbackState({
      contract: response.contract,
      provider: response.provider,
      attempt_id: response.attempt_id,
      state: "pending",
      message: response.message,
      profile_id: response.profile_id,
      expires_at_unix_ms: response.expires_at_unix_ms,
    });
    oauthPopupRef.current = openOauthWindow(response.authorization_url);
    scheduleOauthPolling(response.attempt_id);
    if (oauthPopupRef.current === null) {
      setNotice("Provider OAuth URL issued. The pop-up was blocked.");
      return;
    }
    const providerLabel = providerConfig(authProviderKindFromString(response.provider)).label;
    setNotice(
      `${providerLabel} OAuth window opened. Finish the authorization to complete the profile.`,
    );
  }

  function scheduleOauthPolling(attemptId: string): void {
    clearOauthPolling();
    oauthAttemptIdRef.current = attemptId;
    if (typeof window === "undefined") {
      return;
    }
    oauthPollTimerRef.current = window.setTimeout(() => {
      void checkOpenAiCallbackState(attemptId);
    }, OPENAI_OAUTH_POLL_INTERVAL_MS);
  }

  function clearOauthPolling(): void {
    oauthAttemptIdRef.current = null;
    if (oauthPollTimerRef.current !== null && typeof window !== "undefined") {
      window.clearTimeout(oauthPollTimerRef.current);
    }
    oauthPollTimerRef.current = null;
  }

  function closeOauthWindow(): void {
    if (oauthPopupRef.current === null) {
      return;
    }
    try {
      oauthPopupRef.current.close();
    } catch {
      // Browser may block close on a window/tab not opened by this call site.
    }
    oauthPopupRef.current = null;
  }

  return {
    authBusy,
    authPolling,
    authProfiles,
    authHealth,
    authProviderState,
    authProviderStates,
    authProviderProbeMode,
    authProviderProbeResults,
    authApiKeyDraft,
    setAuthApiKeyDraft,
    authOAuthDraft,
    setAuthOAuthDraft,
    authActiveOauthAttempt,
    authOauthCallbackState,
    refreshAuth,
    connectApiKeyProfile,
    startProviderOAuth,
    reconnectProviderProfile,
    refreshProviderProfile,
    revokeOpenAiProfile,
    revokeProviderProfile,
    setDefaultProviderProfile,
    probeProvider,
    checkOpenAiCallbackState,
    openActiveOauthWindow,
    prepareApiKeyRotation,
    cancelApiKeyRotation,
    resetAuthDomain,
  };
}

function createDefaultApiKeyDraft(): AuthApiKeyDraft {
  return {
    provider: "openai",
    profileId: "",
    profileName: "",
    scopeKind: "global",
    agentId: "",
    apiKey: "",
    setDefault: true,
  };
}

function resolvePrimaryProviderState(
  providerStates: Record<string, ProviderAuthStateEnvelope>,
  profiles: AuthProfileView[],
): ProviderAuthStateEnvelope | null {
  const selectedProfileId = AUTH_PROVIDER_CONFIGS.map(
    (provider) => providerStates[provider.key]?.default_profile_id,
  ).find((profileId) => profileId !== undefined);
  if (selectedProfileId !== undefined) {
    const selectedProfile = profiles.find((profile) => profile.profile_id === selectedProfileId);
    if (selectedProfile !== undefined) {
      return providerStates[providerKeyForProfile(selectedProfile)] ?? null;
    }
  }
  return (
    AUTH_PROVIDER_CONFIGS.map((provider) => providerStates[provider.key]).find(Boolean) ?? null
  );
}

function providerStateKeysForProfiles(profiles: AuthProfileView[]): AuthProviderKind[] {
  const keys = new Set<AuthProviderKind>(["openai", "anthropic"]);
  for (const profile of profiles) {
    keys.add(providerKeyForProfile(profile));
  }
  return AUTH_PROVIDER_CONFIGS.map((provider) => provider.key).filter((key) => keys.has(key));
}

function createDefaultOAuthDraft(): AuthOAuthDraft {
  return {
    provider: "openai",
    profileName: "",
    scopeKind: "global",
    agentId: "",
    clientId: "",
    clientSecret: "",
    scopes: DEFAULT_OPENAI_OAUTH_SCOPES,
    setDefault: false,
  };
}

export function providerKeyForProfile(profile: AuthProfileView): AuthProviderKind {
  if (
    profile.provider.kind === "custom" &&
    profile.provider.custom_name?.toLowerCase() === "minimax"
  ) {
    return "minimax";
  }
  if (profile.provider.kind === "anthropic") {
    return "anthropic";
  }
  return "openai";
}

export function providerConfig(provider: AuthProviderKind): AuthProviderConfig {
  return (
    AUTH_PROVIDER_CONFIGS.find((config) => config.key === provider) ?? AUTH_PROVIDER_CONFIGS[0]
  );
}

function authProviderKindFromString(provider: string): AuthProviderKind {
  return provider === "anthropic" || provider === "minimax" ? provider : "openai";
}

function normalizeProfileSelection(profileId?: string | null): string | undefined {
  return emptyToUndefined(profileId ?? "");
}

function resolveScope(scopeKind: OpenAiScopeKind, agentId: string): AuthProfileScope {
  if (scopeKind === "agent") {
    const normalizedAgentId = emptyToUndefined(agentId);
    if (normalizedAgentId === undefined) {
      throw new Error("Agent-scoped provider profiles require agent_id.");
    }
    return {
      kind: "agent",
      agent_id: normalizedAgentId,
    };
  }
  return {
    kind: "global",
  };
}

function normalizeScopes(
  raw: string,
  defaultScopes: string = DEFAULT_OPENAI_OAUTH_SCOPES,
): string[] {
  const scopes = raw
    .split(/[\s,]+/u)
    .map((scope) => scope.trim())
    .filter((scope) => scope.length > 0);
  return scopes.length > 0 ? Array.from(new Set(scopes)) : defaultScopes.split(" ");
}

function openOauthWindow(url: string): Window | null {
  if (typeof window === "undefined") {
    return null;
  }
  const popup = window.open(
    url,
    "palyra-openai-auth",
    "popup=yes,width=720,height=860,resizable=yes,scrollbars=yes",
  );
  popup?.focus();
  return popup;
}

function isOauthCallbackMessage(value: unknown): value is { type: string; attempt_id: string } {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return false;
  }
  const record = value as Record<string, unknown>;
  return (
    record.type === OPENAI_OAUTH_CALLBACK_EVENT_TYPE &&
    typeof record.attempt_id === "string" &&
    record.attempt_id.trim().length > 0
  );
}
