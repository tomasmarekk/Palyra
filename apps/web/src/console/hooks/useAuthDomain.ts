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
const OPENAI_OAUTH_CALLBACK_EVENT_TYPE = "palyra-openai-oauth-complete";
const OPENAI_OAUTH_POLL_INTERVAL_MS = 1_500;

type UseAuthDomainArgs = {
  api: ConsoleApiClient;
  setError: (message: string | null) => void;
  setNotice: (message: string | null) => void;
};

export type OpenAiScopeKind = "global" | "agent";
export type AuthProviderKind = "openai" | "anthropic";

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

    const [profilesResponse, healthResponse, openAiProviderResponse, anthropicProviderResponse] =
      await Promise.all([
        api.listAuthProfiles(),
        api.getAuthHealth(healthParams),
        api.getOpenAiProviderState(),
        api.getAnthropicProviderState(),
      ]);

    setAuthProfiles(profilesResponse.profiles);
    setAuthHealth(healthResponse);
    const providerStates = {
      openai: openAiProviderResponse,
      anthropic: anthropicProviderResponse,
    };
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
    const providerLabel = authApiKeyDraft.provider === "anthropic" ? "Anthropic" : "OpenAI";
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
      const response =
        authApiKeyDraft.provider === "anthropic"
          ? await api.connectAnthropicApiKey(request)
          : await api.connectOpenAiApiKey(request);
      resetAuthApiKeyDraft();
      setNotice(response.message);
      await loadAuthState();
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setAuthBusy(false);
    }
  }

  async function startOpenAiOAuth(): Promise<void> {
    const profileName = authOAuthDraft.profileName.trim();
    const clientId = authOAuthDraft.clientId.trim();
    const clientSecret = authOAuthDraft.clientSecret.trim();
    if (profileName.length === 0) {
      setError("OpenAI OAuth connect requires a profile name.");
      return;
    }
    if (clientId.length === 0) {
      setError("OpenAI OAuth connect requires client_id.");
      return;
    }
    if (clientSecret.length === 0) {
      setError("OpenAI OAuth connect requires client_secret.");
      return;
    }

    setAuthBusy(true);
    setError(null);
    setNotice(null);
    try {
      const response = await api.startOpenAiProviderBootstrap({
        profile_name: profileName,
        scope: resolveScope(authOAuthDraft.scopeKind, authOAuthDraft.agentId),
        client_id: clientId,
        client_secret: clientSecret,
        scopes: normalizeScopes(authOAuthDraft.scopes),
        set_default: authOAuthDraft.setDefault,
      });
      beginOauthAttempt(response);
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setAuthBusy(false);
    }
  }

  async function reconnectOpenAiProfile(profileId: string): Promise<void> {
    setAuthBusy(true);
    setError(null);
    setNotice(null);
    try {
      const response = await api.reconnectOpenAiProvider({
        profile_id: normalizeProfileSelection(profileId),
      });
      beginOauthAttempt(response);
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setAuthBusy(false);
    }
  }

  async function refreshOpenAiProfile(profileId: string): Promise<void> {
    setAuthBusy(true);
    setError(null);
    setNotice(null);
    try {
      const response = await api.refreshOpenAiProvider({
        profile_id: normalizeProfileSelection(profileId),
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
      const response =
        profile.provider.kind === "anthropic"
          ? await api.revokeAnthropicProvider({
              profile_id: normalizedProfileId,
            })
          : await api.revokeOpenAiProvider({
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
      const response =
        profile.provider.kind === "anthropic"
          ? await api.setAnthropicDefaultProfile({
              profile_id: normalizedProfileId,
            })
          : await api.setOpenAiDefaultProfile({
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
      setError("No active OpenAI OAuth attempt is waiting for a callback.");
      return;
    }

    setAuthPolling(true);
    try {
      const response = await api.getOpenAiProviderCallbackState(activeAttemptId);
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
      setError("No active OpenAI OAuth authorization URL is available.");
      return;
    }
    oauthPopupRef.current = openOauthWindow(authActiveOauthAttempt.authorization_url);
    if (oauthPopupRef.current === null) {
      setNotice(
        "OpenAI OAuth URL is ready, but the browser blocked the pop-up. Use the manual link.",
      );
      return;
    }
    setNotice("OpenAI OAuth window opened.");
  }

  function prepareApiKeyRotation(profile: AuthProfileView): void {
    setAuthApiKeyDraft({
      provider: profile.provider.kind === "anthropic" ? "anthropic" : "openai",
      profileId: profile.profile_id,
      profileName: profile.profile_name,
      scopeKind: profile.scope.kind === "agent" ? "agent" : "global",
      agentId: profile.scope.agent_id ?? "",
      apiKey: "",
      setDefault:
        authProviderStates[profile.provider.kind]?.default_profile_id === profile.profile_id,
    });
    setError(null);
    setNotice(
      `Editing ${profile.provider.kind} API key profile '${profile.profile_name}'. Submit a new key to rotate it.`,
    );
  }

  function cancelApiKeyRotation(): void {
    resetAuthApiKeyDraft();
    setError(null);
    setNotice("OpenAI API key form reset.");
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
      setNotice(
        "OpenAI OAuth URL issued. The pop-up was blocked, so use the manual open link below.",
      );
      return;
    }
    setNotice("OpenAI OAuth window opened. Finish the authorization to complete the profile.");
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
    startOpenAiOAuth,
    reconnectOpenAiProfile,
    refreshOpenAiProfile,
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
  const selectedProfileId =
    providerStates.openai?.default_profile_id ?? providerStates.anthropic?.default_profile_id;
  if (selectedProfileId !== undefined) {
    const selectedProfile = profiles.find((profile) => profile.profile_id === selectedProfileId);
    if (selectedProfile !== undefined) {
      return providerStates[selectedProfile.provider.kind] ?? null;
    }
  }
  return providerStates.openai ?? providerStates.anthropic ?? null;
}

function createDefaultOAuthDraft(): AuthOAuthDraft {
  return {
    profileName: "",
    scopeKind: "global",
    agentId: "",
    clientId: "",
    clientSecret: "",
    scopes: DEFAULT_OPENAI_OAUTH_SCOPES,
    setDefault: false,
  };
}

function normalizeProfileSelection(profileId?: string | null): string | undefined {
  return emptyToUndefined(profileId ?? "");
}

function resolveScope(scopeKind: OpenAiScopeKind, agentId: string): AuthProfileScope {
  if (scopeKind === "agent") {
    const normalizedAgentId = emptyToUndefined(agentId);
    if (normalizedAgentId === undefined) {
      throw new Error("Agent-scoped OpenAI profiles require agent_id.");
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

function normalizeScopes(raw: string): string[] {
  const scopes = raw
    .split(/[\s,]+/u)
    .map((scope) => scope.trim())
    .filter((scope) => scope.length > 0);
  return scopes.length > 0 ? Array.from(new Set(scopes)) : DEFAULT_OPENAI_OAUTH_SCOPES.split(" ");
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
