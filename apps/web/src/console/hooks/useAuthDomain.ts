import { useState } from "react";

import type { ConsoleApiClient, JsonValue } from "../../consoleApi";
import {
  emptyToUndefined,
  isJsonObject,
  readString,
  toErrorMessage,
  type JsonObject
} from "../shared";

type UseAuthDomainArgs = {
  api: ConsoleApiClient;
  setError: (message: string | null) => void;
  setNotice: (message: string | null) => void;
};

export function useAuthDomain({ api, setError, setNotice }: UseAuthDomainArgs) {
  const [authBusy, setAuthBusy] = useState(false);
  const [authProfiles, setAuthProfiles] = useState<JsonObject[]>([]);
  const [authHealth, setAuthHealth] = useState<JsonObject | null>(null);
  const [authProviderState, setAuthProviderState] = useState<JsonObject | null>(null);
  const [authDefaultProfileId, setAuthDefaultProfileId] = useState("");
  const [authBootstrapProfileId, setAuthBootstrapProfileId] = useState("");

  async function refreshAuth(): Promise<void> {
    setAuthBusy(true);
    setError(null);
    try {
      const [profilesResponse, healthResponse, providerResponse] = await Promise.all([
        api.listAuthProfiles(),
        api.getAuthHealth(),
        api.getOpenAiProviderState()
      ]);
      const nextProfiles = profilesResponse.profiles
        .map((entry) => (isJsonObject(entry as unknown as JsonValue) ? (entry as unknown as JsonObject) : null))
        .filter((entry): entry is JsonObject => entry !== null);
      setAuthProfiles(nextProfiles);
      setAuthHealth(
        isJsonObject(healthResponse as unknown as JsonValue) ? (healthResponse as unknown as JsonObject) : null
      );
      const providerRecord = isJsonObject(providerResponse as unknown as JsonValue)
        ? (providerResponse as unknown as JsonObject)
        : null;
      setAuthProviderState(providerRecord);
      if (providerRecord !== null) {
        const defaultProfileId = readString(providerRecord, "default_profile_id") ?? "";
        setAuthDefaultProfileId((current) => (current.trim().length === 0 ? defaultProfileId : current));
        setAuthBootstrapProfileId((current) => (current.trim().length === 0 ? defaultProfileId : current));
      }
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setAuthBusy(false);
    }
  }

  async function executeOpenAiAction(
    action: "bootstrap" | "reconnect" | "revoke" | "default-profile"
  ): Promise<void> {
    setAuthBusy(true);
    setError(null);
    setNotice(null);
    try {
      const profileId = emptyToUndefined(
        action === "default-profile" ? authDefaultProfileId : authBootstrapProfileId
      );
      const response =
        action === "bootstrap"
          ? await api.startOpenAiProviderBootstrap({ profile_id: profileId })
          : action === "reconnect"
            ? await api.reconnectOpenAiProvider({ profile_id: profileId })
            : action === "revoke"
              ? await api.revokeOpenAiProvider({ profile_id: profileId })
              : await api.setOpenAiDefaultProfile({ profile_id: profileId });
      setNotice(response.message);
      await refreshAuth();
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setAuthBusy(false);
    }
  }

  function resetAuthDomain(): void {
    setAuthBusy(false);
    setAuthProfiles([]);
    setAuthHealth(null);
    setAuthProviderState(null);
    setAuthDefaultProfileId("");
    setAuthBootstrapProfileId("");
  }

  return {
    authBusy,
    authProfiles,
    authHealth,
    authProviderState,
    authDefaultProfileId,
    setAuthDefaultProfileId,
    authBootstrapProfileId,
    setAuthBootstrapProfileId,
    refreshAuth,
    executeOpenAiAction,
    resetAuthDomain
  };
}
