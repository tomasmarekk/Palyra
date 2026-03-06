import { useState } from "react";

import type { ConsoleApiClient, JsonValue } from "../../consoleApi";
import {
  emptyToUndefined,
  encodeBase64,
  isJsonObject,
  toErrorMessage,
  toJsonObjectArray,
  type JsonObject
} from "../shared";

type UseConfigDomainArgs = {
  api: ConsoleApiClient;
  setError: (message: string | null) => void;
  setNotice: (message: string | null) => void;
};

export function useConfigDomain({ api, setError, setNotice }: UseConfigDomainArgs) {
  const [configBusy, setConfigBusy] = useState(false);
  const [configInspectPath, setConfigInspectPath] = useState("palyra.toml");
  const [configInspectSnapshot, setConfigInspectSnapshot] = useState<JsonObject | null>(null);
  const [configMutationKey, setConfigMutationKey] = useState("");
  const [configMutationValue, setConfigMutationValue] = useState("");
  const [configValidation, setConfigValidation] = useState<JsonObject | null>(null);
  const [configSecretsScope, setConfigSecretsScope] = useState("global");
  const [configSecrets, setConfigSecrets] = useState<JsonObject[]>([]);
  const [configSecretKey, setConfigSecretKey] = useState("");
  const [configSecretValue, setConfigSecretValue] = useState("");
  const [configSecretReveal, setConfigSecretReveal] = useState<JsonObject | null>(null);

  async function refreshConfigSurface(): Promise<void> {
    setConfigBusy(true);
    setError(null);
    try {
      const [inspectResponse, validationResponse, secretsResponse] = await Promise.all([
        api.inspectConfig({ path: emptyToUndefined(configInspectPath), show_secrets: false, backups: 3 }),
        api.validateConfig({ path: emptyToUndefined(configInspectPath) }),
        api.listSecrets(configSecretsScope)
      ]);
      setConfigInspectSnapshot(
        isJsonObject(inspectResponse as unknown as JsonValue) ? (inspectResponse as unknown as JsonObject) : null
      );
      setConfigValidation(
        isJsonObject(validationResponse as unknown as JsonValue)
          ? (validationResponse as unknown as JsonObject)
          : null
      );
      setConfigSecrets(toJsonObjectArray(secretsResponse.secrets as unknown as JsonValue[]));
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setConfigBusy(false);
    }
  }

  async function inspectConfigSurface(): Promise<void> {
    setConfigBusy(true);
    setError(null);
    try {
      const response = await api.inspectConfig({
        path: emptyToUndefined(configInspectPath),
        show_secrets: false,
        backups: 3
      });
      setConfigInspectSnapshot(isJsonObject(response as unknown as JsonValue) ? (response as unknown as JsonObject) : null);
      setNotice("Config snapshot refreshed.");
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setConfigBusy(false);
    }
  }

  async function validateConfigSurface(): Promise<void> {
    setConfigBusy(true);
    setError(null);
    try {
      const response = await api.validateConfig({ path: emptyToUndefined(configInspectPath) });
      setConfigValidation(isJsonObject(response as unknown as JsonValue) ? (response as unknown as JsonObject) : null);
      setNotice(response.valid ? "Config validation passed." : "Config validation reported issues.");
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setConfigBusy(false);
    }
  }

  async function mutateConfigSurface(): Promise<void> {
    if (configMutationKey.trim().length === 0) {
      setError("Config key cannot be empty.");
      return;
    }
    setConfigBusy(true);
    setError(null);
    setNotice(null);
    try {
      await api.mutateConfig({
        path: emptyToUndefined(configInspectPath),
        key: configMutationKey.trim(),
        value: configMutationValue
      });
      setNotice("Config mutation applied.");
      await refreshConfigSurface();
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setConfigBusy(false);
    }
  }

  async function refreshSecrets(): Promise<void> {
    setConfigBusy(true);
    setError(null);
    try {
      const response = await api.listSecrets(configSecretsScope);
      setConfigSecrets(toJsonObjectArray(response.secrets as unknown as JsonValue[]));
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setConfigBusy(false);
    }
  }

  async function setSecretValue(): Promise<void> {
    if (configSecretKey.trim().length === 0) {
      setError("Secret key cannot be empty.");
      return;
    }
    setConfigBusy(true);
    setError(null);
    setNotice(null);
    try {
      await api.setSecret({
        scope: configSecretsScope,
        key: configSecretKey.trim(),
        value_base64: encodeBase64(configSecretValue)
      });
      setNotice("Secret stored.");
      setConfigSecretValue("");
      await refreshSecrets();
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setConfigBusy(false);
    }
  }

  async function revealSecretValue(): Promise<void> {
    if (configSecretKey.trim().length === 0) {
      setError("Secret key cannot be empty.");
      return;
    }
    setConfigBusy(true);
    setError(null);
    setNotice(null);
    try {
      const response = await api.revealSecret({
        scope: configSecretsScope,
        key: configSecretKey.trim(),
        reveal: true
      });
      setConfigSecretReveal(isJsonObject(response as unknown as JsonValue) ? (response as unknown as JsonObject) : null);
      setNotice("Secret revealed in current session.");
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setConfigBusy(false);
    }
  }

  async function deleteSecretValue(): Promise<void> {
    if (configSecretKey.trim().length === 0) {
      setError("Secret key cannot be empty.");
      return;
    }
    setConfigBusy(true);
    setError(null);
    setNotice(null);
    try {
      await api.deleteSecret({
        scope: configSecretsScope,
        key: configSecretKey.trim()
      });
      setNotice("Secret deleted.");
      setConfigSecretReveal(null);
      await refreshSecrets();
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setConfigBusy(false);
    }
  }

  function resetConfigDomain(): void {
    setConfigBusy(false);
    setConfigInspectPath("palyra.toml");
    setConfigInspectSnapshot(null);
    setConfigMutationKey("");
    setConfigMutationValue("");
    setConfigValidation(null);
    setConfigSecretsScope("global");
    setConfigSecrets([]);
    setConfigSecretKey("");
    setConfigSecretValue("");
    setConfigSecretReveal(null);
  }

  return {
    configBusy,
    configInspectPath,
    setConfigInspectPath,
    configInspectSnapshot,
    configMutationKey,
    setConfigMutationKey,
    configMutationValue,
    setConfigMutationValue,
    configValidation,
    configSecretsScope,
    setConfigSecretsScope,
    configSecrets,
    configSecretKey,
    setConfigSecretKey,
    configSecretValue,
    setConfigSecretValue,
    configSecretReveal,
    refreshConfigSurface,
    inspectConfigSurface,
    validateConfigSurface,
    mutateConfigSurface,
    refreshSecrets,
    setSecretValue,
    revealSecretValue,
    deleteSecretValue,
    resetConfigDomain
  };
}
