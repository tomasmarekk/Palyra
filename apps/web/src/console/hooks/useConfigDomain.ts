import { useState } from "react";

import type { ConsoleApiClient, JsonValue } from "../../consoleApi";
import {
  emptyToUndefined,
  encodeBase64,
  isJsonObject,
  parseInteger,
  toErrorMessage,
  toJsonObjectArray,
  type JsonObject,
} from "../shared";

type UseConfigDomainArgs = {
  api: ConsoleApiClient;
  setError: (message: string | null) => void;
  setNotice: (message: string | null) => void;
};

export function useConfigDomain({ api, setError, setNotice }: UseConfigDomainArgs) {
  const [configBusy, setConfigBusy] = useState(false);
  const [configInspectPath, setConfigInspectPath] = useState("palyra.toml");
  const [configBackups, setConfigBackups] = useState("3");
  const [configMutationMode, setConfigMutationMode] = useState<"set" | "unset">("set");
  const [configInspectSnapshot, setConfigInspectSnapshot] = useState<JsonObject | null>(null);
  const [configMutationKey, setConfigMutationKey] = useState("");
  const [configMutationValue, setConfigMutationValue] = useState("");
  const [configValidation, setConfigValidation] = useState<JsonObject | null>(null);
  const [configLastMutation, setConfigLastMutation] = useState<JsonObject | null>(null);
  const [configDiffPreview, setConfigDiffPreview] = useState<string | null>(null);
  const [configRecoverBackup, setConfigRecoverBackup] = useState("1");
  const [configDeploymentPosture, setConfigDeploymentPosture] = useState<JsonObject | null>(null);
  const [configSecretsScope, setConfigSecretsScope] = useState("global");
  const [configSecrets, setConfigSecrets] = useState<JsonObject[]>([]);
  const [configSecretKey, setConfigSecretKey] = useState("");
  const [configSecretMetadata, setConfigSecretMetadata] = useState<JsonObject | null>(null);
  const [configSecretValue, setConfigSecretValue] = useState("");
  const [configSecretReveal, setConfigSecretReveal] = useState<JsonObject | null>(null);
  const [configuredSecrets, setConfiguredSecrets] = useState<JsonObject[]>([]);
  const [configuredSecretDetail, setConfiguredSecretDetail] = useState<JsonObject | null>(null);
  const [configReloadPlan, setConfigReloadPlan] = useState<JsonObject | null>(null);
  const [configReloadResult, setConfigReloadResult] = useState<JsonObject | null>(null);

  async function refreshConfigSurface(): Promise<void> {
    setConfigBusy(true);
    setError(null);
    try {
      const [secretsResponse, configuredSecretsResponse, deploymentResponse] = await Promise.all([
        api.listSecrets(configSecretsScope),
        api.listConfiguredSecrets(),
        api.getDeploymentPosture(),
      ]);
      setConfigSecrets(toJsonObjectArray(secretsResponse.secrets as unknown as JsonValue[]));
      setConfiguredSecrets(
        toJsonObjectArray(configuredSecretsResponse.secrets as unknown as JsonValue[]),
      );
      setConfigDeploymentPosture(
        isJsonObject(deploymentResponse as unknown as JsonValue)
          ? (deploymentResponse as unknown as JsonObject)
          : null,
      );
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
      const backups = normalizedBackupCount(configBackups);
      const response = await api.inspectConfig({
        path: emptyToUndefined(configInspectPath),
        show_secrets: false,
        backups,
      });
      setConfigInspectSnapshot(
        isJsonObject(response as unknown as JsonValue) ? (response as unknown as JsonObject) : null,
      );
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
      setConfigValidation(
        isJsonObject(response as unknown as JsonValue) ? (response as unknown as JsonObject) : null,
      );
      setNotice(
        response.valid ? "Config validation passed." : "Config validation reported issues.",
      );
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
      const previousSnapshot = readDocumentToml(configInspectSnapshot);
      const response = await api.mutateConfig({
        path: emptyToUndefined(configInspectPath),
        key: configMutationKey.trim(),
        value: configMutationMode === "unset" ? undefined : configMutationValue,
        backups: normalizedBackupCount(configBackups),
      });
      setConfigLastMutation(
        isJsonObject(response as unknown as JsonValue) ? (response as unknown as JsonObject) : null,
      );
      const nextSnapshot = await api.inspectConfig({
        path: emptyToUndefined(configInspectPath),
        show_secrets: false,
        backups: normalizedBackupCount(configBackups),
      });
      const normalizedSnapshot = isJsonObject(nextSnapshot as unknown as JsonValue)
        ? (nextSnapshot as unknown as JsonObject)
        : null;
      setConfigInspectSnapshot(normalizedSnapshot);
      setConfigDiffPreview(
        buildRedactedDiff(previousSnapshot, readDocumentToml(normalizedSnapshot)),
      );
      await validateConfigSurface();
      setNotice(`Config ${configMutationMode === "unset" ? "unset" : "mutation"} applied.`);
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setConfigBusy(false);
    }
  }

  async function migrateConfigSurface(): Promise<void> {
    setConfigBusy(true);
    setError(null);
    setNotice(null);
    try {
      const previousSnapshot = readDocumentToml(configInspectSnapshot);
      const response = await api.migrateConfig({
        path: emptyToUndefined(configInspectPath),
        show_secrets: false,
        backups: normalizedBackupCount(configBackups),
      });
      setConfigLastMutation(
        isJsonObject(response as unknown as JsonValue) ? (response as unknown as JsonObject) : null,
      );
      const nextSnapshot = await api.inspectConfig({
        path: emptyToUndefined(configInspectPath),
        show_secrets: false,
        backups: normalizedBackupCount(configBackups),
      });
      const normalizedSnapshot = isJsonObject(nextSnapshot as unknown as JsonValue)
        ? (nextSnapshot as unknown as JsonObject)
        : null;
      setConfigInspectSnapshot(normalizedSnapshot);
      setConfigDiffPreview(
        buildRedactedDiff(previousSnapshot, readDocumentToml(normalizedSnapshot)),
      );
      await validateConfigSurface();
      setNotice("Config migration completed.");
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setConfigBusy(false);
    }
  }

  async function recoverConfigSurface(): Promise<void> {
    const backup = parseInteger(configRecoverBackup);
    if (backup === null || backup <= 0) {
      setError("Recover backup index must be a positive integer.");
      return;
    }
    setConfigBusy(true);
    setError(null);
    setNotice(null);
    try {
      const previousSnapshot = readDocumentToml(configInspectSnapshot);
      const response = await api.recoverConfig({
        path: emptyToUndefined(configInspectPath),
        backup,
        backups: normalizedBackupCount(configBackups),
      });
      setConfigLastMutation(
        isJsonObject(response as unknown as JsonValue) ? (response as unknown as JsonObject) : null,
      );
      const nextSnapshot = await api.inspectConfig({
        path: emptyToUndefined(configInspectPath),
        show_secrets: false,
        backups: normalizedBackupCount(configBackups),
      });
      const normalizedSnapshot = isJsonObject(nextSnapshot as unknown as JsonValue)
        ? (nextSnapshot as unknown as JsonObject)
        : null;
      setConfigInspectSnapshot(normalizedSnapshot);
      setConfigDiffPreview(
        buildRedactedDiff(previousSnapshot, readDocumentToml(normalizedSnapshot)),
      );
      await validateConfigSurface();
      setNotice(`Recovered config from backup ${backup}.`);
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
      const [response, configuredResponse] = await Promise.all([
        api.listSecrets(configSecretsScope),
        api.listConfiguredSecrets(),
      ]);
      setConfigSecrets(toJsonObjectArray(response.secrets as unknown as JsonValue[]));
      setConfiguredSecrets(toJsonObjectArray(configuredResponse.secrets as unknown as JsonValue[]));
      setConfigSecretMetadata(null);
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setConfigBusy(false);
    }
  }

  async function loadSecretMetadata(secretKey?: string): Promise<void> {
    const resolvedSecretKey = (secretKey ?? configSecretKey).trim();
    if (resolvedSecretKey.length === 0) {
      setError("Secret key cannot be empty.");
      return;
    }
    setConfigBusy(true);
    setError(null);
    try {
      const response = await api.getSecretMetadata(configSecretsScope, resolvedSecretKey);
      setConfigSecretMetadata(
        isJsonObject(response.secret as unknown as JsonValue)
          ? (response.secret as unknown as JsonObject)
          : null,
      );
      setNotice("Secret metadata refreshed.");
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
        value_base64: encodeBase64(configSecretValue),
      });
      setNotice("Secret stored.");
      setConfigSecretValue("");
      await refreshSecrets();
      await loadSecretMetadata(configSecretKey);
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setConfigBusy(false);
    }
  }

  async function revealSecretValue(secretKey?: string): Promise<void> {
    const resolvedSecretKey = (secretKey ?? configSecretKey).trim();
    if (resolvedSecretKey.length === 0) {
      setError("Secret key cannot be empty.");
      return;
    }
    setConfigBusy(true);
    setError(null);
    setNotice(null);
    try {
      const response = await api.revealSecret({
        scope: configSecretsScope,
        key: resolvedSecretKey,
        reveal: true,
      });
      setConfigSecretReveal(
        isJsonObject(response as unknown as JsonValue) ? (response as unknown as JsonObject) : null,
      );
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
        key: configSecretKey.trim(),
      });
      setNotice("Secret deleted.");
      setConfigSecretMetadata(null);
      setConfigSecretReveal(null);
      await refreshSecrets();
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setConfigBusy(false);
    }
  }

  async function loadConfiguredSecret(secretId: string): Promise<void> {
    if (secretId.trim().length === 0) {
      setError("Configured secret id cannot be empty.");
      return;
    }
    setConfigBusy(true);
    setError(null);
    try {
      const response = await api.getConfiguredSecret(secretId.trim());
      setConfiguredSecretDetail(
        isJsonObject(response.secret as unknown as JsonValue)
          ? (response.secret as unknown as JsonObject)
          : null,
      );
      setNotice("Configured secret detail refreshed.");
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setConfigBusy(false);
    }
  }

  async function planConfigReload(): Promise<void> {
    setConfigBusy(true);
    setError(null);
    try {
      const response = await api.planConfigReload({
        path: emptyToUndefined(configInspectPath),
      });
      setConfigReloadPlan(
        isJsonObject(response as unknown as JsonValue) ? (response as unknown as JsonObject) : null,
      );
      setNotice("Reload plan generated.");
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setConfigBusy(false);
    }
  }

  async function applyConfigReload(dryRun = false): Promise<void> {
    setConfigBusy(true);
    setError(null);
    try {
      const response = await api.applyConfigReload({
        path: emptyToUndefined(configInspectPath),
        dry_run: dryRun,
      });
      setConfigReloadResult(
        isJsonObject(response as unknown as JsonValue) ? (response as unknown as JsonObject) : null,
      );
      const normalizedPlan = isJsonObject(response.plan as unknown as JsonValue)
        ? (response.plan as unknown as JsonObject)
        : null;
      setConfigReloadPlan(normalizedPlan);
      await refreshConfigSurface();
      setNotice(dryRun ? "Reload dry-run completed." : "Reload apply finished.");
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setConfigBusy(false);
    }
  }

  function resetConfigDomain(): void {
    setConfigBusy(false);
    setConfigInspectPath("palyra.toml");
    setConfigBackups("3");
    setConfigMutationMode("set");
    setConfigInspectSnapshot(null);
    setConfigMutationKey("");
    setConfigMutationValue("");
    setConfigValidation(null);
    setConfigLastMutation(null);
    setConfigDiffPreview(null);
    setConfigRecoverBackup("1");
    setConfigDeploymentPosture(null);
    setConfigSecretsScope("global");
    setConfigSecrets([]);
    setConfigSecretKey("");
    setConfigSecretMetadata(null);
    setConfigSecretValue("");
    setConfigSecretReveal(null);
    setConfiguredSecrets([]);
    setConfiguredSecretDetail(null);
    setConfigReloadPlan(null);
    setConfigReloadResult(null);
  }

  return {
    configBusy,
    configInspectPath,
    setConfigInspectPath,
    configBackups,
    setConfigBackups,
    configMutationMode,
    setConfigMutationMode,
    configInspectSnapshot,
    configMutationKey,
    setConfigMutationKey,
    configMutationValue,
    setConfigMutationValue,
    configValidation,
    configLastMutation,
    configDiffPreview,
    configRecoverBackup,
    setConfigRecoverBackup,
    configDeploymentPosture,
    configSecretsScope,
    setConfigSecretsScope,
    configSecrets,
    configSecretKey,
    setConfigSecretKey,
    configSecretMetadata,
    configSecretValue,
    setConfigSecretValue,
    configSecretReveal,
    configuredSecrets,
    configuredSecretDetail,
    configReloadPlan,
    configReloadResult,
    refreshConfigSurface,
    inspectConfigSurface,
    validateConfigSurface,
    mutateConfigSurface,
    migrateConfigSurface,
    recoverConfigSurface,
    refreshSecrets,
    loadSecretMetadata,
    setSecretValue,
    revealSecretValue,
    deleteSecretValue,
    loadConfiguredSecret,
    planConfigReload,
    applyConfigReload,
    resetConfigDomain,
  };
}

function normalizedBackupCount(raw: string): number {
  const parsed = parseInteger(raw);
  if (parsed === null || parsed <= 0) {
    return 3;
  }
  return Math.min(parsed, 16);
}

function readDocumentToml(snapshot: JsonObject | null): string {
  const value = snapshot?.document_toml;
  return typeof value === "string" ? value : "";
}

function buildRedactedDiff(previous: string, next: string): string {
  if (previous === next) {
    return "No redacted diff. Snapshot is unchanged.";
  }
  const previousLines = previous.split(/\r?\n/u);
  const nextLines = next.split(/\r?\n/u);
  const diff: string[] = [];
  const maxLines = Math.max(previousLines.length, nextLines.length);
  for (let index = 0; index < maxLines; index += 1) {
    const before = previousLines[index];
    const after = nextLines[index];
    if (before === after) {
      continue;
    }
    if (before !== undefined) {
      diff.push(`- ${before}`);
    }
    if (after !== undefined) {
      diff.push(`+ ${after}`);
    }
    if (diff.length >= 80) {
      diff.push("... diff truncated ...");
      break;
    }
  }
  return diff.join("\n");
}
