import { useState } from "react";

import type { AuthHealthProfile, AuthProfileView } from "../../consoleApi";
import {
  ActionButton,
  AppForm,
  CheckboxField,
  SelectField,
  TextInputField,
} from "../components/ui";
import {
  WorkspaceMetricCard,
  WorkspacePageHeader,
  WorkspaceSectionCard,
  WorkspaceStatusChip,
} from "../components/workspace/WorkspaceChrome";
import {
  WorkspaceEmptyState,
  WorkspaceInlineNotice,
  WorkspaceRedactedValue,
  WorkspaceTable,
  workspaceToneForState,
} from "../components/workspace/WorkspacePatterns";
import { readProviderRegistrySummary } from "../providerRegistry";
import { formatUnixMs, readNumber, type JsonObject } from "../shared";
import {
  AUTH_PROVIDER_CONFIGS,
  providerConfig,
  providerKeyForProfile,
} from "../hooks/useAuthDomain";
import type { ConsoleAppState } from "../useConsoleAppState";

type AuthSectionProps = {
  app: Pick<
    ConsoleAppState,
    | "authBusy"
    | "authPolling"
    | "authProfiles"
    | "authHealth"
    | "authProviderState"
    | "authProviderStates"
    | "authProviderProbeMode"
    | "authProviderProbeResults"
    | "diagnosticsSnapshot"
    | "authApiKeyDraft"
    | "setAuthApiKeyDraft"
    | "authOAuthDraft"
    | "setAuthOAuthDraft"
    | "authActiveOauthAttempt"
    | "authOauthCallbackState"
    | "refreshAuth"
    | "connectApiKeyProfile"
    | "startProviderOAuth"
    | "reconnectProviderProfile"
    | "refreshProviderProfile"
    | "revokeProviderProfile"
    | "setDefaultProviderProfile"
    | "probeProvider"
    | "checkOpenAiCallbackState"
    | "openActiveOauthWindow"
    | "prepareApiKeyRotation"
    | "cancelApiKeyRotation"
    | "revealSensitiveValues"
  >;
};

export function AuthSection({ app }: AuthSectionProps) {
  const [selectedProfileId, setSelectedProfileId] = useState("");
  const profiles = [...app.authProfiles].sort(
    (left, right) =>
      Number(right.profile_id === app.authProviderState?.default_profile_id) -
        Number(left.profile_id === app.authProviderState?.default_profile_id) ||
      right.updated_at_unix_ms - left.updated_at_unix_ms ||
      left.profile_name.localeCompare(right.profile_name),
  );
  const healthById = new Map(
    (app.authHealth?.profiles ?? []).map((profile) => [profile.profile_id, profile]),
  );
  const selectedProfile =
    profiles.find((profile) => profile.profile_id === selectedProfileId) ??
    profiles.find((profile) => profile.profile_id === app.authProviderState?.default_profile_id) ??
    profiles[0] ??
    null;
  const summary = summarizeAuthHealth(app.authHealth?.profiles ?? []);
  const providerRegistry = readProviderRegistrySummary(app.diagnosticsSnapshot);
  const providerCount = new Set(profiles.map((profile) => providerKeyForProfile(profile))).size;
  const credentialCount = providerRegistry?.credentials.length ?? 0;
  const credentialAttentionCount =
    providerRegistry?.credentials.filter(
      (credential) => credential.availabilityState !== "available",
    ).length ?? 0;
  const runtimeCredentialLabel =
    providerRegistry?.credentialId ?? providerRegistry?.credentialSource ?? "n/a";

  return (
    <main className="workspace-page">
      <WorkspacePageHeader
        eyebrow="Settings"
        title="Profiles"
        description="Provider auth now reads like settings: profile inventory first, connection flows second, and explicit recovery actions only where the backend supports them."
        status={
          <>
            <WorkspaceStatusChip
              tone={workspaceToneForState(app.authProviderState?.state ?? "unknown")}
            >
              Provider: {app.authProviderState?.state ?? "unknown"}
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={profiles.length > 0 ? "success" : "default"}>
              {profiles.length} profiles
            </WorkspaceStatusChip>
          </>
        }
        actions={
          <ActionButton
            type="button"
            variant="primary"
            onPress={() => void app.refreshAuth()}
            isDisabled={app.authBusy}
          >
            {app.authBusy ? "Refreshing..." : "Refresh profiles"}
          </ActionButton>
        }
      />

      <section className="workspace-metric-grid workspace-metric-grid--compact">
        <WorkspaceMetricCard
          label="Default profile"
          value={app.authProviderState?.default_profile_id ?? "none"}
          detail="Explicit default selection remains visible."
          tone={app.authProviderState?.default_profile_id ? "success" : "default"}
        />
        <WorkspaceMetricCard
          label="Healthy"
          value={summary.ok + summary.staticCount}
          detail={`${summary.expiring} expiring, ${summary.expired} expired, ${summary.missing} missing`}
          tone={
            summary.expired > 0 || summary.missing > 0
              ? "danger"
              : summary.expiring > 0
                ? "warning"
                : "success"
          }
        />
        <WorkspaceMetricCard
          label="Providers"
          value={providerCount}
          detail={
            providerRegistry === null
              ? "Profile inventory spans every provider kind currently published by auth."
              : `${credentialCount} runtime credential bindings, ${credentialAttentionCount} needing attention.`
          }
          tone={providerCount > 1 ? "accent" : "default"}
        />
        <WorkspaceMetricCard
          label="OAuth bootstrap"
          value={app.authProviderState?.bootstrap_supported ? "Ready" : "Unavailable"}
          detail={app.authProviderState?.note ?? "Provider contract posture."}
          tone={app.authProviderState?.bootstrap_supported ? "success" : "default"}
        />
      </section>

      <section className="workspace-aside-grid">
        <div className="workspace-stack">
          <WorkspaceSectionCard
            title="Profile inventory"
            description="Readable list first, with scope and health visible before any edit flow."
          >
            {profiles.length === 0 ? (
              <WorkspaceEmptyState
                title="No profiles configured"
                description="Create an API-key or OAuth profile to make provider auth available to chat and agent flows."
              />
            ) : (
              <WorkspaceTable
                ariaLabel="Provider auth profiles"
                columns={["Profile", "Provider", "Scope", "Credential", "State", "Actions"]}
              >
                {profiles.map((profile) => {
                  const health = healthById.get(profile.profile_id);
                  const state =
                    health?.state ?? (profile.credential.type === "oauth" ? "managed" : "static");
                  const scopeLabel =
                    profile.scope.kind === "agent"
                      ? `agent:${profile.scope.agent_id ?? "unassigned"}`
                      : "global";
                  return (
                    <tr key={profile.profile_id}>
                      <td>
                        <div className="workspace-table__meta">
                          <strong>{profile.profile_name}</strong>
                          <span className="chat-muted">{profile.profile_id}</span>
                        </div>
                      </td>
                      <td>{providerConfig(providerKeyForProfile(profile)).label}</td>
                      <td>{scopeLabel}</td>
                      <td>{profile.credential.type === "oauth" ? "OAuth" : "API key"}</td>
                      <td>
                        <div className="workspace-table__status">
                          <WorkspaceStatusChip tone={workspaceToneForState(state)}>
                            {state}
                          </WorkspaceStatusChip>
                        </div>
                      </td>
                      <td>
                        <div className="workspace-table__actions">
                          <ActionButton
                            aria-label={`Inspect ${profile.profile_name}`}
                            type="button"
                            variant="secondary"
                            onPress={() => setSelectedProfileId(profile.profile_id)}
                          >
                            Inspect
                          </ActionButton>
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </WorkspaceTable>
            )}
          </WorkspaceSectionCard>
        </div>

        <div className="workspace-stack">
          <WorkspaceSectionCard
            title="Selected profile"
            description="Inspect scope, vault references, expiry, and safe recovery actions without opening raw config."
          >
            {selectedProfile === null ? (
              <WorkspaceEmptyState
                title="Nothing selected"
                description="Choose a profile from the inventory to inspect details and available actions."
                compact
              />
            ) : (
              <SelectedProfileCard
                app={app}
                profile={selectedProfile}
                health={healthById.get(selectedProfile.profile_id)}
              />
            )}
          </WorkspaceSectionCard>

          {app.authActiveOauthAttempt !== null ? (
            <WorkspaceInlineNotice
              title="OAuth callback"
              tone={workspaceToneForState(app.authOauthCallbackState?.state ?? "pending")}
            >
              <p>{app.authOauthCallbackState?.message ?? app.authActiveOauthAttempt.message}</p>
              <div className="workspace-inline">
                <ActionButton
                  type="button"
                  variant="secondary"
                  onPress={() => app.openActiveOauthWindow()}
                  isDisabled={app.authBusy}
                >
                  Reopen authorization
                </ActionButton>
                <ActionButton
                  type="button"
                  variant="secondary"
                  onPress={() =>
                    void app.checkOpenAiCallbackState(app.authActiveOauthAttempt?.attempt_id)
                  }
                  isDisabled={app.authPolling}
                >
                  {app.authPolling ? "Checking..." : "Poll callback"}
                </ActionButton>
              </div>
            </WorkspaceInlineNotice>
          ) : null}

          <WorkspaceSectionCard
            title="Provider registry"
            description="Auth inventory is now grounded in the same provider registry the runtime uses for routing and failover."
          >
            {providerRegistry === null ? (
              <WorkspaceEmptyState
                compact
                title="No provider diagnostics loaded"
                description="Diagnostics publish provider health, discovery, and registry bindings when available."
              />
            ) : (
              <div className="workspace-stack">
                <dl className="workspace-key-value-grid">
                  <div>
                    <dt>Runtime provider</dt>
                    <dd>{providerRegistry.providerKind}</dd>
                  </div>
                  <div>
                    <dt>Default chat model</dt>
                    <dd>{providerRegistry.defaultChatModelId ?? "n/a"}</dd>
                  </div>
                  <div>
                    <dt>Runtime credential</dt>
                    <dd>{runtimeCredentialLabel}</dd>
                  </div>
                  <div>
                    <dt>Failover</dt>
                    <dd>{providerRegistry.failoverEnabled ? "enabled" : "disabled"}</dd>
                  </div>
                  <div>
                    <dt>Response cache</dt>
                    <dd>{providerRegistry.responseCacheEnabled ? "enabled" : "disabled"}</dd>
                  </div>
                  <div>
                    <dt>Credential entries</dt>
                    <dd>{providerRegistry.credentials.length}</dd>
                  </div>
                </dl>
                <WorkspaceTable
                  ariaLabel="Provider registry health"
                  columns={["Provider", "Kind", "Health", "Discovery", "Binding", "Actions"]}
                >
                  {providerRegistry.providers.map((provider) => {
                    const probe = app.authProviderProbeResults[provider.providerId];
                    return (
                      <tr key={provider.providerId}>
                        <td>
                          <div className="workspace-table__meta">
                            <strong>{provider.displayName}</strong>
                            <span className="chat-muted">
                              {probe?.checked_at_unix_ms
                                ? `Checked ${formatUnixMs(probe.checked_at_unix_ms)}`
                                : "No active probe yet"}
                            </span>
                          </div>
                        </td>
                        <td>{provider.kind}</td>
                        <td>
                          <div className="workspace-stack">
                            <div className="workspace-table__status">
                              <WorkspaceStatusChip
                                tone={workspaceToneForState(provider.healthState)}
                              >
                                {probe?.state ?? provider.healthState}
                              </WorkspaceStatusChip>
                            </div>
                            {provider.lastError !== undefined ? (
                              <small className="text-muted">
                                {provider.lastError.class} ·{" "}
                                {provider.lastError.recommendedAction ?? "inspect"}
                              </small>
                            ) : null}
                          </div>
                        </td>
                        <td>
                          <div className="workspace-table__meta">
                            <strong>
                              {probe?.discovery_source === "live"
                                ? "live"
                                : provider.discoveryStatus}
                            </strong>
                            <span className="chat-muted">
                              {(probe?.discovered_model_ids ?? provider.discoveredModelIds).join(
                                ", ",
                              ) || "No models"}
                            </span>
                            <span className="chat-muted">
                              {provider.avgLatencyMs > 0
                                ? `${provider.avgLatencyMs} ms avg`
                                : "No runtime latency yet"}
                            </span>
                          </div>
                        </td>
                        <td>
                          <div className="workspace-table__meta">
                            <strong>{provider.credentialId ?? "unbound"}</strong>
                            <span className="chat-muted">
                              {provider.authProfileId ??
                                provider.credentialSource ??
                                "registry only"}
                            </span>
                          </div>
                        </td>
                        <td>
                          <div className="workspace-table__actions">
                            <ActionButton
                              type="button"
                              variant="secondary"
                              onPress={() => void app.probeProvider(provider.providerId, false)}
                              isDisabled={app.authBusy}
                            >
                              Test connection
                            </ActionButton>
                            <ActionButton
                              type="button"
                              variant="secondary"
                              onPress={() => void app.probeProvider(provider.providerId, true)}
                              isDisabled={app.authBusy}
                            >
                              Discover models
                            </ActionButton>
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                </WorkspaceTable>
                {providerRegistry.credentials.length > 0 ? (
                  <WorkspaceTable
                    ariaLabel="Credential registry health"
                    columns={[
                      "Credential",
                      "Provider",
                      "Availability",
                      "Capabilities",
                      "Activity",
                      "Last failure",
                    ]}
                  >
                    {providerRegistry.credentials.map((credential) => (
                      <tr key={credential.credentialId}>
                        <td>
                          <div className="workspace-table__meta">
                            <strong>{credential.credentialId}</strong>
                            <span className="chat-muted">
                              {credential.authProfileId ??
                                credential.credentialSource ??
                                "runtime-only binding"}
                            </span>
                          </div>
                        </td>
                        <td>
                          <div className="workspace-table__meta">
                            <strong>{credential.providerId}</strong>
                            <span className="chat-muted">{credential.providerKind}</span>
                          </div>
                        </td>
                        <td>
                          <div className="workspace-stack">
                            <div className="workspace-table__status">
                              <WorkspaceStatusChip
                                tone={toneForCredentialAvailability(credential.availabilityState)}
                              >
                                {credential.availabilityState}
                              </WorkspaceStatusChip>
                            </div>
                            <small className="text-muted">
                              {credential.healthState}
                              {credential.healthMessage ? ` · ${credential.healthMessage}` : ""}
                            </small>
                          </div>
                        </td>
                        <td>{credential.capabilitySummary.join(", ") || "n/a"}</td>
                        <td>
                          <div className="workspace-table__meta">
                            <strong>{formatUnixMs(credential.lastUsedAtUnixMs)}</strong>
                            <span className="chat-muted">
                              {describeCredentialActivity(
                                credential.lastSuccessAtUnixMs,
                                credential.lastErrorAtUnixMs,
                              )}
                            </span>
                          </div>
                        </td>
                        <td>
                          {credential.lastError === undefined ? (
                            <span className="text-muted">No failure recorded</span>
                          ) : (
                            <div className="workspace-table__meta">
                              <strong>{credential.lastError.class}</strong>
                              <span className="chat-muted">
                                {credential.lastError.recommendedAction ?? "inspect"}
                                {credential.lastError.message
                                  ? ` · ${credential.lastError.message}`
                                  : ""}
                              </span>
                            </div>
                          )}
                        </td>
                      </tr>
                    ))}
                  </WorkspaceTable>
                ) : null}
                {Object.values(app.authProviderProbeResults).length > 0 ? (
                  <WorkspaceTable
                    ariaLabel="Provider probe results"
                    columns={["Provider", "Mode", "State", "Latency", "Message"]}
                  >
                    {Object.values(app.authProviderProbeResults)
                      .sort((left, right) => left.provider_id.localeCompare(right.provider_id))
                      .map((probe) => (
                        <tr key={probe.provider_id}>
                          <td>{probe.provider_id}</td>
                          <td>{app.authProviderProbeMode ?? "probe"}</td>
                          <td>
                            <WorkspaceStatusChip tone={workspaceToneForState(probe.state)}>
                              {probe.state}
                            </WorkspaceStatusChip>
                          </td>
                          <td>
                            {probe.latency_ms === undefined ? "n/a" : `${probe.latency_ms} ms`}
                          </td>
                          <td>{probe.message}</td>
                        </tr>
                      ))}
                  </WorkspaceTable>
                ) : null}
              </div>
            )}
          </WorkspaceSectionCard>
        </div>
      </section>

      <section className="workspace-two-column">
        <ApiKeyForm app={app} />
        <OAuthForm app={app} />
      </section>
    </main>
  );
}

function summarizeAuthHealth(profiles: AuthHealthProfile[]) {
  return profiles.reduce(
    (summary, profile) => {
      if (profile.state === "ok") summary.ok += 1;
      else if (profile.state === "expiring") summary.expiring += 1;
      else if (profile.state === "expired") summary.expired += 1;
      else if (profile.state === "missing") summary.missing += 1;
      else if (profile.state === "static") summary.staticCount += 1;
      return summary;
    },
    { ok: 0, expiring: 0, expired: 0, missing: 0, staticCount: 0 },
  );
}

function toneForCredentialAvailability(
  state: string,
): Parameters<typeof WorkspaceStatusChip>[0]["tone"] {
  switch (state) {
    case "available":
      return "success";
    case "rate_limited":
    case "provider_degraded":
    case "degraded":
      return "warning";
    case "auth_invalid":
    case "auth_expired":
    case "permission_denied":
    case "missing_auth":
      return "danger";
    default:
      return workspaceToneForState(state);
  }
}

function describeCredentialActivity(
  lastSuccessAtUnixMs: number | undefined,
  lastErrorAtUnixMs: number | undefined,
) {
  if (lastErrorAtUnixMs !== undefined && lastSuccessAtUnixMs !== undefined) {
    return `ok ${formatUnixMs(lastSuccessAtUnixMs)} · err ${formatUnixMs(lastErrorAtUnixMs)}`;
  }
  if (lastSuccessAtUnixMs !== undefined) {
    return `ok ${formatUnixMs(lastSuccessAtUnixMs)}`;
  }
  if (lastErrorAtUnixMs !== undefined) {
    return `err ${formatUnixMs(lastErrorAtUnixMs)}`;
  }
  return "No success or failure recorded";
}

type SelectedProfileCardProps = {
  app: AuthSectionProps["app"];
  profile: AuthProfileView;
  health: AuthHealthProfile | undefined;
};

function SelectedProfileCard({ app, profile, health }: SelectedProfileCardProps) {
  const providerKind = providerKeyForProfile(profile);
  const providerState = app.authProviderStates[providerKind] ?? null;
  const providerLabel = providerConfig(providerKind).label;
  const isDefault = profile.profile_id === providerState?.default_profile_id;
  const oauthCredential = profile.credential.type === "oauth" ? profile.credential : null;

  return (
    <div className="workspace-stack">
      <div className="workspace-inline">
        <WorkspaceStatusChip tone={workspaceToneForState(health?.state ?? "unknown")}>
          {health?.state ?? (profile.credential.type === "oauth" ? "managed" : "static")}
        </WorkspaceStatusChip>
        <WorkspaceStatusChip tone={profile.scope.kind === "agent" ? "accent" : "default"}>
          {profile.scope.kind === "agent"
            ? `agent:${profile.scope.agent_id ?? "unassigned"}`
            : "global"}
        </WorkspaceStatusChip>
        {isDefault && <WorkspaceStatusChip tone="success">Default</WorkspaceStatusChip>}
      </div>

      <dl className="workspace-key-value-grid">
        <div>
          <dt>Profile id</dt>
          <dd>{profile.profile_id}</dd>
        </div>
        <div>
          <dt>Provider</dt>
          <dd>{providerLabel}</dd>
        </div>
        <div>
          <dt>Updated</dt>
          <dd>{formatUnixMs(profile.updated_at_unix_ms)}</dd>
        </div>
        <div>
          <dt>Created</dt>
          <dd>{formatUnixMs(profile.created_at_unix_ms)}</dd>
        </div>
        <div>
          <dt>Expiry</dt>
          <dd>
            {formatUnixMs(
              health?.expires_at_unix_ms ??
                (profile.credential.type === "oauth"
                  ? profile.credential.expires_at_unix_ms
                  : undefined),
            )}
          </dd>
        </div>
      </dl>

      {profile.credential.type === "api_key" ? (
        <WorkspaceRedactedValue
          label="API key vault reference"
          value={profile.credential.api_key_vault_ref}
          sensitive
          revealed={app.revealSensitiveValues}
          allowCopy
          hint="Credential material stays in Vault. Rotation writes a new key instead of editing config."
        />
      ) : (
        <div className="workspace-stack">
          <WorkspaceRedactedValue
            label="Access token vault ref"
            value={oauthCredential?.access_token_vault_ref}
            sensitive
            revealed={app.revealSensitiveValues}
            allowCopy
          />
          <WorkspaceRedactedValue
            label="Refresh token vault ref"
            value={oauthCredential?.refresh_token_vault_ref}
            sensitive
            revealed={app.revealSensitiveValues}
            allowCopy
          />
          <WorkspaceRedactedValue
            label="Client secret vault ref"
            value={oauthCredential?.client_secret_vault_ref}
            sensitive
            revealed={app.revealSensitiveValues}
            allowCopy
            placeholder="No client secret vault ref published"
          />
          <dl className="workspace-key-value-grid">
            <div>
              <dt>Token endpoint</dt>
              <dd>{oauthCredential?.token_endpoint ?? "n/a"}</dd>
            </div>
            <div>
              <dt>Scopes</dt>
              <dd>{oauthCredential?.scopes.join(", ") || "n/a"}</dd>
            </div>
            <div>
              <dt>Refresh failures</dt>
              <dd>
                {readNumber(
                  (oauthCredential?.refresh_state ?? {}) as JsonObject,
                  "failure_count",
                ) ?? 0}
              </dd>
            </div>
            <div>
              <dt>Last success</dt>
              <dd>
                {formatUnixMs(
                  readNumber(
                    (oauthCredential?.refresh_state ?? {}) as JsonObject,
                    "last_success_unix_ms",
                  ),
                )}
              </dd>
            </div>
          </dl>
        </div>
      )}

      {health?.reason !== undefined && health.reason.trim().length > 0 ? (
        <WorkspaceInlineNotice title="Health note" tone={workspaceToneForState(health.state)}>
          <p>{health.reason}</p>
        </WorkspaceInlineNotice>
      ) : null}

      {providerState !== null ? (
        <div className="workspace-inline">
          {!isDefault && providerState?.default_selection_supported && (
            <ActionButton
              type="button"
              variant="secondary"
              onPress={() => void app.setDefaultProviderProfile(profile)}
              isDisabled={app.authBusy}
            >
              Set as default
            </ActionButton>
          )}
          {profile.credential.type === "oauth" ? (
            <>
              <ActionButton
                type="button"
                variant="secondary"
                onPress={() => void app.reconnectProviderProfile(profile)}
                isDisabled={app.authBusy || !providerState?.reconnect_supported}
              >
                Reconnect
              </ActionButton>
              <ActionButton
                type="button"
                variant="secondary"
                onPress={() => void app.refreshProviderProfile(profile)}
                isDisabled={app.authBusy}
              >
                Refresh token
              </ActionButton>
            </>
          ) : (
            <ActionButton
              type="button"
              variant="secondary"
              onPress={() => app.prepareApiKeyRotation(profile)}
              isDisabled={app.authBusy}
            >
              Rotate API key
            </ActionButton>
          )}
          <ActionButton
            type="button"
            variant="danger"
            onPress={() => void app.revokeProviderProfile(profile)}
            isDisabled={app.authBusy || !providerState?.revoke_supported}
          >
            Revoke
          </ActionButton>
        </div>
      ) : (
        <WorkspaceInlineNotice title="Provider actions unavailable" tone="default">
          <p>Interactive provider actions are not published for this provider kind yet.</p>
        </WorkspaceInlineNotice>
      )}
    </div>
  );
}

function ApiKeyForm({ app }: { app: AuthSectionProps["app"] }) {
  const selectedProvider = providerConfig(app.authApiKeyDraft.provider);
  return (
    <WorkspaceSectionCard
      title={
        app.authApiKeyDraft.profileId.trim().length > 0 ? "Rotate API key" : "Connect via API key"
      }
      description={
        app.authApiKeyDraft.profileId.trim().length > 0
          ? `Updating profile ${app.authApiKeyDraft.profileId}.`
          : `Create a new ${selectedProvider.label} auth profile backed by a Vault-stored API key.`
      }
      actions={
        app.authApiKeyDraft.profileId.trim().length > 0 ? (
          <ActionButton
            type="button"
            variant="secondary"
            onPress={() => app.cancelApiKeyRotation()}
            isDisabled={app.authBusy}
          >
            Cancel rotation
          </ActionButton>
        ) : undefined
      }
    >
      <AppForm
        className="workspace-stack"
        onSubmit={(event) => {
          event.preventDefault();
          void app.connectApiKeyProfile();
        }}
      >
        <div className="workspace-form-grid">
          <SelectField
            label="Provider"
            value={app.authApiKeyDraft.provider}
            onChange={(value) =>
              app.setAuthApiKeyDraft((current) => ({
                ...current,
                provider: value === "anthropic" || value === "minimax" ? value : "openai",
              }))
            }
            options={AUTH_PROVIDER_CONFIGS.map((provider) => ({
              key: provider.key,
              label: provider.label,
            }))}
            disabled={app.authApiKeyDraft.profileId.trim().length > 0}
          />
          <TextInputField
            label="Profile name"
            value={app.authApiKeyDraft.profileName}
            onChange={(value) =>
              app.setAuthApiKeyDraft((current) => ({ ...current, profileName: value }))
            }
            placeholder="default-openai"
          />
          <SelectField
            label="Scope"
            value={app.authApiKeyDraft.scopeKind}
            onChange={(value) =>
              app.setAuthApiKeyDraft((current) => ({
                ...current,
                scopeKind: value === "agent" ? "agent" : "global",
              }))
            }
            options={[
              { key: "global", label: "global" },
              { key: "agent", label: "agent" },
            ]}
          />
          <CheckboxField
            checked={app.authApiKeyDraft.setDefault}
            label="Set as default"
            onChange={(checked) =>
              app.setAuthApiKeyDraft((current) => ({ ...current, setDefault: checked }))
            }
          />
        </div>
        {app.authApiKeyDraft.scopeKind === "agent" && (
          <TextInputField
            label="Agent id"
            value={app.authApiKeyDraft.agentId}
            onChange={(value) =>
              app.setAuthApiKeyDraft((current) => ({ ...current, agentId: value }))
            }
          />
        )}
        <TextInputField
          label="API key"
          type="password"
          autoComplete="off"
          value={app.authApiKeyDraft.apiKey}
          onChange={(value) => app.setAuthApiKeyDraft((current) => ({ ...current, apiKey: value }))}
        />
        <div className="workspace-inline">
          <ActionButton type="submit" variant="primary" isDisabled={app.authBusy}>
            {app.authBusy
              ? "Submitting..."
              : app.authApiKeyDraft.profileId.trim().length > 0
                ? "Rotate API key"
                : `Create ${selectedProvider.label} profile`}
          </ActionButton>
        </div>
      </AppForm>
    </WorkspaceSectionCard>
  );
}

function OAuthForm({ app }: { app: AuthSectionProps["app"] }) {
  const selectedProvider = providerConfig(app.authOAuthDraft.provider);
  return (
    <WorkspaceSectionCard
      title="Connect via OAuth"
      description="Bootstrap OAuth from the dashboard and keep callback state visible without exposing secrets."
    >
      <AppForm
        className="workspace-stack"
        onSubmit={(event) => {
          event.preventDefault();
          void app.startProviderOAuth();
        }}
      >
        <div className="workspace-form-grid">
          <SelectField
            label="Provider"
            value={app.authOAuthDraft.provider}
            onChange={(value) => {
              const provider = value === "minimax" || value === "anthropic" ? value : "openai";
              app.setAuthOAuthDraft((current) => ({
                ...current,
                provider,
                scopes: providerConfig(provider).defaultOAuthScopes,
              }));
            }}
            options={AUTH_PROVIDER_CONFIGS.filter((provider) => provider.oauthSupported).map(
              (provider) => ({ key: provider.key, label: provider.label }),
            )}
          />
          <TextInputField
            label="Profile name"
            value={app.authOAuthDraft.profileName}
            onChange={(value) =>
              app.setAuthOAuthDraft((current) => ({ ...current, profileName: value }))
            }
            placeholder={
              app.authOAuthDraft.provider === "minimax"
                ? "default-minimax-oauth"
                : "default-openai-oauth"
            }
          />
          <SelectField
            label="Scope"
            value={app.authOAuthDraft.scopeKind}
            onChange={(value) =>
              app.setAuthOAuthDraft((current) => ({
                ...current,
                scopeKind: value === "agent" ? "agent" : "global",
              }))
            }
            options={[
              { key: "global", label: "global" },
              { key: "agent", label: "agent" },
            ]}
          />
          <CheckboxField
            checked={app.authOAuthDraft.setDefault}
            label="Set as default"
            onChange={(checked) =>
              app.setAuthOAuthDraft((current) => ({ ...current, setDefault: checked }))
            }
          />
        </div>
        {app.authOAuthDraft.scopeKind === "agent" && (
          <TextInputField
            label="Agent id"
            value={app.authOAuthDraft.agentId}
            onChange={(value) =>
              app.setAuthOAuthDraft((current) => ({ ...current, agentId: value }))
            }
          />
        )}
        <div className="workspace-form-grid">
          <TextInputField
            label="Client id"
            value={app.authOAuthDraft.clientId}
            onChange={(value) =>
              app.setAuthOAuthDraft((current) => ({ ...current, clientId: value }))
            }
          />
          {selectedProvider.oauthRequiresClientSecret && (
            <TextInputField
              label="Client secret"
              type="password"
              autoComplete="off"
              value={app.authOAuthDraft.clientSecret}
              onChange={(value) =>
                app.setAuthOAuthDraft((current) => ({ ...current, clientSecret: value }))
              }
            />
          )}
          <TextInputField
            label="Scopes"
            value={app.authOAuthDraft.scopes}
            onChange={(value) =>
              app.setAuthOAuthDraft((current) => ({ ...current, scopes: value }))
            }
            placeholder={selectedProvider.defaultOAuthScopes}
          />
        </div>
        <div className="workspace-inline">
          <ActionButton type="submit" variant="primary" isDisabled={app.authBusy}>
            {app.authBusy ? "Starting..." : `Start ${selectedProvider.label} OAuth`}
          </ActionButton>
          {app.authActiveOauthAttempt !== null && (
            <ActionButton
              type="button"
              variant="secondary"
              onPress={() => app.openActiveOauthWindow()}
              isDisabled={app.authBusy}
            >
              Reopen authorization
            </ActionButton>
          )}
          {app.authActiveOauthAttempt !== null && (
            <ActionButton
              type="button"
              variant="secondary"
              onPress={() =>
                void app.checkOpenAiCallbackState(app.authActiveOauthAttempt?.attempt_id)
              }
              isDisabled={app.authPolling}
            >
              {app.authPolling ? "Checking..." : "Poll callback"}
            </ActionButton>
          )}
        </div>
      </AppForm>
    </WorkspaceSectionCard>
  );
}
