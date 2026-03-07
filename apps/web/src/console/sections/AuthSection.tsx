import { useEffect, useState } from "react";

import type { AuthHealthProfile, AuthProfileView, JsonValue } from "../../consoleApi";
import type { ConsoleAppState } from "../useConsoleAppState";
import { ConsoleSectionHeader } from "../components/ConsoleSectionHeader";
import { isJsonObject, readString } from "../shared";

type AuthSectionProps = {
  app: Pick<
    ConsoleAppState,
    | "authBusy"
    | "authPolling"
    | "authProfiles"
    | "authHealth"
    | "authProviderState"
    | "authApiKeyDraft"
    | "setAuthApiKeyDraft"
    | "authOAuthDraft"
    | "setAuthOAuthDraft"
    | "authActiveOauthAttempt"
    | "authOauthCallbackState"
    | "refreshAuth"
    | "connectOpenAiApiKey"
    | "startOpenAiOAuth"
    | "reconnectOpenAiProfile"
    | "refreshOpenAiProfile"
    | "revokeOpenAiProfile"
    | "setOpenAiDefaultProfile"
    | "checkOpenAiCallbackState"
    | "openActiveOauthWindow"
    | "prepareApiKeyRotation"
    | "cancelApiKeyRotation"
  >;
};

export function AuthSection({ app }: AuthSectionProps) {
  const [selectedProfileId, setSelectedProfileId] = useState("");
  const healthByProfileId = new Map(app.authHealth?.profiles.map((profile) => [profile.profile_id, profile]));
  const healthSummary = summarizeAuthHealth(app.authHealth?.profiles ?? []);
  const expiryDistribution = readMetricsObject(app.authHealth?.expiry_distribution ?? null);
  const refreshMetrics = readMetricsObject(app.authHealth?.refresh_metrics ?? null);
  const openAiRefreshMetrics = readProviderRefreshMetrics(refreshMetrics, "openai");
  const activeAttemptPending = app.authOauthCallbackState?.state === "pending";
  const selectedProfile = app.authProfiles.find((profile) => profile.profile_id === selectedProfileId) ?? null;
  const selectedHealth = selectedProfile === null ? undefined : healthByProfileId.get(selectedProfile.profile_id);

  useEffect(() => {
    if (app.authProfiles.length === 0) {
      setSelectedProfileId("");
      return;
    }
    if (app.authProfiles.some((profile) => profile.profile_id === selectedProfileId)) {
      return;
    }
    setSelectedProfileId(app.authProviderState?.default_profile_id ?? app.authProfiles[0]?.profile_id ?? "");
  }, [app.authProfiles, app.authProviderState?.default_profile_id, selectedProfileId]);

  return (
    <main className="console-card">
      <ConsoleSectionHeader
        title="OpenAI and Auth"
        description="Connect OpenAI directly from the dashboard with API keys or OAuth, monitor health, and operate reconnect or revoke flows without raw config edits."
        actions={(
          <button type="button" onClick={() => void app.refreshAuth()} disabled={app.authBusy}>
            {app.authBusy ? "Refreshing..." : "Refresh auth"}
          </button>
        )}
      />

      <section className="console-grid-4 console-summary-grid">
        <article className="console-subpanel auth-summary-card">
          <h3>Provider state</h3>
          <div className="auth-summary-value">
            <span className={statusPillClass(app.authProviderState?.state ?? "missing")}>
              {app.authProviderState?.state ?? "unknown"}
            </span>
          </div>
          <p><strong>Default profile:</strong> {app.authProviderState?.default_profile_id ?? "none"}</p>
          <p><strong>Available profiles:</strong> {app.authProviderState?.available_profile_ids.length ?? 0}</p>
          <p><strong>OAuth bootstrap:</strong> {app.authProviderState?.bootstrap_supported ? "ready" : "unavailable"}</p>
          {app.authProviderState?.note !== undefined && app.authProviderState.note.trim().length > 0 && (
            <p className="chat-muted">{app.authProviderState.note}</p>
          )}
        </article>

        <article className="console-subpanel auth-summary-card">
          <h3>Health summary</h3>
          <div className="auth-summary-grid">
            <p><strong>Total:</strong> {healthSummary.total}</p>
            <p><strong>Healthy:</strong> {healthSummary.ok}</p>
            <p><strong>Expiring:</strong> {healthSummary.expiring}</p>
            <p><strong>Expired:</strong> {healthSummary.expired}</p>
            <p><strong>Missing:</strong> {healthSummary.missing}</p>
            <p><strong>Static:</strong> {healthSummary.staticCount}</p>
          </div>
        </article>

        <article className="console-subpanel auth-summary-card">
          <h3>Refresh metrics</h3>
          <p><strong>Attempts:</strong> {readMetricNumber(refreshMetrics, "attempts")}</p>
          <p><strong>Successes:</strong> {readMetricNumber(refreshMetrics, "successes")}</p>
          <p><strong>Failures:</strong> {readMetricNumber(refreshMetrics, "failures")}</p>
          {openAiRefreshMetrics !== null && (
            <p className="chat-muted">
              OpenAI provider attempts={readMetricNumber(openAiRefreshMetrics, "attempts")} successes=
              {readMetricNumber(openAiRefreshMetrics, "successes")} failures=
              {readMetricNumber(openAiRefreshMetrics, "failures")}
            </p>
          )}
        </article>

        <article className="console-subpanel auth-summary-card">
          <h3>OAuth callback</h3>
          {app.authActiveOauthAttempt === null || app.authOauthCallbackState === null ? (
            <p>No active OpenAI OAuth attempt.</p>
          ) : (
            <>
              <div className="auth-summary-value">
                <span className={statusPillClass(app.authOauthCallbackState.state)}>
                  {app.authOauthCallbackState.state}
                </span>
              </div>
              <p><strong>Attempt:</strong> {app.authActiveOauthAttempt.attempt_id}</p>
              <p><strong>Profile:</strong> {app.authActiveOauthAttempt.profile_id ?? "new profile"}</p>
              <p><strong>Message:</strong> {app.authOauthCallbackState.message}</p>
              <p><strong>Expires:</strong> {formatUnixMs(app.authOauthCallbackState.expires_at_unix_ms)}</p>
              <div className="console-inline-actions">
                <button type="button" className="secondary" onClick={() => app.openActiveOauthWindow()}>
                  Open authorization window
                </button>
                <button
                  type="button"
                  className="secondary"
                  onClick={() => void app.checkOpenAiCallbackState(app.authActiveOauthAttempt?.attempt_id)}
                  disabled={app.authPolling}
                >
                  {app.authPolling ? "Checking..." : "Check callback status"}
                </button>
              </div>
            </>
          )}
        </article>
      </section>

      <section className="console-grid-2">
        <article className="console-subpanel">
          <div className="console-subpanel__header">
            <div>
              <h3>{app.authApiKeyDraft.profileId.trim().length > 0 ? "Rotate API key" : "Connect via API key"}</h3>
              <p className="chat-muted">
                {app.authApiKeyDraft.profileId.trim().length > 0
                  ? `Updating profile ${app.authApiKeyDraft.profileId}.`
                  : "Create a new OpenAI auth profile backed by Vault-stored API key material."}
              </p>
            </div>
          </div>
          <form
            className="console-form"
            onSubmit={(event) => {
              event.preventDefault();
              void app.connectOpenAiApiKey();
            }}
          >
            <div className="console-grid-2">
              <label>
                Profile name
                <input
                  value={app.authApiKeyDraft.profileName}
                  onChange={(event) =>
                    app.setAuthApiKeyDraft((current) => ({ ...current, profileName: event.target.value }))
                  }
                  placeholder="default-openai"
                />
              </label>
              <label>
                Scope
                <select
                  value={app.authApiKeyDraft.scopeKind}
                  onChange={(event) =>
                    app.setAuthApiKeyDraft((current) => ({
                      ...current,
                      scopeKind: event.target.value === "agent" ? "agent" : "global"
                    }))
                  }
                >
                  <option value="global">global</option>
                  <option value="agent">agent</option>
                </select>
              </label>
            </div>

            {app.authApiKeyDraft.scopeKind === "agent" && (
              <label>
                Agent ID
                <input
                  value={app.authApiKeyDraft.agentId}
                  onChange={(event) =>
                    app.setAuthApiKeyDraft((current) => ({ ...current, agentId: event.target.value }))
                  }
                  placeholder="agent-primary"
                />
              </label>
            )}

            <label>
              OpenAI API key
              <input
                type="password"
                value={app.authApiKeyDraft.apiKey}
                onChange={(event) =>
                  app.setAuthApiKeyDraft((current) => ({ ...current, apiKey: event.target.value }))
                }
                placeholder="sk-..."
                autoComplete="off"
              />
            </label>

            <label className="console-checkbox-inline">
              <input
                type="checkbox"
                checked={app.authApiKeyDraft.setDefault}
                onChange={(event) =>
                  app.setAuthApiKeyDraft((current) => ({ ...current, setDefault: event.target.checked }))
                }
              />
              Set as default model auth profile
            </label>

            <div className="console-inline-actions">
              <button type="submit" disabled={app.authBusy}>
                {app.authBusy
                  ? app.authApiKeyDraft.profileId.trim().length > 0
                    ? "Rotating..."
                    : "Connecting..."
                  : app.authApiKeyDraft.profileId.trim().length > 0
                    ? "Rotate API key"
                    : "Connect API key"}
              </button>
              {app.authApiKeyDraft.profileId.trim().length > 0 && (
                <button
                  type="button"
                  className="secondary"
                  onClick={() => app.cancelApiKeyRotation()}
                  disabled={app.authBusy}
                >
                  Cancel rotation
                </button>
              )}
            </div>
          </form>
        </article>

        <article className="console-subpanel">
          <div className="console-subpanel__header">
            <div>
              <h3>Connect via OAuth</h3>
              <p className="chat-muted">
                Start browser-based OpenAI authorization in a popup or new window, then poll callback state until the profile completes.
              </p>
            </div>
          </div>
          <form
            className="console-form"
            onSubmit={(event) => {
              event.preventDefault();
              void app.startOpenAiOAuth();
            }}
          >
            <div className="console-grid-2">
              <label>
                Profile name
                <input
                  value={app.authOAuthDraft.profileName}
                  onChange={(event) =>
                    app.setAuthOAuthDraft((current) => ({ ...current, profileName: event.target.value }))
                  }
                  placeholder="default-openai-oauth"
                />
              </label>
              <label>
                Scope
                <select
                  value={app.authOAuthDraft.scopeKind}
                  onChange={(event) =>
                    app.setAuthOAuthDraft((current) => ({
                      ...current,
                      scopeKind: event.target.value === "agent" ? "agent" : "global"
                    }))
                  }
                >
                  <option value="global">global</option>
                  <option value="agent">agent</option>
                </select>
              </label>
            </div>

            {app.authOAuthDraft.scopeKind === "agent" && (
              <label>
                Agent ID
                <input
                  value={app.authOAuthDraft.agentId}
                  onChange={(event) =>
                    app.setAuthOAuthDraft((current) => ({ ...current, agentId: event.target.value }))
                  }
                  placeholder="agent-primary"
                />
              </label>
            )}

            <div className="console-grid-2">
              <label>
                OAuth client_id
                <input
                  value={app.authOAuthDraft.clientId}
                  onChange={(event) =>
                    app.setAuthOAuthDraft((current) => ({ ...current, clientId: event.target.value }))
                  }
                  placeholder="openai-client-id"
                  autoComplete="off"
                />
              </label>
              <label>
                OAuth client_secret
                <input
                  type="password"
                  value={app.authOAuthDraft.clientSecret}
                  onChange={(event) =>
                    app.setAuthOAuthDraft((current) => ({ ...current, clientSecret: event.target.value }))
                  }
                  autoComplete="off"
                />
              </label>
            </div>

            <label>
              Requested scopes
              <input
                value={app.authOAuthDraft.scopes}
                onChange={(event) =>
                  app.setAuthOAuthDraft((current) => ({ ...current, scopes: event.target.value }))
                }
                placeholder="openid profile email offline_access"
              />
            </label>

            <label className="console-checkbox-inline">
              <input
                type="checkbox"
                checked={app.authOAuthDraft.setDefault}
                onChange={(event) =>
                  app.setAuthOAuthDraft((current) => ({ ...current, setDefault: event.target.checked }))
                }
              />
              Set as default model auth profile after callback completes
            </label>

            <div className="console-inline-actions">
              <button type="submit" disabled={app.authBusy}>
                {app.authBusy ? "Starting OAuth..." : "Start OpenAI OAuth"}
              </button>
              {app.authActiveOauthAttempt !== null && (
                <button
                  type="button"
                  className="secondary"
                  onClick={() => app.openActiveOauthWindow()}
                  disabled={app.authBusy}
                >
                  Reopen authorization window
                </button>
              )}
              {app.authActiveOauthAttempt !== null && (
                <button
                  type="button"
                  className="secondary"
                  onClick={() => void app.checkOpenAiCallbackState(app.authActiveOauthAttempt?.attempt_id)}
                  disabled={app.authPolling}
                >
                  {app.authPolling ? "Checking..." : "Poll callback state"}
                </button>
              )}
            </div>
          </form>
          {app.authActiveOauthAttempt !== null && (
            <div className="auth-inline-note">
              <p><strong>Authorization URL ready.</strong> If the pop-up was blocked, reopen it from this card.</p>
              <p className="chat-muted">
                Expires {formatUnixMs(app.authActiveOauthAttempt.expires_at_unix_ms)}.
                {activeAttemptPending ? " Polling continues until the callback resolves." : ""}
              </p>
            </div>
          )}
        </article>
      </section>

      <section className="console-subpanel">
        <div className="console-subpanel__header">
          <div>
            <h3>Selected profile detail</h3>
            <p className="chat-muted">
              Inspect one OpenAI auth profile in detail before you reconnect, refresh, rotate, or revoke it.
            </p>
          </div>
          {app.authProfiles.length > 0 && (
            <label className="auth-profile-select">
              Selected profile
              <select value={selectedProfileId} onChange={(event) => setSelectedProfileId(event.target.value)}>
                {app.authProfiles.map((profile) => (
                  <option key={profile.profile_id} value={profile.profile_id}>
                    {profile.profile_name} ({profile.profile_id})
                  </option>
                ))}
              </select>
            </label>
          )}
        </div>
        {selectedProfile === null ? (
          <p>No OpenAI profile selected yet.</p>
        ) : (
          <OpenAiProfileCard
            profile={selectedProfile}
            health={selectedHealth}
            isDefault={app.authProviderState?.default_profile_id === selectedProfile.profile_id}
            isOauthAttemptPending={
              app.authActiveOauthAttempt?.profile_id === selectedProfile.profile_id &&
              app.authOauthCallbackState?.state === "pending"
            }
            isSelected
            onSelect={() => setSelectedProfileId(selectedProfile.profile_id)}
            onRotateApiKey={() => app.prepareApiKeyRotation(selectedProfile)}
            onReconnect={() => void app.reconnectOpenAiProfile(selectedProfile.profile_id)}
            onRefresh={() => void app.refreshOpenAiProfile(selectedProfile.profile_id)}
            onSetDefault={() => void app.setOpenAiDefaultProfile(selectedProfile.profile_id)}
            onRevoke={() => void app.revokeOpenAiProfile(selectedProfile.profile_id)}
            busy={app.authBusy}
          />
        )}
      </section>

      <section className="console-subpanel">
        <div className="console-subpanel__header">
          <div>
            <h3>OpenAI profiles</h3>
            <p className="chat-muted">
              Profile cards surface health, expiry, refresh posture, default selection, and recovery actions.
            </p>
          </div>
        </div>

        {app.authProfiles.length === 0 ? (
          <p>No OpenAI auth profiles found yet.</p>
        ) : (
          <div className="console-stack">
            {app.authProfiles.map((profile) => (
              <OpenAiProfileCard
                key={profile.profile_id}
                profile={profile}
                health={healthByProfileId.get(profile.profile_id)}
                isDefault={app.authProviderState?.default_profile_id === profile.profile_id}
                isOauthAttemptPending={
                  app.authActiveOauthAttempt?.profile_id === profile.profile_id &&
                  app.authOauthCallbackState?.state === "pending"
                }
                isSelected={selectedProfileId === profile.profile_id}
                onSelect={() => setSelectedProfileId(profile.profile_id)}
                onRotateApiKey={() => app.prepareApiKeyRotation(profile)}
                onReconnect={() => void app.reconnectOpenAiProfile(profile.profile_id)}
                onRefresh={() => void app.refreshOpenAiProfile(profile.profile_id)}
                onSetDefault={() => void app.setOpenAiDefaultProfile(profile.profile_id)}
                onRevoke={() => void app.revokeOpenAiProfile(profile.profile_id)}
                busy={app.authBusy}
              />
            ))}
          </div>
        )}
      </section>

      <section className="console-grid-2">
        <article className="console-subpanel">
          <h3>Expiry distribution</h3>
          {expiryDistribution === null ? (
            <p>No expiry distribution available.</p>
          ) : (
            <div className="auth-summary-grid">
              <p><strong>Expired:</strong> {readMetricNumber(expiryDistribution, "expired")}</p>
              <p><strong>&lt;5m:</strong> {readMetricNumber(expiryDistribution, "under_5m")}</p>
              <p><strong>5m-15m:</strong> {readMetricNumber(expiryDistribution, "between_5m_15m")}</p>
              <p><strong>15m-60m:</strong> {readMetricNumber(expiryDistribution, "between_15m_60m")}</p>
              <p><strong>1h-24h:</strong> {readMetricNumber(expiryDistribution, "between_1h_24h")}</p>
              <p><strong>&gt;24h:</strong> {readMetricNumber(expiryDistribution, "over_24h")}</p>
              <p><strong>Unknown:</strong> {readMetricNumber(expiryDistribution, "unknown")}</p>
              <p><strong>Missing:</strong> {readMetricNumber(expiryDistribution, "missing")}</p>
            </div>
          )}
        </article>

        <article className="console-subpanel">
          <h3>Provider capabilities</h3>
          {app.authProviderState === null ? (
            <p>No provider contract state loaded.</p>
          ) : (
            <div className="auth-summary-grid">
              <p><strong>OAuth:</strong> {app.authProviderState.oauth_supported ? "supported" : "unsupported"}</p>
              <p><strong>Callback:</strong> {app.authProviderState.callback_supported ? "supported" : "unsupported"}</p>
              <p><strong>Reconnect:</strong> {app.authProviderState.reconnect_supported ? "supported" : "unsupported"}</p>
              <p><strong>Revoke:</strong> {app.authProviderState.revoke_supported ? "supported" : "unsupported"}</p>
              <p><strong>Default selection:</strong> {app.authProviderState.default_selection_supported ? "supported" : "unsupported"}</p>
            </div>
          )}
        </article>
      </section>
    </main>
  );
}

type OpenAiProfileCardProps = {
  profile: AuthProfileView;
  health: AuthHealthProfile | undefined;
  isDefault: boolean;
  isOauthAttemptPending: boolean;
  isSelected: boolean;
  onSelect: () => void;
  onRotateApiKey: () => void;
  onReconnect: () => void;
  onRefresh: () => void;
  onSetDefault: () => void;
  onRevoke: () => void;
  busy: boolean;
};

function OpenAiProfileCard({
  profile,
  health,
  isDefault,
  isOauthAttemptPending,
  isSelected,
  onSelect,
  onRotateApiKey,
  onReconnect,
  onRefresh,
  onSetDefault,
  onRevoke,
  busy
}: OpenAiProfileCardProps) {
  const credential = profile.credential;
  const isOauth = credential.type === "oauth";
  const refreshState = isOauth ? readMetricsObject(credential.refresh_state) : null;

  return (
    <article className="console-item-card auth-profile-card">
      <div className="auth-profile-card__header">
        <div className="console-item-card__meta">
          <div className="auth-profile-card__title-row">
            <h4>{profile.profile_name}</h4>
            {isDefault && <span className="auth-tag auth-tag--default">default</span>}
            {isSelected && <span className="auth-tag">selected</span>}
            <span className={statusPillClass(health?.state ?? (isOauth ? "missing" : "static"))}>
              {health?.state ?? (isOauth ? "missing" : "static")}
            </span>
            {isOauthAttemptPending && <span className="auth-tag">oauth pending</span>}
          </div>
          <p className="chat-muted">{profile.profile_id}</p>
        </div>
      </div>

      <div className="auth-profile-card__grid">
        <p><strong>Credential:</strong> {isOauth ? "OAuth" : "API key"}</p>
        <p><strong>Scope:</strong> {formatScope(profile)}</p>
        <p><strong>Created:</strong> {formatUnixMs(profile.created_at_unix_ms)}</p>
        <p><strong>Updated:</strong> {formatUnixMs(profile.updated_at_unix_ms)}</p>
        {health !== undefined && (
          <>
            <p><strong>Health:</strong> {health.state}</p>
            <p><strong>Reason:</strong> {health.reason.length > 0 ? health.reason : "n/a"}</p>
          </>
        )}
        {isOauth && (
          <>
            <p><strong>Expires:</strong> {formatUnixMs(credential.expires_at_unix_ms)}</p>
            <p><strong>Scopes:</strong> {credential.scopes.length > 0 ? credential.scopes.join(", ") : "default"}</p>
          </>
        )}
      </div>

      {isOauth && refreshState !== null && (
        <div className="auth-inline-note">
          <p><strong>Refresh failures:</strong> {readMetricNumber(refreshState, "failure_count")}</p>
          <p><strong>Last refresh success:</strong> {formatUnixMs(readMetricNumber(refreshState, "last_success_unix_ms"))}</p>
          <p><strong>Next allowed refresh:</strong> {formatUnixMs(readMetricNumber(refreshState, "next_allowed_refresh_unix_ms"))}</p>
          {readMetricText(refreshState, "last_error") !== "n/a" && (
            <p><strong>Last error:</strong> {readMetricText(refreshState, "last_error")}</p>
          )}
        </div>
      )}

      {!isOauth && (
        <div className="auth-inline-note">
          <p>Credential material is stored in Vault. Rotate this profile by submitting a new API key.</p>
        </div>
      )}

      <div className="console-inline-actions">
        {!isSelected && (
          <button type="button" className="secondary" onClick={onSelect} disabled={busy}>
            Inspect
          </button>
        )}
        {!isDefault && (
          <button type="button" className="secondary" onClick={onSetDefault} disabled={busy}>
            Set as default
          </button>
        )}
        {isOauth ? (
          <>
            <button type="button" className="secondary" onClick={onReconnect} disabled={busy}>
              Reconnect
            </button>
            <button type="button" className="secondary" onClick={onRefresh} disabled={busy}>
              Refresh token
            </button>
          </>
        ) : (
          <button type="button" className="secondary" onClick={onRotateApiKey} disabled={busy}>
            Rotate API key
          </button>
        )}
        <button type="button" className="button--warn" onClick={onRevoke} disabled={busy}>
          {isOauth ? "Revoke" : "Delete profile"}
        </button>
      </div>
    </article>
  );
}

function summarizeAuthHealth(profiles: AuthHealthProfile[]): {
  total: number;
  ok: number;
  expiring: number;
  expired: number;
  missing: number;
  staticCount: number;
} {
  const summary = {
    total: profiles.length,
    ok: 0,
    expiring: 0,
    expired: 0,
    missing: 0,
    staticCount: 0
  };
  for (const profile of profiles) {
    if (profile.state === "ok") {
      summary.ok += 1;
    }
    if (profile.state === "expiring") {
      summary.expiring += 1;
    }
    if (profile.state === "expired") {
      summary.expired += 1;
    }
    if (profile.state === "missing") {
      summary.missing += 1;
    }
    if (profile.state === "static") {
      summary.staticCount += 1;
    }
  }
  return summary;
}

function formatScope(profile: AuthProfileView): string {
  if (profile.scope.kind === "agent") {
    return profile.scope.agent_id === undefined ? "agent" : `agent:${profile.scope.agent_id}`;
  }
  return "global";
}

function formatUnixMs(value: number | null | undefined): string {
  if (value === undefined || value === null || !Number.isFinite(value) || value <= 0) {
    return "n/a";
  }
  return new Date(value).toLocaleString();
}

function statusPillClass(state: string): string {
  const tone =
    state === "ok" || state === "static" || state === "succeeded" || state === "ready"
      ? "ok"
      : state === "expiring" || state === "cooldown" || state === "not_due" || state === "pending"
        ? "warn"
        : "danger";
  return `auth-status-pill auth-status-pill--${tone}`;
}

function readMetricsObject(value: JsonValue | null): Record<string, JsonValue> | null {
  return value !== null && isJsonObject(value) ? value : null;
}

function readMetricNumber(record: Record<string, JsonValue> | null, key: string): number {
  if (record === null) {
    return 0;
  }
  const value = record[key];
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string") {
    const parsed = Number.parseInt(value, 10);
    return Number.isFinite(parsed) ? parsed : 0;
  }
  return 0;
}

function readMetricText(record: Record<string, JsonValue> | null, key: string): string {
  if (record === null) {
    return "n/a";
  }
  return readString(record, key) ?? "n/a";
}

function readProviderRefreshMetrics(
  record: Record<string, JsonValue> | null,
  provider: string
): Record<string, JsonValue> | null {
  if (record === null || !Array.isArray(record.by_provider)) {
    return null;
  }
  for (const entry of record.by_provider) {
    if (isJsonObject(entry) && readString(entry, "provider") === provider) {
      return entry;
    }
  }
  return null;
}
