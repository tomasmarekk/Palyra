import { Button } from "@heroui/react";

import { WorkspaceMetricCard, WorkspacePageHeader, WorkspaceSectionCard, WorkspaceStatusChip } from "../components/workspace/WorkspaceChrome";
import { PrettyJsonBlock, formatUnixMs, readBool, readNumber, readString, type JsonObject } from "../shared";
import type { ConsoleAppState } from "../useConsoleAppState";

type BrowserSectionProps = {
  app: Pick<
    ConsoleAppState,
    | "browserBusy"
    | "browserPrincipal"
    | "setBrowserPrincipal"
    | "browserProfiles"
    | "browserActiveProfileId"
    | "browserProfileName"
    | "setBrowserProfileName"
    | "browserProfileTheme"
    | "setBrowserProfileTheme"
    | "browserProfilePersistence"
    | "setBrowserProfilePersistence"
    | "browserProfilePrivate"
    | "setBrowserProfilePrivate"
    | "browserRenameProfileId"
    | "setBrowserRenameProfileId"
    | "browserRenameName"
    | "setBrowserRenameName"
    | "browserRelaySessionId"
    | "setBrowserRelaySessionId"
    | "browserRelayExtensionId"
    | "setBrowserRelayExtensionId"
    | "browserRelayTtlMs"
    | "setBrowserRelayTtlMs"
    | "browserRelayToken"
    | "browserRelayTokenExpiry"
    | "browserRelayAction"
    | "setBrowserRelayAction"
    | "browserRelayOpenTabUrl"
    | "setBrowserRelayOpenTabUrl"
    | "browserRelaySelector"
    | "setBrowserRelaySelector"
    | "browserRelayResult"
    | "browserDownloadsSessionId"
    | "setBrowserDownloadsSessionId"
    | "browserDownloadsQuarantinedOnly"
    | "setBrowserDownloadsQuarantinedOnly"
    | "browserDownloads"
    | "refreshBrowserProfiles"
    | "createBrowserProfile"
    | "activateBrowserProfile"
    | "deleteBrowserProfile"
    | "renameBrowserProfile"
    | "mintBrowserRelayToken"
    | "dispatchBrowserRelayAction"
    | "refreshBrowserDownloads"
    | "revealSensitiveValues"
  >;
};

export function BrowserSection({ app }: BrowserSectionProps) {
  const profiles = Array.isArray(app.browserProfiles) ? app.browserProfiles : [];
  const activeProfile =
    profiles.find((profile) => readString(profile, "profile_id") === app.browserActiveProfileId) ?? null;
  const downloads = Array.isArray(app.browserDownloads) ? app.browserDownloads : [];
  const quarantinedDownloads = downloads.filter((artifact) => readBool(asJsonObject(artifact), "quarantined"));

  return (
    <main className="workspace-page">
      <WorkspacePageHeader
        eyebrow="Control"
        title="Browser"
        description="Manage browser profiles first, keep downloads visible second, and leave relay tooling in a clearly advanced lane."
        status={
          <>
            <WorkspaceStatusChip tone={profiles.length > 0 ? "success" : "default"}>
              {profiles.length} profiles
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={downloads.length > 0 ? "success" : "default"}>
              {downloads.length} downloads
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={quarantinedDownloads.length > 0 ? "warning" : "default"}>
              {quarantinedDownloads.length} quarantined
            </WorkspaceStatusChip>
          </>
        }
        actions={
          <Button
            variant="secondary"
            onPress={() => void app.refreshBrowserProfiles()}
            isDisabled={app.browserBusy}
          >
            {app.browserBusy ? "Refreshing..." : "Refresh browser"}
          </Button>
        }
      />

      <section className="workspace-metric-grid">
        <WorkspaceMetricCard
          label="Browser service"
          value={profiles.length > 0 ? "Ready" : "No profiles yet"}
          detail={activeProfile === null ? "Create or load a profile to start." : readString(activeProfile, "name") ?? "Active profile loaded"}
          tone={profiles.length > 0 ? "success" : "default"}
        />
        <WorkspaceMetricCard
          label="Active profile"
          value={activeProfile === null ? "None" : readString(activeProfile, "name") ?? app.browserActiveProfileId}
          detail={activeProfile === null ? "Nothing activated." : app.browserActiveProfileId}
        />
        <WorkspaceMetricCard
          label="Downloads"
          value={downloads.length}
          detail={downloads[0] === undefined ? "No browser artifacts loaded." : readString(asJsonObject(downloads[0]), "file_name") ?? "Latest artifact available"}
        />
        <WorkspaceMetricCard
          label="Relay access"
          value={app.browserRelayToken.trim().length > 0 ? "Token minted" : "Idle"}
          detail={app.browserRelayTokenExpiry === null ? "No active relay token." : `Expires ${formatUnixMs(app.browserRelayTokenExpiry)}`}
          tone={app.browserRelayToken.trim().length > 0 ? "success" : "default"}
        />
      </section>

      <section className="workspace-two-column">
        <WorkspaceSectionCard
          title="Profiles"
          description="Profiles are the main object on this page, with activation and deletion kept close to each record."
        >
          {profiles.length === 0 ? (
            <p className="chat-muted">No browser profiles available.</p>
          ) : (
            <div className="workspace-list">
              {profiles.map((profile) => {
                const profileId = readString(profile, "profile_id") ?? "unknown";
                const isActive = profileId === app.browserActiveProfileId;
                return (
                  <article key={profileId} className="workspace-list-item">
                    <div>
                      <strong>{readString(profile, "name") ?? profileId}</strong>
                      <p className="chat-muted">
                        {readString(profile, "principal") ?? "n/a"} · last used{" "}
                        {formatUnixMs(readUnixMillis(profile, "last_used_unix_ms"))}
                      </p>
                    </div>
                    <div className="workspace-inline">
                      <WorkspaceStatusChip tone={isActive ? "success" : "default"}>
                        {isActive ? "active" : "inactive"}
                      </WorkspaceStatusChip>
                      <WorkspaceStatusChip tone="default">
                        {readBool(profile, "private_profile") ? "private" : "shared"}
                      </WorkspaceStatusChip>
                    </div>
                    <div className="console-inline-actions">
                      <Button
                        variant="secondary"
                        size="sm"
                        onPress={() => app.setBrowserRenameProfileId(profileId)}
                      >
                        Select
                      </Button>
                      <Button
                        size="sm"
                        onPress={() => void app.activateBrowserProfile(profile)}
                        isDisabled={app.browserBusy}
                      >
                        Activate
                      </Button>
                      <Button
                        variant="danger-soft"
                        size="sm"
                        onPress={() => void app.deleteBrowserProfile(profile)}
                        isDisabled={app.browserBusy}
                      >
                        Delete
                      </Button>
                    </div>
                  </article>
                );
              })}
            </div>
          )}
        </WorkspaceSectionCard>

        <WorkspaceSectionCard
          title="Profile operations"
          description="Create and rename flows stay focused while preserving persistence and privacy controls."
        >
          <div className="workspace-stack">
            <form className="workspace-form" onSubmit={(event) => void app.createBrowserProfile(event)}>
              <div className="workspace-form-grid">
                <label>
                  Principal
                  <input value={app.browserPrincipal} onChange={(event) => app.setBrowserPrincipal(event.target.value)} />
                </label>
                <label>
                  Profile name
                  <input value={app.browserProfileName} onChange={(event) => app.setBrowserProfileName(event.target.value)} />
                </label>
                <label>
                  Theme color
                  <input value={app.browserProfileTheme} onChange={(event) => app.setBrowserProfileTheme(event.target.value)} />
                </label>
              </div>
              <div className="workspace-inline">
                <label className="console-checkbox-inline">
                  <input
                    type="checkbox"
                    checked={app.browserProfilePersistence}
                    onChange={(event) => app.setBrowserProfilePersistence(event.target.checked)}
                  />
                  Persistence enabled
                </label>
                <label className="console-checkbox-inline">
                  <input
                    type="checkbox"
                    checked={app.browserProfilePrivate}
                    onChange={(event) => app.setBrowserProfilePrivate(event.target.checked)}
                  />
                  Private profile
                </label>
              </div>
              <Button type="submit" isDisabled={app.browserBusy}>
                {app.browserBusy ? "Creating..." : "Create profile"}
              </Button>
            </form>

            <form className="workspace-form" onSubmit={(event) => {
              event.preventDefault();
              void app.renameBrowserProfile();
            }}>
              <div className="workspace-form-grid">
                <label>
                  Profile ID
                  <input value={app.browserRenameProfileId} onChange={(event) => app.setBrowserRenameProfileId(event.target.value)} />
                </label>
                <label>
                  New name
                  <input value={app.browserRenameName} onChange={(event) => app.setBrowserRenameName(event.target.value)} />
                </label>
              </div>
              <Button variant="secondary" type="submit" isDisabled={app.browserBusy}>
                {app.browserBusy ? "Renaming..." : "Rename profile"}
              </Button>
            </form>
          </div>
        </WorkspaceSectionCard>
      </section>

      <section className="workspace-two-column">
        <WorkspaceSectionCard
          title="Downloads"
          description="Downloads stay visible and scannable, with quarantine posture called out instead of hidden in raw JSON."
          actions={
            <Button
              variant="secondary"
              size="sm"
              onPress={() => void app.refreshBrowserDownloads()}
              isDisabled={app.browserBusy}
            >
              {app.browserBusy ? "Loading..." : "Load downloads"}
            </Button>
          }
        >
          <div className="workspace-form-grid">
            <label>
              Session ID
              <input
                value={app.browserDownloadsSessionId}
                onChange={(event) => app.setBrowserDownloadsSessionId(event.target.value)}
              />
            </label>
            <label className="console-checkbox-inline">
              <input
                type="checkbox"
                checked={app.browserDownloadsQuarantinedOnly}
                onChange={(event) => app.setBrowserDownloadsQuarantinedOnly(event.target.checked)}
              />
              Quarantined only
            </label>
          </div>

          {downloads.length === 0 ? (
            <p className="chat-muted">No browser downloads loaded.</p>
          ) : (
            <div className="workspace-list">
              {downloads.map((artifact) => {
                const record = asJsonObject(artifact);
                const artifactId = readString(record, "artifact_id") ?? "unknown";
                const sizeBytes = readNumber(record, "size_bytes");
                return (
                  <article key={artifactId} className="workspace-list-item">
                    <div>
                      <strong>{readString(record, "file_name") ?? artifactId}</strong>
                      <p className="chat-muted">
                        {readString(record, "mime_type") ?? "mime unavailable"} ·{" "}
                        {sizeBytes === null ? "size n/a" : `${Math.round(sizeBytes / 1024)} KiB`}
                      </p>
                    </div>
                    <div className="workspace-inline">
                      <WorkspaceStatusChip tone={readBool(record, "quarantined") ? "warning" : "success"}>
                        {readBool(record, "quarantined") ? "quarantined" : "available"}
                      </WorkspaceStatusChip>
                      <WorkspaceStatusChip tone="default">
                        {formatUnixMs(readUnixMillis(record, "created_at_unix_ms"))}
                      </WorkspaceStatusChip>
                    </div>
                  </article>
                );
              })}
            </div>
          )}
        </WorkspaceSectionCard>

        <WorkspaceSectionCard
          title="Relay tools"
          description="Relay token minting and advanced actions stay secondary so they do not dominate the page."
        >
          <div className="workspace-stack">
            <div className="workspace-form-grid">
              <label>
                Session ID
                <input value={app.browserRelaySessionId} onChange={(event) => app.setBrowserRelaySessionId(event.target.value)} />
              </label>
              <label>
                Extension ID
                <input value={app.browserRelayExtensionId} onChange={(event) => app.setBrowserRelayExtensionId(event.target.value)} />
              </label>
              <label>
                TTL ms
                <input value={app.browserRelayTtlMs} onChange={(event) => app.setBrowserRelayTtlMs(event.target.value)} />
              </label>
            </div>
            <div className="console-inline-actions">
              <Button onPress={() => void app.mintBrowserRelayToken()} isDisabled={app.browserBusy}>
                {app.browserBusy ? "Minting..." : "Mint relay token"}
              </Button>
              <WorkspaceStatusChip tone={app.browserRelayToken.trim().length > 0 ? "success" : "default"}>
                {app.browserRelayTokenExpiry === null ? "No token" : `Expires ${formatUnixMs(app.browserRelayTokenExpiry)}`}
              </WorkspaceStatusChip>
            </div>
            {(app.browserRelayToken.trim().length > 0 || app.browserRelayTokenExpiry !== null) && (
              <PrettyJsonBlock
                value={{
                  relay_token: app.browserRelayToken,
                  expires_at_unix_ms: app.browserRelayTokenExpiry
                }}
                revealSensitiveValues={app.revealSensitiveValues}
              />
            )}

            <div className="workspace-form-grid">
              <label>
                Action
                <select
                  value={app.browserRelayAction}
                  onChange={(event) =>
                    app.setBrowserRelayAction(
                      event.target.value as "open_tab" | "capture_selection" | "send_page_snapshot"
                    )
                  }
                >
                  <option value="capture_selection">capture_selection</option>
                  <option value="open_tab">open_tab</option>
                  <option value="send_page_snapshot">send_page_snapshot</option>
                </select>
              </label>
              <label>
                Open-tab URL
                <input value={app.browserRelayOpenTabUrl} onChange={(event) => app.setBrowserRelayOpenTabUrl(event.target.value)} />
              </label>
              <label>
                Capture selector
                <input value={app.browserRelaySelector} onChange={(event) => app.setBrowserRelaySelector(event.target.value)} />
              </label>
            </div>

            <Button
              variant="secondary"
              onPress={() => void app.dispatchBrowserRelayAction()}
              isDisabled={app.browserBusy}
            >
              {app.browserBusy ? "Dispatching..." : "Dispatch relay action"}
            </Button>

            {app.browserRelayResult !== null && (
              <PrettyJsonBlock
                value={app.browserRelayResult}
                revealSensitiveValues={app.revealSensitiveValues}
              />
            )}
          </div>
        </WorkspaceSectionCard>
      </section>
    </main>
  );
}

function asJsonObject(value: unknown): JsonObject {
  if (value !== null && typeof value === "object" && !Array.isArray(value)) {
    return value as JsonObject;
  }
  return {};
}

function readUnixMillis(record: JsonObject, key: string): number | null {
  const value = record[key];
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}
