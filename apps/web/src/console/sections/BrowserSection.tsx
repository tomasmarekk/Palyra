import { Button } from "@heroui/react";

import { CheckboxField, SelectField, TextInputField } from "../components/ui";
import {
  WorkspaceMetricCard,
  WorkspacePageHeader,
  WorkspaceSectionCard,
  WorkspaceStatusChip,
} from "../components/workspace/WorkspaceChrome";
import {
  PrettyJsonBlock,
  formatUnixMs,
  readBool,
  readNumber,
  readString,
  type JsonObject,
} from "../shared";
import type { ConsoleAppState } from "../useConsoleAppState";

type BrowserSectionProps = {
  app: Pick<
    ConsoleAppState,
    | "browserBusy"
    | "diagnosticsSnapshot"
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
  const diagnostics = asJsonObject(app.diagnosticsSnapshot);
  const observability = asJsonObject(diagnostics?.observability);
  const selfHealing = asJsonObject(observability?.self_healing);
  const activeIncidents = Array.isArray(selfHealing?.active_incidents)
    ? selfHealing.active_incidents
        .map(asJsonObject)
        .filter((value): value is JsonObject => value !== null)
    : [];
  const browserIncidents = activeIncidents.filter(
    (incident) => readString(incident, "domain") === "browser",
  );
  const profiles = Array.isArray(app.browserProfiles) ? app.browserProfiles : [];
  const activeProfile =
    profiles.find((profile) => readString(profile, "profile_id") === app.browserActiveProfileId) ??
    null;
  const downloads = Array.isArray(app.browserDownloads) ? app.browserDownloads : [];
  const quarantinedDownloads = downloads.filter((artifact) =>
    readBool(asJsonObject(artifact), "quarantined"),
  );

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
            <WorkspaceStatusChip tone={browserIncidents.length > 0 ? "warning" : "default"}>
              {browserIncidents.length} healing incidents
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
          detail={
            activeProfile === null
              ? "Create or load a profile to start."
              : (readString(activeProfile, "name") ?? "Active profile loaded")
          }
          tone={profiles.length > 0 ? "success" : "default"}
        />
        <WorkspaceMetricCard
          label="Active profile"
          value={
            activeProfile === null
              ? "None"
              : (readString(activeProfile, "name") ?? app.browserActiveProfileId)
          }
          detail={activeProfile === null ? "Nothing activated." : app.browserActiveProfileId}
        />
        <WorkspaceMetricCard
          label="Downloads"
          value={downloads.length}
          detail={
            downloads[0] === undefined
              ? "No browser artifacts loaded."
              : (readString(asJsonObject(downloads[0]), "file_name") ?? "Latest artifact available")
          }
        />
        <WorkspaceMetricCard
          label="Relay access"
          value={app.browserRelayToken.trim().length > 0 ? "Token minted" : "Idle"}
          detail={
            app.browserRelayTokenExpiry === null
              ? "No active relay token."
              : `Expires ${formatUnixMs(app.browserRelayTokenExpiry)}`
          }
          tone={app.browserRelayToken.trim().length > 0 ? "success" : "default"}
        />
        <WorkspaceMetricCard
          label="Healing watch"
          value={browserIncidents.length}
          detail={
            browserIncidents[0] === undefined
              ? "No browser incidents reported."
              : (readString(browserIncidents[0], "summary") ?? "Incident available in diagnostics")
          }
          tone={browserIncidents.length > 0 ? "warning" : "default"}
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
                        aria-label={`Select ${readString(profile, "name") ?? profileId}`}
                        variant="secondary"
                        size="sm"
                        onPress={() => app.setBrowserRenameProfileId(profileId)}
                      >
                        Select
                      </Button>
                      <Button
                        aria-label={`Activate ${readString(profile, "name") ?? profileId}`}
                        size="sm"
                        onPress={() => void app.activateBrowserProfile(profile)}
                        isDisabled={app.browserBusy}
                      >
                        Activate
                      </Button>
                      <Button
                        aria-label={`Delete ${readString(profile, "name") ?? profileId}`}
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
            <form
              className="workspace-form"
              onSubmit={(event) => void app.createBrowserProfile(event)}
            >
              <div className="workspace-form-grid">
                <TextInputField
                  label="Principal"
                  value={app.browserPrincipal}
                  onChange={app.setBrowserPrincipal}
                />
                <TextInputField
                  label="Profile name"
                  value={app.browserProfileName}
                  onChange={app.setBrowserProfileName}
                />
                <TextInputField
                  label="Theme color"
                  value={app.browserProfileTheme}
                  onChange={app.setBrowserProfileTheme}
                />
              </div>
              <div className="workspace-inline">
                <CheckboxField
                  checked={app.browserProfilePersistence}
                  label="Persistence enabled"
                  onChange={app.setBrowserProfilePersistence}
                />
                <CheckboxField
                  checked={app.browserProfilePrivate}
                  label="Private profile"
                  onChange={app.setBrowserProfilePrivate}
                />
              </div>
              <Button type="submit" isDisabled={app.browserBusy}>
                {app.browserBusy ? "Creating..." : "Create profile"}
              </Button>
            </form>

            <form
              className="workspace-form"
              onSubmit={(event) => {
                event.preventDefault();
                void app.renameBrowserProfile();
              }}
            >
              <div className="workspace-form-grid">
                <TextInputField
                  label="Profile ID"
                  value={app.browserRenameProfileId}
                  onChange={app.setBrowserRenameProfileId}
                />
                <TextInputField
                  label="New name"
                  value={app.browserRenameName}
                  onChange={app.setBrowserRenameName}
                />
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
            <TextInputField
              label="Session ID"
              value={app.browserDownloadsSessionId}
              onChange={app.setBrowserDownloadsSessionId}
            />
            <CheckboxField
              checked={app.browserDownloadsQuarantinedOnly}
              label="Quarantined only"
              onChange={app.setBrowserDownloadsQuarantinedOnly}
            />
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
                      <WorkspaceStatusChip
                        tone={readBool(record, "quarantined") ? "warning" : "success"}
                      >
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
              <TextInputField
                label="Session ID"
                value={app.browserRelaySessionId}
                onChange={app.setBrowserRelaySessionId}
              />
              <TextInputField
                label="Extension ID"
                value={app.browserRelayExtensionId}
                onChange={app.setBrowserRelayExtensionId}
              />
              <TextInputField
                label="TTL ms"
                value={app.browserRelayTtlMs}
                onChange={app.setBrowserRelayTtlMs}
              />
            </div>
            <div className="console-inline-actions">
              <Button onPress={() => void app.mintBrowserRelayToken()} isDisabled={app.browserBusy}>
                {app.browserBusy ? "Minting..." : "Mint relay token"}
              </Button>
              <WorkspaceStatusChip
                tone={app.browserRelayToken.trim().length > 0 ? "success" : "default"}
              >
                {app.browserRelayTokenExpiry === null
                  ? "No token"
                  : `Expires ${formatUnixMs(app.browserRelayTokenExpiry)}`}
              </WorkspaceStatusChip>
            </div>
            {(app.browserRelayToken.trim().length > 0 || app.browserRelayTokenExpiry !== null) && (
              <PrettyJsonBlock
                value={{
                  relay_token: app.browserRelayToken,
                  expires_at_unix_ms: app.browserRelayTokenExpiry,
                }}
                revealSensitiveValues={app.revealSensitiveValues}
              />
            )}

            <div className="workspace-form-grid">
              <SelectField
                label="Action"
                options={[
                  { key: "capture_selection", label: "capture_selection" },
                  { key: "open_tab", label: "open_tab" },
                  { key: "send_page_snapshot", label: "send_page_snapshot" },
                ]}
                value={app.browserRelayAction}
                onChange={(value) =>
                  app.setBrowserRelayAction(
                    value as "open_tab" | "capture_selection" | "send_page_snapshot",
                  )
                }
              />
              <TextInputField
                label="Open-tab URL"
                value={app.browserRelayOpenTabUrl}
                onChange={app.setBrowserRelayOpenTabUrl}
              />
              <TextInputField
                label="Capture selector"
                value={app.browserRelaySelector}
                onChange={app.setBrowserRelaySelector}
              />
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
