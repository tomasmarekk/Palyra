import { Button } from "@heroui/react";

import { CheckboxField, TextInputField } from "../components/ui";
import {
  WorkspaceMetricCard,
  WorkspacePageHeader,
  WorkspaceSectionCard,
  WorkspaceStatusChip,
} from "../components/workspace/WorkspaceChrome";
import { PrettyJsonBlock, readBool, readNumber, readString, type JsonObject } from "../shared";
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
    | "browserDownloads"
    | "browserSessionId"
    | "setBrowserSessionId"
    | "browserSessionChannel"
    | "setBrowserSessionChannel"
    | "browserSessionProfileId"
    | "setBrowserSessionProfileId"
    | "browserSessionPersistenceEnabled"
    | "setBrowserSessionPersistenceEnabled"
    | "browserSessionPrivateProfile"
    | "setBrowserSessionPrivateProfile"
    | "browserSessionPersistenceId"
    | "setBrowserSessionPersistenceId"
    | "browserSessionAllowDownloads"
    | "setBrowserSessionAllowDownloads"
    | "browserSessionAllowPrivateTargets"
    | "setBrowserSessionAllowPrivateTargets"
    | "browserSessions"
    | "browserSessionDetail"
    | "browserSessionInspect"
    | "browserConsoleEntries"
    | "browserConsoleDiagnostics"
    | "browserPdfExport"
    | "browserLastActionResult"
    | "browserActionUrl"
    | "setBrowserActionUrl"
    | "browserActionSelector"
    | "setBrowserActionSelector"
    | "browserActionText"
    | "setBrowserActionText"
    | "browserActionValue"
    | "setBrowserActionValue"
    | "browserActionKey"
    | "setBrowserActionKey"
    | "browserHighlightDurationMs"
    | "setBrowserHighlightDurationMs"
    | "refreshBrowserProfiles"
    | "refreshBrowserSessions"
    | "createBrowserProfile"
    | "activateBrowserProfile"
    | "deleteBrowserProfile"
    | "renameBrowserProfile"
    | "createBrowserSession"
    | "inspectBrowserSessionWorkspace"
    | "closeBrowserSession"
    | "runBrowserSessionAction"
    | "exportBrowserPdf"
    | "openBrowserSessionWorkbench"
    | "refreshBrowserDownloads"
    | "browserDownloadsSessionId"
    | "setBrowserDownloadsSessionId"
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
    | "mintBrowserRelayToken"
    | "dispatchBrowserRelayAction"
    | "revealSensitiveValues"
  >;
};

export function BrowserSection({ app }: BrowserSectionProps) {
  const profiles = Array.isArray(app.browserProfiles) ? app.browserProfiles : [];
  const downloads = Array.isArray(app.browserDownloads) ? app.browserDownloads : [];
  const sessions = Array.isArray(app.browserSessions) ? app.browserSessions : [];
  const sessionDetail = asJsonObject(app.browserSessionDetail);
  const sessionSummary = asJsonObject(sessionDetail?.summary);
  const consoleDiagnostics = asJsonObject(app.browserConsoleDiagnostics);
  const consoleEntries = Array.isArray(app.browserConsoleEntries) ? app.browserConsoleEntries : [];
  const pdfExport = asJsonObject(app.browserPdfExport);
  const lastActionResult = asJsonObject(app.browserLastActionResult);
  const mode = deriveBrowserMode(
    sessionSummary,
    app.browserSessionPrivateProfile,
    app.browserSessionPersistenceEnabled,
  );
  const runbooks = buildRunbooks(lastActionResult, consoleDiagnostics, pdfExport);
  const failureScreenshot = asDataUri(
    readString(lastActionResult ?? {}, "failure_screenshot_mime_type"),
    readString(lastActionResult ?? {}, "failure_screenshot_base64"),
  );

  return (
    <main className="workspace-page">
      <WorkspacePageHeader
        eyebrow="Control"
        title="Browser"
        description="Run browser debug sessions with explicit profile mode, retained diagnostics, failure artifacts, and PDF evidence in one workbench."
        status={
          <>
            <WorkspaceStatusChip tone={profiles.length > 0 ? "success" : "default"}>
              {profiles.length} profiles
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={sessions.length > 0 ? "success" : "default"}>
              {sessions.length} sessions
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={consoleEntries.length > 0 ? "warning" : "default"}>
              {consoleEntries.length} console entries
            </WorkspaceStatusChip>
          </>
        }
        actions={
          <div className="console-inline-actions">
            <Button variant="secondary" onPress={() => void app.refreshBrowserProfiles()}>
              Profiles
            </Button>
            <Button variant="secondary" onPress={() => void app.refreshBrowserSessions()}>
              Sessions
            </Button>
            <Button variant="secondary" onPress={() => void app.refreshBrowserDownloads()}>
              Downloads
            </Button>
          </div>
        }
      />

      <section className="workspace-metric-grid">
        <WorkspaceMetricCard
          label="Selected session"
          value={app.browserSessionId.trim().length > 0 ? app.browserSessionId : "None"}
          detail={
            readString(sessionSummary ?? {}, "active_tab_title") ??
            "No active browser session selected."
          }
          tone={app.browserSessionId.trim().length > 0 ? "success" : "default"}
        />
        <WorkspaceMetricCard
          label="Profile mode"
          value={mode.label}
          detail={mode.detail}
          tone={mode.tone}
        />
        <WorkspaceMetricCard
          label="Console errors"
          value={readNumber(consoleDiagnostics ?? {}, "error_count") ?? 0}
          detail={`${consoleEntries.length} console entries loaded`}
          tone={
            (readNumber(consoleDiagnostics ?? {}, "error_count") ?? 0) > 0 ? "warning" : "default"
          }
        />
        <WorkspaceMetricCard
          label="Downloads"
          value={downloads.length}
          detail={
            downloads.length > 0
              ? (readString(asJsonObject(downloads[0]) ?? {}, "file_name") ?? "Artifact available")
              : "No browser artifacts loaded."
          }
        />
      </section>

      <section className="workspace-two-column">
        <WorkspaceSectionCard
          title="Session workbench"
          description="Create a session, pin its ID, and keep shared versus isolated profile posture explicit."
        >
          <div className="workspace-stack">
            <form
              className="workspace-form"
              onSubmit={(event) => void app.createBrowserSession(event)}
            >
              <div className="workspace-form-grid">
                <TextInputField
                  label="Principal"
                  value={app.browserPrincipal}
                  onChange={app.setBrowserPrincipal}
                />
                <TextInputField
                  label="Channel"
                  value={app.browserSessionChannel}
                  onChange={app.setBrowserSessionChannel}
                />
                <TextInputField
                  label="Profile ID"
                  value={app.browserSessionProfileId}
                  onChange={app.setBrowserSessionProfileId}
                />
                <TextInputField
                  label="Persistence ID"
                  value={app.browserSessionPersistenceId}
                  onChange={app.setBrowserSessionPersistenceId}
                />
              </div>
              <div className="workspace-inline">
                <CheckboxField
                  checked={app.browserSessionPersistenceEnabled}
                  label="Persistence enabled"
                  onChange={app.setBrowserSessionPersistenceEnabled}
                />
                <CheckboxField
                  checked={app.browserSessionPrivateProfile}
                  label="Isolated profile"
                  onChange={app.setBrowserSessionPrivateProfile}
                />
                <CheckboxField
                  checked={app.browserSessionAllowDownloads}
                  label="Allow downloads"
                  onChange={app.setBrowserSessionAllowDownloads}
                />
                <CheckboxField
                  checked={app.browserSessionAllowPrivateTargets}
                  label="Allow private targets"
                  onChange={app.setBrowserSessionAllowPrivateTargets}
                />
              </div>
              <Button type="submit" isDisabled={app.browserBusy}>
                {app.browserBusy ? "Creating..." : "Create debug session"}
              </Button>
            </form>

            <div className="workspace-form-grid">
              <TextInputField
                label="Active session ID"
                value={app.browserSessionId}
                onChange={app.setBrowserSessionId}
              />
              <TextInputField
                label="Downloads session ID"
                value={app.browserDownloadsSessionId}
                onChange={app.setBrowserDownloadsSessionId}
              />
              <TextInputField
                label="Relay session ID"
                value={app.browserRelaySessionId}
                onChange={app.setBrowserRelaySessionId}
              />
            </div>
            <div className="console-inline-actions">
              <Button
                variant="secondary"
                onPress={() => void app.inspectBrowserSessionWorkspace()}
                isDisabled={app.browserBusy}
              >
                Inspect session
              </Button>
              <Button
                variant="danger-soft"
                onPress={() => void app.closeBrowserSession()}
                isDisabled={app.browserBusy}
              >
                Close session
              </Button>
            </div>
          </div>
        </WorkspaceSectionCard>

        <WorkspaceSectionCard
          title="Inspector"
          description="Keep the selected session summary and runbook visible while you debug."
        >
          <div className="workspace-stack">
            <div className="workspace-inline">
              <WorkspaceStatusChip tone={mode.tone}>{mode.label}</WorkspaceStatusChip>
              <WorkspaceStatusChip tone="default">
                {readNumber(sessionSummary ?? {}, "tab_count") ?? 0} tabs
              </WorkspaceStatusChip>
            </div>
            <PrettyJsonBlock
              value={app.browserSessionInspect ?? app.browserSessionDetail}
              revealSensitiveValues={app.revealSensitiveValues}
            />
            {runbooks.length > 0 ? (
              <ul className="workspace-bullet-list">
                {runbooks.map((item) => (
                  <li key={item}>{item}</li>
                ))}
              </ul>
            ) : null}
          </div>
        </WorkspaceSectionCard>
      </section>

      <section className="workspace-two-column">
        <WorkspaceSectionCard
          title="Manual debug actions"
          description="Use the same action verbs as CLI: navigate, click, type, press, select, highlight, then export PDF evidence when needed."
        >
          <div className="workspace-stack">
            <div className="workspace-form-grid">
              <TextInputField
                label="URL"
                value={app.browserActionUrl}
                onChange={app.setBrowserActionUrl}
              />
              <TextInputField
                label="Selector"
                value={app.browserActionSelector}
                onChange={app.setBrowserActionSelector}
              />
              <TextInputField
                label="Text"
                value={app.browserActionText}
                onChange={app.setBrowserActionText}
              />
              <TextInputField
                label="Select value"
                value={app.browserActionValue}
                onChange={app.setBrowserActionValue}
              />
              <TextInputField
                label="Key"
                value={app.browserActionKey}
                onChange={app.setBrowserActionKey}
              />
              <TextInputField
                label="Highlight duration ms"
                value={app.browserHighlightDurationMs}
                onChange={app.setBrowserHighlightDurationMs}
              />
            </div>
            <div className="console-inline-actions">
              <Button
                onPress={() => void app.runBrowserSessionAction("navigate")}
                isDisabled={app.browserBusy}
              >
                Navigate
              </Button>
              <Button
                variant="secondary"
                onPress={() => void app.runBrowserSessionAction("click")}
                isDisabled={app.browserBusy}
              >
                Click
              </Button>
              <Button
                variant="secondary"
                onPress={() => void app.runBrowserSessionAction("type")}
                isDisabled={app.browserBusy}
              >
                Type
              </Button>
              <Button
                variant="secondary"
                onPress={() => void app.runBrowserSessionAction("press")}
                isDisabled={app.browserBusy}
              >
                Press
              </Button>
              <Button
                variant="secondary"
                onPress={() => void app.runBrowserSessionAction("select")}
                isDisabled={app.browserBusy}
              >
                Select
              </Button>
              <Button
                variant="secondary"
                onPress={() => void app.runBrowserSessionAction("highlight")}
                isDisabled={app.browserBusy}
              >
                Highlight
              </Button>
              <Button
                variant="secondary"
                onPress={() => void app.exportBrowserPdf()}
                isDisabled={app.browserBusy}
              >
                Export PDF
              </Button>
            </div>
            {failureScreenshot !== null ? (
              <img
                alt="Browser failure screenshot"
                className="workspace-media-preview"
                src={failureScreenshot}
              />
            ) : null}
            {lastActionResult !== null ? (
              <PrettyJsonBlock
                value={lastActionResult}
                revealSensitiveValues={app.revealSensitiveValues}
              />
            ) : (
              <p className="chat-muted">No browser action result captured yet.</p>
            )}
          </div>
        </WorkspaceSectionCard>

        <WorkspaceSectionCard
          title="Diagnostics and evidence"
          description="Console output, page diagnostics, retained downloads, and relay tokens stay together so operator recovery does not fragment."
        >
          <div className="workspace-stack">
            <PrettyJsonBlock
              value={app.browserConsoleDiagnostics ?? { entries: app.browserConsoleEntries }}
              revealSensitiveValues={app.revealSensitiveValues}
            />
            {app.browserPdfExport !== null ? (
              <PrettyJsonBlock
                value={app.browserPdfExport}
                revealSensitiveValues={app.revealSensitiveValues}
              />
            ) : null}
            {sessions.length > 0 ? (
              <div className="workspace-list">
                {sessions.slice(0, 6).map((session) => {
                  const record = asJsonObject(session);
                  const sessionId = readString(record ?? {}, "session_id") ?? "unknown";
                  return (
                    <article key={sessionId} className="workspace-list-item">
                      <div>
                        <strong>{sessionId}</strong>
                        <p className="chat-muted">
                          {readString(record ?? {}, "active_tab_title") ??
                            readString(record ?? {}, "active_tab_url") ??
                            "No active tab summary"}
                        </p>
                      </div>
                      <Button
                        size="sm"
                        variant="secondary"
                        onPress={() => app.openBrowserSessionWorkbench(sessionId)}
                      >
                        Open detail
                      </Button>
                    </article>
                  );
                })}
              </div>
            ) : null}
          </div>
        </WorkspaceSectionCard>
      </section>

      <section className="workspace-two-column">
        <WorkspaceSectionCard
          title="Profiles"
          description="Profile lifecycle stays available so shared versus isolated state remains explicit before you attach sessions to it."
        >
          <div className="workspace-stack">
            {profiles.length === 0 ? (
              <p className="chat-muted">No browser profiles loaded.</p>
            ) : (
              <div className="workspace-list">
                {profiles.slice(0, 8).map((profile) => {
                  const record = asJsonObject(profile);
                  const profileId = readString(record ?? {}, "profile_id") ?? "unknown";
                  const profileName = readString(record ?? {}, "name") ?? profileId;
                  const isActive = profileId === app.browserActiveProfileId;
                  return (
                    <article key={profileId} className="workspace-list-item">
                      <div>
                        <strong>{profileName}</strong>
                        <p className="chat-muted">
                          {readBool(record ?? {}, "private_profile") ? "isolated" : "shared"} ·{" "}
                          {readString(record ?? {}, "principal") ?? "n/a"}
                        </p>
                      </div>
                      <div className="workspace-inline">
                        <WorkspaceStatusChip tone={isActive ? "success" : "default"}>
                          {isActive ? "active" : "inactive"}
                        </WorkspaceStatusChip>
                      </div>
                      <div className="console-inline-actions">
                        <Button
                          size="sm"
                          variant="secondary"
                          onPress={() => {
                            app.setBrowserRenameProfileId(profileId);
                            app.setBrowserSessionProfileId(profileId);
                          }}
                        >
                          {`Select ${profileName}`}
                        </Button>
                        <Button
                          size="sm"
                          onPress={() => void app.activateBrowserProfile(record ?? {})}
                          isDisabled={app.browserBusy}
                        >
                          {`Activate ${profileName}`}
                        </Button>
                        <Button
                          size="sm"
                          variant="danger-soft"
                          onPress={() => void app.deleteBrowserProfile(record ?? {})}
                          isDisabled={app.browserBusy}
                        >
                          {`Delete ${profileName}`}
                        </Button>
                      </div>
                    </article>
                  );
                })}
              </div>
            )}

            <form
              className="workspace-form"
              onSubmit={(event) => void app.createBrowserProfile(event)}
            >
              <div className="workspace-form-grid">
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
                  label="Isolated profile"
                  onChange={app.setBrowserProfilePrivate}
                />
              </div>
              <Button type="submit" isDisabled={app.browserBusy}>
                Create profile
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
                Rename profile
              </Button>
            </form>
          </div>
        </WorkspaceSectionCard>

        <WorkspaceSectionCard
          title="Downloads and relay"
          description="Retained artifacts and relay tokens stay available for evidence collection and extension debugging."
        >
          <div className="workspace-stack">
            <div className="workspace-form-grid">
              <TextInputField
                label="Downloads session ID"
                value={app.browserDownloadsSessionId}
                onChange={app.setBrowserDownloadsSessionId}
              />
            </div>
            <Button
              variant="secondary"
              onPress={() => void app.refreshBrowserDownloads()}
              isDisabled={app.browserBusy}
            >
              Load downloads
            </Button>
            <PrettyJsonBlock
              value={{
                downloads: app.browserDownloads,
              }}
              revealSensitiveValues={app.revealSensitiveValues}
            />
            <div className="workspace-form-grid">
              <TextInputField
                label="Relay session ID"
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
            <Button onPress={() => void app.mintBrowserRelayToken()} isDisabled={app.browserBusy}>
              Mint relay token
            </Button>
            <div className="console-inline-actions">
              <Button
                variant={app.browserRelayAction === "capture_selection" ? "primary" : "secondary"}
                onPress={() => app.setBrowserRelayAction("capture_selection")}
                isDisabled={app.browserBusy}
              >
                Capture selection
              </Button>
              <Button
                variant={app.browserRelayAction === "send_page_snapshot" ? "primary" : "secondary"}
                onPress={() => app.setBrowserRelayAction("send_page_snapshot")}
                isDisabled={app.browserBusy}
              >
                Send page snapshot
              </Button>
              <Button
                variant={app.browserRelayAction === "open_tab" ? "primary" : "secondary"}
                onPress={() => app.setBrowserRelayAction("open_tab")}
                isDisabled={app.browserBusy}
              >
                Open tab relay
              </Button>
            </div>
            {app.browserRelayAction === "open_tab" ? (
              <TextInputField
                label="Relay tab URL"
                value={app.browserRelayOpenTabUrl}
                onChange={app.setBrowserRelayOpenTabUrl}
              />
            ) : null}
            {app.browserRelayAction === "capture_selection" ? (
              <TextInputField
                label="Relay selector"
                value={app.browserRelaySelector}
                onChange={app.setBrowserRelaySelector}
              />
            ) : null}
            <Button
              variant="secondary"
              onPress={() => void app.dispatchBrowserRelayAction()}
              isDisabled={app.browserBusy}
            >
              Dispatch relay action
            </Button>
            <PrettyJsonBlock
              value={{
                relay_action: app.browserRelayAction,
                relay_token: app.browserRelayToken,
                expires_at_unix_ms: app.browserRelayTokenExpiry,
                relay_result: app.browserRelayResult,
              }}
              revealSensitiveValues={app.revealSensitiveValues}
            />
          </div>
        </WorkspaceSectionCard>
      </section>
    </main>
  );
}

function asJsonObject(value: unknown): JsonObject | null {
  if (value !== null && typeof value === "object" && !Array.isArray(value)) {
    return value as JsonObject;
  }
  return null;
}

function deriveBrowserMode(
  summary: JsonObject | null,
  privateProfile: boolean,
  persistenceEnabled: boolean,
): { label: string; detail: string; tone: "default" | "success" | "warning" | "danger" } {
  const summaryPrivate = readBool(summary ?? {}, "private_profile") || privateProfile;
  const summaryPersistence = readBool(summary ?? {}, "persistence_enabled") || persistenceEnabled;
  const profileId = readString(summary ?? {}, "profile_id");
  if (summaryPrivate) {
    return {
      label: "isolated profile",
      detail: "State is isolated to this session.",
      tone: "success",
    };
  }
  if (profileId !== null && profileId.length > 0) {
    return {
      label: "shared profile",
      detail: "State can be reused across sessions.",
      tone: "warning",
    };
  }
  if (summaryPersistence) {
    return {
      label: "user-managed profile",
      detail: "Persistent state exists without an attached reusable profile record.",
      tone: "warning",
    };
  }
  return {
    label: "ephemeral session",
    detail: "No retained state is expected after close.",
    tone: "default",
  };
}

function buildRunbooks(
  result: JsonObject | null,
  diagnostics: JsonObject | null,
  pdfExport: JsonObject | null,
): string[] {
  const steps = new Set<string>();
  const error = readString(result ?? {}, "error")?.toLowerCase() ?? "";
  if (error.includes("selector")) {
    steps.add("Confirm the selector against the current DOM snapshot before retrying the action.");
  }
  if (error.includes("session_not_found")) {
    steps.add(
      "Refresh sessions or create a new debug session; the previous session expired or was closed.",
    );
  }
  if ((readNumber(diagnostics ?? {}, "error_count") ?? 0) > 0) {
    steps.add(
      "Review the page console errors first; page-side exceptions often explain failed actions.",
    );
  }
  if (readString(result ?? {}, "failure_screenshot_base64") !== null) {
    steps.add("Compare the failure screenshot with the expected visible state and focus target.");
  }
  if (pdfExport !== null) {
    steps.add(
      "Retain the exported PDF artifact in support notes if this failure needs escalation.",
    );
  }
  return Array.from(steps);
}

function asDataUri(mimeType: string | null, base64: string | null): string | null {
  if (mimeType === null || base64 === null) {
    return null;
  }
  if (mimeType.trim().length === 0 || base64.trim().length === 0) {
    return null;
  }
  return `data:${mimeType};base64,${base64}`;
}
