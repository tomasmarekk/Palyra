import { ConsoleSectionHeader } from "../components/ConsoleSectionHeader";
import { formatUnixMs, readBool, readString, toPrettyJson } from "../shared";
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
  const relayToken = typeof app.browserRelayToken === "string" ? app.browserRelayToken : "";
  const downloads = Array.isArray(app.browserDownloads) ? app.browserDownloads : [];

  return (
    <main className="console-card">
      <ConsoleSectionHeader
        title="Browser"
        description="Manage profiles, mint relay tokens, dispatch extension actions, and inspect download artifacts from the dashboard."
        actions={(
          <button type="button" onClick={() => void app.refreshBrowserProfiles()} disabled={app.browserBusy}>
            {app.browserBusy ? "Refreshing..." : "Refresh browser"}
          </button>
        )}
      />

      <section className="console-grid-2">
        <article className="console-subpanel">
          <h3>Create profile</h3>
          <form className="console-form" onSubmit={(event) => void app.createBrowserProfile(event)}>
            <div className="console-grid-4">
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
              <button type="submit" disabled={app.browserBusy}>{app.browserBusy ? "Creating..." : "Create profile"}</button>
            </div>
            <div className="console-inline-actions">
              <label className="console-checkbox-inline">
                <input type="checkbox" checked={app.browserProfilePersistence} onChange={(event) => app.setBrowserProfilePersistence(event.target.checked)} />
                Persistence enabled
              </label>
              <label className="console-checkbox-inline">
                <input type="checkbox" checked={app.browserProfilePrivate} onChange={(event) => app.setBrowserProfilePrivate(event.target.checked)} />
                Private profile
              </label>
            </div>
          </form>
        </article>
        <article className="console-subpanel">
          <h3>Rename profile</h3>
          <div className="console-grid-3">
            <label>
              Profile ID
              <input value={app.browserRenameProfileId} onChange={(event) => app.setBrowserRenameProfileId(event.target.value)} />
            </label>
            <label>
              New name
              <input value={app.browserRenameName} onChange={(event) => app.setBrowserRenameName(event.target.value)} />
            </label>
            <div className="console-inline-actions">
              <button type="button" onClick={() => void app.renameBrowserProfile()} disabled={app.browserBusy}>
                {app.browserBusy ? "Renaming..." : "Rename profile"}
              </button>
            </div>
          </div>
        </article>
      </section>

      <section className="console-subpanel">
        <div className="console-subpanel__header">
          <div>
            <h3>Profiles</h3>
            <p className="chat-muted">
              Active profile, persistence, and private-profile posture remain visible before opening a session.
            </p>
          </div>
        </div>
        {profiles.length === 0 ? (
          <p>No browser profiles available.</p>
        ) : (
          <div className="console-table-wrap">
            <table className="console-table">
              <thead>
                <tr>
                  <th>Profile ID</th>
                  <th>Name</th>
                  <th>Principal</th>
                  <th>Persistence</th>
                  <th>Private</th>
                  <th>Action</th>
                </tr>
              </thead>
              <tbody>
                {profiles.map((profile) => {
                  const profileId = readString(profile, "profile_id") ?? "unknown";
                  const isActive = profileId === app.browserActiveProfileId;
                  return (
                    <tr key={profileId}>
                      <td>{profileId}{isActive ? " (active)" : ""}</td>
                      <td>{readString(profile, "name") ?? "-"}</td>
                      <td>{readString(profile, "principal") ?? "-"}</td>
                      <td>{readBool(profile, "persistence_enabled") ? "yes" : "no"}</td>
                      <td>{readBool(profile, "private_profile") ? "yes" : "no"}</td>
                      <td className="console-action-cell">
                        <button type="button" className="secondary" onClick={() => app.setBrowserRenameProfileId(profileId)}>
                          Select
                        </button>
                        <button type="button" onClick={() => void app.activateBrowserProfile(profile)}>Activate</button>
                        <button type="button" className="button--warn" onClick={() => void app.deleteBrowserProfile(profile)}>Delete</button>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <section className="console-grid-2">
        <article className="console-subpanel">
          <h3>Relay token and actions</h3>
          <div className="console-grid-3">
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
            <button type="button" onClick={() => void app.mintBrowserRelayToken()} disabled={app.browserBusy}>
              {app.browserBusy ? "Minting..." : "Mint relay token"}
            </button>
            <span className="chat-muted">Expires: {formatUnixMs(app.browserRelayTokenExpiry)}</span>
          </div>
          {relayToken.length > 0 && (
            <pre>{toPrettyJson({ relay_token: relayToken }, app.revealSensitiveValues)}</pre>
          )}
          <div className="console-grid-3">
            <label>
              Action
              <select
                value={app.browserRelayAction}
                onChange={(event) =>
                  app.setBrowserRelayAction(event.target.value as "open_tab" | "capture_selection" | "send_page_snapshot")
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
          <button type="button" onClick={() => void app.dispatchBrowserRelayAction()} disabled={app.browserBusy}>
            {app.browserBusy ? "Dispatching..." : "Dispatch relay action"}
          </button>
          {app.browserRelayResult !== null && <pre>{toPrettyJson(app.browserRelayResult, app.revealSensitiveValues)}</pre>}
        </article>

        <article className="console-subpanel">
          <h3>Download artifacts</h3>
          <div className="console-grid-2">
            <label>
              Session ID
              <input value={app.browserDownloadsSessionId} onChange={(event) => app.setBrowserDownloadsSessionId(event.target.value)} />
            </label>
            <label className="console-checkbox-inline">
              <input type="checkbox" checked={app.browserDownloadsQuarantinedOnly} onChange={(event) => app.setBrowserDownloadsQuarantinedOnly(event.target.checked)} />
              Quarantined only
            </label>
          </div>
          <button type="button" onClick={() => void app.refreshBrowserDownloads()} disabled={app.browserBusy}>
            {app.browserBusy ? "Loading..." : "Load downloads"}
          </button>
          {downloads.length === 0 ? (
            <p>No browser downloads loaded.</p>
          ) : (
            <pre>{toPrettyJson(downloads, app.revealSensitiveValues)}</pre>
          )}
        </article>
      </section>
    </main>
  );
}
