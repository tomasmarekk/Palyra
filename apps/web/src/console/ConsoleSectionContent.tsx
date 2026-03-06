import { ChatConsolePanel } from "../chat/ChatConsolePanel";
import { AuthSection } from "./sections/AuthSection";
import { ConfigSection } from "./sections/ConfigSection";
import { DiagnosticsSection } from "./sections/DiagnosticsSection";
import { OverviewSection } from "./sections/OverviewSection";
import { SupportSection } from "./sections/SupportSection";
import type { ConsoleAppState } from "./useConsoleAppState";
import {
  DiscordOnboardingHighlights,
  channelConnectorAvailability,
  readBool,
  readString,
  toPrettyJson
} from "./shared";

type ConsoleSectionContentProps = {
  app: ConsoleAppState;
};

export function ConsoleSectionContent({ app }: ConsoleSectionContentProps) {
  switch (app.section) {
    case "overview":
      return <OverviewSection app={app} />;
    case "chat":
      return (
        <ChatConsolePanel
          api={app.api}
          revealSensitiveValues={app.revealSensitiveValues}
          setError={app.setError}
          setNotice={app.setNotice}
        />
      );
    case "auth":
      return <AuthSection app={app} />;
    case "approvals":
      return (
        <main className="console-card">
          <header className="console-card__header">
            <h2>Approvals</h2>
            <button type="button" onClick={() => void app.refreshApprovals()} disabled={app.approvalsBusy}>
              {app.approvalsBusy ? "Refreshing..." : "Refresh"}
            </button>
          </header>
          <div className="console-table-wrap">
            <table className="console-table">
              <thead>
                <tr>
                  <th>Approval ID</th>
                  <th>Subject</th>
                  <th>Decision</th>
                  <th>Requested</th>
                  <th>Action</th>
                </tr>
              </thead>
              <tbody>
                {app.approvals.length === 0 && (
                  <tr>
                    <td colSpan={5}>No approvals found.</td>
                  </tr>
                )}
                {app.approvals.map((approval) => {
                  const id = readString(approval, "approval_id") ?? "(missing)";
                  return (
                    <tr key={id}>
                      <td>{id}</td>
                      <td>{readString(approval, "subject_type") ?? "-"}</td>
                      <td>{readString(approval, "decision") ?? "-"}</td>
                      <td>{readString(approval, "requested_at_unix_ms") ?? "-"}</td>
                      <td><button type="button" onClick={() => app.setApprovalId(id)}>Select</button></td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
          <section className="console-subpanel">
            <h3>Decision</h3>
            <div className="console-grid-3">
              <label>
                Approval ID
                <input value={app.approvalId} onChange={(event) => app.setApprovalId(event.target.value)} />
              </label>
              <label>
                Reason
                <input value={app.approvalReason} onChange={(event) => app.setApprovalReason(event.target.value)} />
              </label>
              <label>
                Scope
                <select value={app.approvalScope} onChange={(event) => app.setApprovalScope(event.target.value)}>
                  <option value="once">once</option>
                  <option value="session">session</option>
                  <option value="timeboxed">timeboxed</option>
                </select>
              </label>
            </div>
            <div className="console-inline-actions">
              <button type="button" onClick={() => void app.decideApproval(true)}>Approve</button>
              <button type="button" className="button--warn" onClick={() => void app.decideApproval(false)}>Reject</button>
            </div>
          </section>
        </main>
      );
    case "cron":
      return (
        <main className="console-card">
          <header className="console-card__header">
            <h2>Cron</h2>
            <button type="button" onClick={() => void app.refreshCron()} disabled={app.cronBusy}>
              {app.cronBusy ? "Refreshing..." : "Refresh"}
            </button>
          </header>
          <form className="console-form" onSubmit={(event) => void app.createCron(event)}>
            <div className="console-grid-3">
              <label>
                Name
                <input value={app.cronForm.name} onChange={(event) => app.setCronForm((previous) => ({ ...previous, name: event.target.value }))} />
              </label>
              <label>
                Prompt
                <textarea value={app.cronForm.prompt} onChange={(event) => app.setCronForm((previous) => ({ ...previous, prompt: event.target.value }))} rows={3} />
              </label>
              <label>
                Channel
                <input value={app.cronForm.channel} onChange={(event) => app.setCronForm((previous) => ({ ...previous, channel: event.target.value }))} />
              </label>
            </div>
            <div className="console-grid-4">
              <label>
                Schedule Type
                <select value={app.cronForm.scheduleType} onChange={(event) => app.setCronForm((previous) => ({ ...previous, scheduleType: event.target.value as "cron" | "every" | "at" }))}>
                  <option value="every">every</option>
                  <option value="cron">cron</option>
                  <option value="at">at</option>
                </select>
              </label>
              <label>
                Every interval (ms)
                <input value={app.cronForm.everyIntervalMs} onChange={(event) => app.setCronForm((previous) => ({ ...previous, everyIntervalMs: event.target.value }))} />
              </label>
              <label>
                Cron expression
                <input value={app.cronForm.cronExpression} onChange={(event) => app.setCronForm((previous) => ({ ...previous, cronExpression: event.target.value }))} />
              </label>
              <label>
                At timestamp
                <input value={app.cronForm.atTimestampRfc3339} onChange={(event) => app.setCronForm((previous) => ({ ...previous, atTimestampRfc3339: event.target.value }))} />
              </label>
            </div>
            <button type="submit" disabled={app.cronBusy}>{app.cronBusy ? "Creating..." : "Create job"}</button>
          </form>
          <div className="console-table-wrap">
            <table className="console-table">
              <thead>
                <tr>
                  <th>Job ID</th>
                  <th>Name</th>
                  <th>Enabled</th>
                  <th>Action</th>
                </tr>
              </thead>
              <tbody>
                {app.cronJobs.length === 0 && <tr><td colSpan={4}>No cron jobs found.</td></tr>}
                {app.cronJobs.map((job) => {
                  const id = readString(job, "job_id") ?? "(missing)";
                  const enabled = readBool(job, "enabled");
                  return (
                    <tr key={id}>
                      <td>{id}</td>
                      <td>{readString(job, "name") ?? "-"}</td>
                      <td>{enabled ? "yes" : "no"}</td>
                      <td className="console-action-cell">
                        <button type="button" onClick={() => app.setCronJobId(id)}>Select</button>
                        <button type="button" onClick={() => void app.toggleCron(job, !enabled)}>{enabled ? "Disable" : "Enable"}</button>
                        <button type="button" onClick={() => void app.runCronNow(job)}>Run now</button>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </main>
      );
    case "channels":
      return (
        <main className="console-card">
          <header className="console-card__header">
            <h2>Channel Connectors</h2>
            <button type="button" onClick={() => void app.refreshChannels()} disabled={app.channelsBusy}>
              {app.channelsBusy ? "Refreshing..." : "Refresh"}
            </button>
          </header>
          <section className="console-subpanel">
            <h3>Discord Onboarding Wizard (M45)</h3>
            <div className="console-grid-3">
              <label>
                1) Mode
                <select value={app.discordWizardMode} onChange={(event) => app.setDiscordWizardMode(event.target.value === "remote_vps" ? "remote_vps" : "local")}>
                  <option value="local">local</option>
                  <option value="remote_vps">remote_vps</option>
                </select>
              </label>
              <label>
                2) Discord bot token
                <input value={app.discordWizardToken} onChange={(event) => app.setDiscordWizardToken(event.target.value)} placeholder="Paste token (never persisted in config)" />
              </label>
              <label>
                3) Optional verify channel ID
                <input value={app.discordWizardVerifyChannelId} onChange={(event) => app.setDiscordWizardVerifyChannelId(event.target.value)} />
              </label>
            </div>
            <div className="console-inline-actions">
              <button type="button" onClick={() => void app.runDiscordPreflight()} disabled={app.discordWizardBusy}>
                {app.discordWizardBusy ? "Running..." : "Run preflight"}
              </button>
              <button type="button" onClick={() => void app.applyDiscordOnboarding()} disabled={app.discordWizardBusy}>
                {app.discordWizardBusy ? "Applying..." : "Apply config"}
              </button>
            </div>
            {app.discordWizardPreflight !== null && (
              <>
                <DiscordOnboardingHighlights title="Preflight highlights" payload={app.discordWizardPreflight} />
                <section className="console-subpanel">
                  <h4>Preflight snapshot</h4>
                  <pre>{toPrettyJson(app.discordWizardPreflight, app.revealSensitiveValues)}</pre>
                </section>
              </>
            )}
          </section>
          <div className="console-table-wrap">
            <table className="console-table">
              <thead>
                <tr>
                  <th>Connector ID</th>
                  <th>Kind</th>
                  <th>Availability</th>
                  <th>Enabled</th>
                  <th>Readiness</th>
                  <th>Liveness</th>
                  <th>Action</th>
                </tr>
              </thead>
              <tbody>
                {app.channelsConnectors.length === 0 && <tr><td colSpan={7}>No channel connectors configured.</td></tr>}
                {app.channelsConnectors.map((connector) => {
                  const connectorId = readString(connector, "connector_id") ?? "(missing)";
                  const enabled = readBool(connector, "enabled");
                  return (
                    <tr key={connectorId}>
                      <td>{connectorId}</td>
                      <td>{readString(connector, "kind") ?? "-"}</td>
                      <td>{channelConnectorAvailability(connector)}</td>
                      <td>{enabled ? "yes" : "no"}</td>
                      <td>{readString(connector, "readiness") ?? "-"}</td>
                      <td>{readString(connector, "liveness") ?? "-"}</td>
                      <td className="console-action-cell">
                        <button type="button" onClick={() => void app.selectChannelConnector(connectorId)}>Select</button>
                        <button type="button" onClick={() => void app.toggleConnector(connector, !enabled)}>{enabled ? "Disable" : "Enable"}</button>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
          <section className="console-subpanel">
            <h3>Selected connector status</h3>
            {app.channelsSelectedStatus === null ? <p>Select a connector to inspect status, events, and dead letters.</p> : <pre>{toPrettyJson(app.channelsSelectedStatus, app.revealSensitiveValues)}</pre>}
          </section>
          {readString(app.channelsSelectedStatus ?? {}, "kind") === "discord" && (
            <section className="console-subpanel">
              <h3>Discord direct outbound verification</h3>
              <form className="console-form" onSubmit={(event) => void app.sendDiscordTest(event)}>
                <div className="console-grid-4">
                  <label>
                    Target
                    <input value={app.channelsDiscordTarget} onChange={(event) => app.setChannelsDiscordTarget(event.target.value)} />
                  </label>
                  <label>
                    Text
                    <input value={app.channelsDiscordText} onChange={(event) => app.setChannelsDiscordText(event.target.value)} />
                  </label>
                  <label>
                    Auto reaction
                    <input value={app.channelsDiscordAutoReaction} onChange={(event) => app.setChannelsDiscordAutoReaction(event.target.value)} />
                  </label>
                  <label>
                    Thread ID
                    <input value={app.channelsDiscordThreadId} onChange={(event) => app.setChannelsDiscordThreadId(event.target.value)} />
                  </label>
                </div>
                <label className="console-checkbox-inline">
                  <input type="checkbox" checked={app.channelsDiscordConfirm} onChange={(event) => app.setChannelsDiscordConfirm(event.target.checked)} />
                  Confirm Discord outbound test send
                </label>
                <button type="submit" disabled={app.channelsBusy}>{app.channelsBusy ? "Sending..." : "Send Discord test"}</button>
              </form>
            </section>
          )}
        </main>
      );
    case "memory":
      return (
        <main className="console-card">
          <header className="console-card__header">
            <h2>Memory</h2>
            <button type="button" onClick={() => void app.refreshMemoryStatus()} disabled={app.memoryStatusBusy}>
              {app.memoryStatusBusy ? "Refreshing..." : "Refresh status"}
            </button>
          </header>
          <section className="console-subpanel">
            <h3>Retention + Maintenance</h3>
            {app.memoryStatus === null ? <p>No memory status loaded.</p> : <pre>{toPrettyJson(app.memoryStatus, app.revealSensitiveValues)}</pre>}
          </section>
          <form className="console-form" onSubmit={(event) => void app.searchMemory(event)}>
            <div className="console-grid-3">
              <label>
                Query
                <input value={app.memoryQuery} onChange={(event) => app.setMemoryQuery(event.target.value)} />
              </label>
              <label>
                Channel
                <input value={app.memoryChannel} onChange={(event) => app.setMemoryChannel(event.target.value)} />
              </label>
              <button type="submit" disabled={app.memoryBusy}>{app.memoryBusy ? "Searching..." : "Search"}</button>
            </div>
          </form>
          {app.memoryHits.length === 0 ? <p>No memory hits loaded.</p> : <pre>{toPrettyJson(app.memoryHits, app.revealSensitiveValues)}</pre>}
        </main>
      );
    case "skills":
      return (
        <main className="console-card">
          <header className="console-card__header">
            <h2>Skills</h2>
            <button type="button" onClick={() => void app.refreshSkills()} disabled={app.skillsBusy}>
              {app.skillsBusy ? "Refreshing..." : "Refresh"}
            </button>
          </header>
          <form className="console-form" onSubmit={(event) => void app.installSkill(event)}>
            <div className="console-grid-4">
              <label>
                Artifact path
                <input value={app.skillArtifactPath} onChange={(event) => app.setSkillArtifactPath(event.target.value)} />
              </label>
              <label className="console-checkbox-inline">
                <input type="checkbox" checked={app.skillAllowTofu} onChange={(event) => app.setSkillAllowTofu(event.target.checked)} />
                Allow TOFU
              </label>
              <label className="console-checkbox-inline">
                <input type="checkbox" checked={app.skillAllowUntrusted} onChange={(event) => app.setSkillAllowUntrusted(event.target.checked)} />
                Allow untrusted
              </label>
              <button type="submit" disabled={app.skillsBusy}>{app.skillsBusy ? "Installing..." : "Install skill"}</button>
            </div>
          </form>
          {app.skillsEntries.length === 0 ? <p>No skills installed.</p> : <pre>{toPrettyJson(app.skillsEntries, app.revealSensitiveValues)}</pre>}
        </main>
      );
    case "browser":
      return (
        <main className="console-card">
          <header className="console-card__header">
            <h2>Browser Profiles + Relay</h2>
            <button type="button" onClick={() => void app.refreshBrowserProfiles()} disabled={app.browserBusy}>
              {app.browserBusy ? "Refreshing..." : "Refresh"}
            </button>
          </header>
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
          </form>
          <section className="console-subpanel">
            <h3>Profiles</h3>
            {app.browserProfiles.length === 0 ? <p>No browser profiles available.</p> : <pre>{toPrettyJson(app.browserProfiles, app.revealSensitiveValues)}</pre>}
          </section>
          <section className="console-subpanel">
            <h3>Extension relay token</h3>
            <div className="console-grid-3">
              <label>
                Session ID
                <input value={app.browserRelaySessionId} onChange={(event) => app.setBrowserRelaySessionId(event.target.value)} placeholder="01ARZ3NDEKTSV4RRFFQ69G5FAV" />
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
            <button type="button" onClick={() => void app.mintBrowserRelayToken()} disabled={app.browserBusy}>
              {app.browserBusy ? "Minting..." : "Mint relay token"}
            </button>
            {app.browserRelayToken.length > 0 && <pre>{toPrettyJson({ relay_token: app.browserRelayToken }, app.revealSensitiveValues)}</pre>}
          </section>
        </main>
      );
    case "config":
      return <ConfigSection app={app} />;
    case "diagnostics":
      return <DiagnosticsSection app={app} />;
    case "support":
      return <SupportSection app={app} />;
    case "audit":
      return (
        <main className="console-card">
          <header className="console-card__header">
            <h2>Audit</h2>
            <button type="button" onClick={() => void app.refreshAudit()} disabled={app.auditBusy}>
              {app.auditBusy ? "Refreshing..." : "Refresh"}
            </button>
          </header>
          <div className="console-grid-2">
            <label>
              Principal filter
              <input value={app.auditFilterPrincipal} onChange={(event) => app.setAuditFilterPrincipal(event.target.value)} />
            </label>
            <label>
              Payload contains
              <input value={app.auditFilterContains} onChange={(event) => app.setAuditFilterContains(event.target.value)} />
            </label>
          </div>
          {app.auditEvents.length === 0 ? <p>No audit events loaded.</p> : <pre>{toPrettyJson(app.auditEvents, app.revealSensitiveValues)}</pre>}
        </main>
      );
    default:
      return null;
  }
}
