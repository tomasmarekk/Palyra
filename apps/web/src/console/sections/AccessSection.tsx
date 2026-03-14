import type { CapabilityCatalog } from "../../consoleApi";
import { capabilitiesByMode, capabilitiesForSection } from "../capabilityCatalog";
import { CapabilityCardList } from "../components/CapabilityCards";
import { WorkspaceMetricCard, WorkspacePageHeader, WorkspaceSectionCard, WorkspaceStatusChip } from "../components/workspace/WorkspaceChrome";
import { WorkspaceEmptyState, WorkspaceInlineNotice, WorkspaceTable, workspaceToneForState } from "../components/workspace/WorkspacePatterns";
import { formatUnixMs, isJsonObject, readBool, readString, toStringArray, type JsonObject } from "../shared";
import type { ConsoleAppState } from "../useConsoleAppState";

type AccessSectionProps = {
  app: Pick<
    ConsoleAppState,
    | "supportBusy"
    | "supportPairingSummary"
    | "supportDeployment"
    | "supportPairingChannel"
    | "setSupportPairingChannel"
    | "supportPairingIssuedBy"
    | "setSupportPairingIssuedBy"
    | "supportPairingTtlMs"
    | "setSupportPairingTtlMs"
    | "refreshSupport"
    | "mintSupportPairingCode"
    | "overviewCatalog"
    | "setSection"
  >;
};

export function AccessSection({ app }: AccessSectionProps) {
  const catalog = readCapabilityCatalog(app.overviewCatalog);
  const groupedCapabilities = capabilitiesByMode(capabilitiesForSection(catalog, "access"));
  const channels = readChannelSnapshots(app.supportPairingSummary);
  const warnings = toStringArray(Array.isArray(app.supportDeployment?.warnings) ? app.supportDeployment.warnings : []);
  const pendingCount = channels.reduce((sum, channel) => sum + readArrayLength(channel, "pending"), 0);
  const pairedCount = channels.reduce((sum, channel) => sum + readArrayLength(channel, "paired"), 0);
  const codeCount = channels.reduce((sum, channel) => sum + readArrayLength(channel, "active_codes"), 0);

  return (
    <main className="workspace-page">
      <WorkspacePageHeader
        eyebrow="Settings"
        title="Access"
        description="Keep pairing and remote-access posture mostly informational, with only a few focused actions and explicit CLI handoffs where the backend remains operator-driven."
        status={
          <>
            <WorkspaceStatusChip tone={workspaceToneForState(readBool(app.supportDeployment ?? {}, "remote_bind_detected") ? "warning" : "ready")}>
              {readBool(app.supportDeployment ?? {}, "remote_bind_detected") ? "Remote bind detected" : "Loopback posture"}
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={warnings.length > 0 ? "warning" : "default"}>
              {warnings.length} warnings
            </WorkspaceStatusChip>
          </>
        }
        actions={(
          <button type="button" onClick={() => void app.refreshSupport()} disabled={app.supportBusy}>
            {app.supportBusy ? "Refreshing..." : "Refresh access"}
          </button>
        )}
      />

      <section className="workspace-metric-grid workspace-metric-grid--compact">
        <WorkspaceMetricCard label="Channels" value={channels.length} detail="Channels with pending requests, active codes, or grants." />
        <WorkspaceMetricCard label="Pending requests" value={pendingCount} detail="Requests waiting on pairing and approval state." tone={pendingCount > 0 ? "warning" : "default"} />
        <WorkspaceMetricCard label="Active codes" value={codeCount} detail={`${pairedCount} paired sender grants currently visible.`} tone={codeCount > 0 ? "success" : "default"} />
      </section>

      {warnings.length > 0 ? (
        <WorkspaceInlineNotice title="Deployment posture" tone="warning">
          <ul className="console-compact-list">{warnings.map((warning) => <li key={warning}>{warning}</li>)}</ul>
        </WorkspaceInlineNotice>
      ) : null}

      <section className="workspace-aside-grid">
        <div className="workspace-stack">
          <WorkspaceSectionCard title="Pairing channels" description="Keep pending requests, grants, and code expiry visible before any route is actually exercised.">
            {channels.length === 0 ? (
              <WorkspaceEmptyState title="No pairing channels loaded" description="Refresh access to load current pairing summary and active code state." />
            ) : (
              <WorkspaceTable ariaLabel="Pairing channels" columns={["Channel", "Pending", "Paired", "Codes", "Newest expiry"]}>
                {channels.map((channel) => (
                  <tr key={readString(channel, "channel") ?? "unknown"}>
                    <td>{readString(channel, "channel") ?? "unknown"}</td>
                    <td>{readArrayLength(channel, "pending")}</td>
                    <td>{readArrayLength(channel, "paired")}</td>
                    <td>{readArrayLength(channel, "active_codes")}</td>
                    <td>{formatUnixMs(findNewestCodeExpiry(channel))}</td>
                  </tr>
                ))}
              </WorkspaceTable>
            )}
          </WorkspaceSectionCard>

          <WorkspaceSectionCard title="Mint pairing code" description="Pairing codes stay bounded by TTL and visible as an operator-controlled action instead of an implicit side effect.">
            <div className="workspace-form-grid">
              <label>Channel<input value={app.supportPairingChannel} onChange={(event) => app.setSupportPairingChannel(event.target.value)} /></label>
              <label>Issued by<input value={app.supportPairingIssuedBy} onChange={(event) => app.setSupportPairingIssuedBy(event.target.value)} /></label>
              <label>TTL ms<input value={app.supportPairingTtlMs} onChange={(event) => app.setSupportPairingTtlMs(event.target.value)} /></label>
            </div>
            <div className="workspace-inline">
              <button type="button" onClick={() => void app.mintSupportPairingCode()} disabled={app.supportBusy}>
                {app.supportBusy ? "Minting..." : "Mint pairing code"}
              </button>
              <button type="button" className="secondary" onClick={() => app.setSection("approvals")}>Open approvals</button>
            </div>
          </WorkspaceSectionCard>
        </div>

        <div className="workspace-stack">
          <WorkspaceSectionCard title="Remote posture" description="Make the gateway mode, bind profile, and auth gates obvious before a remote operator attempts verification or tunneling.">
            <dl className="workspace-key-value-grid">
              <div><dt>Mode</dt><dd>{readString(app.supportDeployment ?? {}, "mode") ?? "n/a"}</dd></div>
              <div><dt>Bind profile</dt><dd>{readString(app.supportDeployment ?? {}, "bind_profile") ?? "n/a"}</dd></div>
              <div><dt>TLS</dt><dd>{readBool(readChild(app.supportDeployment, "tls"), "gateway_enabled") ? "enabled" : "disabled"}</dd></div>
              <div><dt>Admin auth</dt><dd>{readBool(app.supportDeployment ?? {}, "admin_auth_required") ? "required" : "unknown"}</dd></div>
            </dl>
          </WorkspaceSectionCard>

          <WorkspaceSectionCard title="CLI handoffs" description="Remote verify and tunnel flows stay documented here when they are intentionally not browser-native actions.">
            <CapabilityCardList entries={groupedCapabilities.cli_handoff} emptyMessage="No CLI handoffs are currently published for access." />
          </WorkspaceSectionCard>

          <WorkspaceSectionCard title="Direct actions" description="Only current dashboard actions stay here; deeper remote workflows remain explicit handoffs.">
            <CapabilityCardList entries={groupedCapabilities.direct_action} emptyMessage="No direct dashboard actions are currently published for access." />
          </WorkspaceSectionCard>
        </div>
      </section>
    </main>
  );
}

function readCapabilityCatalog(value: JsonObject | null): CapabilityCatalog | null {
  return value !== null && Array.isArray(value.capabilities) ? value as unknown as CapabilityCatalog : null;
}

function readChannelSnapshots(summary: JsonObject | null): JsonObject[] {
  return summary !== null && Array.isArray(summary.channels) ? summary.channels.filter(isJsonObject) : [];
}

function readArrayLength(record: JsonObject, key: string): number {
  return Array.isArray(record[key]) ? record[key].length : 0;
}

function findNewestCodeExpiry(channel: JsonObject): number | null {
  if (!Array.isArray(channel.active_codes)) {
    return null;
  }
  let newest: number | null = null;
  for (const entry of channel.active_codes) {
    if (!isJsonObject(entry) || typeof entry.expires_at_unix_ms !== "number") {
      continue;
    }
    newest = newest === null ? entry.expires_at_unix_ms : Math.max(newest, entry.expires_at_unix_ms);
  }
  return newest;
}

function readChild(record: JsonObject | null, key: string): JsonObject {
  return isJsonObject(record?.[key] ?? null) ? record?.[key] as JsonObject : {};
}
