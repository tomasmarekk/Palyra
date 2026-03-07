import type { CapabilityCatalog } from "../../consoleApi";
import { capabilitiesByMode, capabilitiesForSection } from "../capabilityCatalog";
import { CapabilityCardList } from "../components/CapabilityCards";
import { ConsoleSectionHeader } from "../components/ConsoleSectionHeader";
import {
  formatUnixMs,
  isJsonObject,
  readBool,
  readString,
  toPrettyJson,
  toStringArray,
  type JsonObject,
} from "../shared";
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
    | "revealSensitiveValues"
    | "setSection"
  >;
};

export function AccessSection({ app }: AccessSectionProps) {
  const catalog = readCapabilityCatalog(app.overviewCatalog);
  const groupedCapabilities = capabilitiesByMode(capabilitiesForSection(catalog, "access"));
  const channels = readChannelSnapshots(app.supportPairingSummary);
  const pendingCount = channels.reduce((sum, channel) => sum + readArrayLength(channel, "pending"), 0);
  const pairedCount = channels.reduce((sum, channel) => sum + readArrayLength(channel, "paired"), 0);
  const activeCodeCount = channels.reduce((sum, channel) => sum + readArrayLength(channel, "active_codes"), 0);
  const deploymentWarnings = toStringArray(
    Array.isArray(app.supportDeployment?.warnings) ? app.supportDeployment.warnings : []
  );

  return (
    <main className="console-card">
      <ConsoleSectionHeader
        title="Pairing and Gateway Access"
        description="Operate DM pairing and understand remote gateway posture from one place. Verification and SSH tunnel flows remain visible here even when they intentionally stay CLI-driven."
        actions={(
          <button type="button" onClick={() => void app.refreshSupport()} disabled={app.supportBusy}>
            {app.supportBusy ? "Refreshing..." : "Refresh access"}
          </button>
        )}
      />

      <section className="console-grid-4 console-summary-grid">
        <article className="console-subpanel">
          <h3>Gateway posture</h3>
          <p><strong>Mode:</strong> {readString(app.supportDeployment ?? {}, "mode") ?? "n/a"}</p>
          <p><strong>Bind profile:</strong> {readString(app.supportDeployment ?? {}, "bind_profile") ?? "n/a"}</p>
          <p><strong>TLS:</strong> {readBool(readChild(app.supportDeployment, "tls"), "gateway_enabled") ? "enabled" : "disabled"}</p>
          <p><strong>Remote bind detected:</strong> {readBool(app.supportDeployment ?? {}, "remote_bind_detected") ? "yes" : "no"}</p>
        </article>
        <article className="console-subpanel">
          <h3>Pairing state</h3>
          <p><strong>Channels:</strong> {channels.length}</p>
          <p><strong>Pending:</strong> {pendingCount}</p>
          <p><strong>Paired:</strong> {pairedCount}</p>
          <p><strong>Active codes:</strong> {activeCodeCount}</p>
        </article>
        <article className="console-subpanel">
          <h3>Warnings</h3>
          {deploymentWarnings.length === 0 ? (
            <p>No deployment warnings were published.</p>
          ) : (
            <ul className="console-compact-list">
              {deploymentWarnings.map((warning) => (
                <li key={warning}>{warning}</li>
              ))}
            </ul>
          )}
        </article>
        <article className="console-subpanel">
          <h3>Operator shortcuts</h3>
          <div className="console-inline-actions">
            <button type="button" className="secondary" onClick={() => app.setSection("approvals")}>
              Open approvals
            </button>
            <button type="button" className="secondary" onClick={() => app.setSection("support")}>
              Open recovery
            </button>
          </div>
        </article>
      </section>

      <section className="console-grid-2">
        <article className="console-subpanel">
          <div className="console-subpanel__header">
            <div>
              <h3>Mint pairing code</h3>
              <p className="chat-muted">
                Pairing codes stay bounded by TTL and surface pending approval state when a DM request has not been approved yet.
              </p>
            </div>
          </div>
          <div className="console-grid-3">
            <label>
              Channel
              <input value={app.supportPairingChannel} onChange={(event) => app.setSupportPairingChannel(event.target.value)} />
            </label>
            <label>
              Issued by
              <input value={app.supportPairingIssuedBy} onChange={(event) => app.setSupportPairingIssuedBy(event.target.value)} />
            </label>
            <label>
              TTL ms
              <input value={app.supportPairingTtlMs} onChange={(event) => app.setSupportPairingTtlMs(event.target.value)} />
            </label>
          </div>
          <div className="console-inline-actions">
            <button type="button" onClick={() => void app.mintSupportPairingCode()} disabled={app.supportBusy}>
              {app.supportBusy ? "Minting..." : "Mint pairing code"}
            </button>
          </div>
        </article>

        <article className="console-subpanel">
          <div className="console-subpanel__header">
            <div>
              <h3>Published CLI handoffs</h3>
              <p className="chat-muted">
                Remote verification and SSH tunneling stay explicit instead of disappearing behind undocumented operator steps.
              </p>
            </div>
          </div>
          <CapabilityCardList
            entries={groupedCapabilities.cli_handoff}
            emptyMessage="No CLI handoffs are currently published for gateway access."
          />
        </article>
      </section>

      <section className="console-subpanel">
        <div className="console-subpanel__header">
          <div>
            <h3>Pairing channels</h3>
            <p className="chat-muted">
              Active codes, pending requests, and approved senders stay visible even before a message route is executed.
            </p>
          </div>
        </div>
        {channels.length === 0 ? (
          <p>No pairing channels loaded.</p>
        ) : (
          <div className="console-table-wrap">
            <table className="console-table">
              <thead>
                <tr>
                  <th>Channel</th>
                  <th>Pending</th>
                  <th>Paired</th>
                  <th>Active codes</th>
                  <th>Newest code expiry</th>
                </tr>
              </thead>
              <tbody>
                {channels.map((channel) => (
                  <tr key={readString(channel, "channel") ?? "unknown"}>
                    <td>{readString(channel, "channel") ?? "unknown"}</td>
                    <td>{readArrayLength(channel, "pending")}</td>
                    <td>{readArrayLength(channel, "paired")}</td>
                    <td>{readArrayLength(channel, "active_codes")}</td>
                    <td>{formatUnixMs(findNewestCodeExpiry(channel))}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <section className="console-grid-2">
        <article className="console-subpanel">
          <h3>Direct dashboard actions</h3>
          <CapabilityCardList
            entries={groupedCapabilities.direct_action}
            emptyMessage="No direct dashboard actions are published for access."
          />
        </article>
        <article className="console-subpanel">
          <h3>Redacted posture snapshot</h3>
          {app.supportDeployment === null ? (
            <p>No deployment posture loaded.</p>
          ) : (
            <pre>{toPrettyJson(app.supportDeployment, app.revealSensitiveValues)}</pre>
          )}
        </article>
      </section>
    </main>
  );
}

function readCapabilityCatalog(value: JsonObject | null): CapabilityCatalog | null {
  if (value === null || !Array.isArray(value.capabilities)) {
    return null;
  }
  return value as unknown as CapabilityCatalog;
}

function readChannelSnapshots(summary: JsonObject | null): JsonObject[] {
  if (summary === null || !Array.isArray(summary.channels)) {
    return [];
  }
  return summary.channels.filter(isJsonObject);
}

function readArrayLength(record: JsonObject, key: string): number {
  const value = record[key];
  return Array.isArray(value) ? value.length : 0;
}

function findNewestCodeExpiry(channel: JsonObject): number | null {
  const value = channel.active_codes;
  if (!Array.isArray(value)) {
    return null;
  }
  let newest: number | null = null;
  for (const entry of value) {
    if (!isJsonObject(entry)) {
      continue;
    }
    const expiresAt = entry.expires_at_unix_ms;
    if (typeof expiresAt !== "number" || !Number.isFinite(expiresAt)) {
      continue;
    }
    newest = newest === null ? expiresAt : Math.max(newest, expiresAt);
  }
  return newest;
}

function readChild(record: JsonObject | null, key: string): JsonObject {
  const value = record?.[key] ?? null;
  return isJsonObject(value) ? value : {};
}
