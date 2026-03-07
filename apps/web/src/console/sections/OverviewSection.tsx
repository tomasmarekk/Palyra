import type { CapabilityCatalog } from "../../consoleApi";
import {
  capabilitiesByMode,
  capabilityModeCounts,
  sectionCapabilityCounts,
} from "../capabilityCatalog";
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

type OverviewSectionProps = {
  app: Pick<
    ConsoleAppState,
    | "overviewBusy"
    | "overviewCatalog"
    | "overviewDeployment"
    | "overviewSupportJobs"
    | "revealSensitiveValues"
    | "refreshOverview"
    | "setSection"
  >;
};

export function OverviewSection({ app }: OverviewSectionProps) {
  const catalog = readCapabilityCatalog(app.overviewCatalog);
  const groupedCapabilities = capabilitiesByMode(catalog?.capabilities ?? []);
  const totalCounts = capabilityModeCounts(catalog?.capabilities ?? []);
  const deployment = app.overviewDeployment;
  const deploymentWarnings = deployment === null ? [] : toStringArray(Array.isArray(deployment.warnings) ? deployment.warnings : []);
  const activeSupportJob = app.overviewSupportJobs[0] ?? null;

  return (
    <main className="console-card">
      <ConsoleSectionHeader
        title="Overview"
        description="The dashboard is the canonical operator surface. Anything not directly executable here is surfaced as a CLI handoff or called out as intentionally internal-only."
        actions={(
          <button type="button" onClick={() => void app.refreshOverview()} disabled={app.overviewBusy}>
            {app.overviewBusy ? "Refreshing..." : "Refresh overview"}
          </button>
        )}
      />

      <section className="console-grid-4 console-summary-grid">
        <article className="console-subpanel">
          <h3>Capability exposure</h3>
          <p><strong>Total:</strong> {catalog?.capabilities.length ?? 0}</p>
          <p><strong>Direct actions:</strong> {totalCounts.direct_action}</p>
          <p><strong>CLI handoffs:</strong> {totalCounts.cli_handoff}</p>
          <p><strong>Internal only:</strong> {totalCounts.internal_only}</p>
        </article>
        <article className="console-subpanel">
          <h3>Deployment posture</h3>
          <p><strong>Mode:</strong> {readString(deployment ?? {}, "mode") ?? "n/a"}</p>
          <p><strong>Bind profile:</strong> {readString(deployment ?? {}, "bind_profile") ?? "n/a"}</p>
          <p><strong>Admin auth:</strong> {readBool(deployment ?? {}, "admin_auth_required") ? "required" : "unknown"}</p>
          <p><strong>Warnings:</strong> {deploymentWarnings.length}</p>
          <button type="button" className="secondary" onClick={() => app.setSection("access")}>
            Open access posture
          </button>
        </article>
        <article className="console-subpanel">
          <h3>Support queue</h3>
          <p><strong>Queued jobs:</strong> {app.overviewSupportJobs.length}</p>
          <p><strong>Latest job:</strong> {readString(activeSupportJob ?? {}, "job_id") ?? "n/a"}</p>
          <p><strong>Updated:</strong> {formatUnixMs(readUnixMillis(activeSupportJob, "requested_at_unix_ms"))}</p>
          <button type="button" className="secondary" onClick={() => app.setSection("support")}>
            Open support and recovery
          </button>
        </article>
        <article className="console-subpanel">
          <h3>High-value shortcuts</h3>
          <div className="console-inline-actions">
            <button type="button" className="secondary" onClick={() => app.setSection("auth")}>
              Open auth
            </button>
            <button type="button" className="secondary" onClick={() => app.setSection("config")}>
              Open config
            </button>
            <button type="button" className="secondary" onClick={() => app.setSection("operations")}>
              Open operations
            </button>
          </div>
        </article>
      </section>

      <section className="console-subpanel">
        <div className="console-subpanel__header">
          <div>
            <h3>Section coverage</h3>
            <p className="chat-muted">
              Each domain below reflects real backend capability entries from the M52 catalog, grouped by M56 exposure policy.
            </p>
          </div>
        </div>
        <div className="console-capability-summary-grid">
          {sectionCapabilityCounts(catalog).map((section) => (
            <article key={section.section} className="console-capability-summary-card">
              <h4>{section.label}</h4>
              <p><strong>Direct:</strong> {section.counts.direct_action}</p>
              <p><strong>CLI:</strong> {section.counts.cli_handoff}</p>
              <p><strong>Internal:</strong> {section.counts.internal_only}</p>
              <button type="button" className="secondary" onClick={() => app.setSection(section.section)}>
                Open section
              </button>
            </article>
          ))}
        </div>
      </section>

      <section className="console-grid-2">
        <article className="console-subpanel">
          <div className="console-subpanel__header">
            <div>
              <h3>CLI handoff surface</h3>
              <p className="chat-muted">
                These capabilities stay CLI-driven, but the dashboard still publishes the exact command shape so operators can discover them here.
              </p>
            </div>
          </div>
          <CapabilityCardList
            entries={groupedCapabilities.cli_handoff}
            emptyMessage="No CLI handoffs are published in the current capability catalog."
          />
        </article>

        <article className="console-subpanel">
          <div className="console-subpanel__header">
            <div>
              <h3>Internal-only capabilities</h3>
              <p className="chat-muted">
                Internal entries remain visible with a justification note so hidden operator surface area is explicit.
              </p>
            </div>
          </div>
          <CapabilityCardList
            entries={groupedCapabilities.internal_only}
            emptyMessage="No internal-only capability notes are published in the current capability catalog."
          />
        </article>
      </section>

      <section className="console-subpanel">
        <div className="console-subpanel__header">
          <div>
            <h3>Redacted deployment snapshot</h3>
            <p className="chat-muted">
              Remote bind warnings, TLS posture, and admin auth state remain redacted by default.
            </p>
          </div>
        </div>
        {deployment === null ? (
          <p>No deployment posture loaded.</p>
        ) : (
          <pre>{toPrettyJson(deployment, app.revealSensitiveValues)}</pre>
        )}
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

function readUnixMillis(record: JsonObject | null, key: string): number | null {
  if (record === null || !isJsonObject(record)) {
    return null;
  }
  const value = record[key];
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}
