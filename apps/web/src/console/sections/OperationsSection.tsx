import type { CapabilityCatalog } from "../../consoleApi";
import { capabilitiesByMode, capabilitiesForSection } from "../capabilityCatalog";
import { CapabilityCardList } from "../components/CapabilityCards";
import { WorkspaceMetricCard, WorkspacePageHeader, WorkspaceSectionCard, WorkspaceStatusChip } from "../components/workspace/WorkspaceChrome";
import { WorkspaceEmptyState, WorkspaceInlineNotice, WorkspaceTable, workspaceToneForState } from "../components/workspace/WorkspacePatterns";
import { PrettyJsonBlock, formatUnixMs, readNumber, readObject, readString, toStringArray, type JsonObject } from "../shared";
import type { ConsoleAppState } from "../useConsoleAppState";

type OperationsSectionProps = {
  app: Pick<
    ConsoleAppState,
    | "auditBusy"
    | "auditFilterContains"
    | "setAuditFilterContains"
    | "auditFilterPrincipal"
    | "setAuditFilterPrincipal"
    | "auditEvents"
    | "refreshAudit"
    | "diagnosticsBusy"
    | "diagnosticsSnapshot"
    | "refreshDiagnostics"
    | "overviewCatalog"
    | "revealSensitiveValues"
  >;
};

export function OperationsSection({ app }: OperationsSectionProps) {
  const catalog = readCapabilityCatalog(app.overviewCatalog);
  const groupedCapabilities = capabilitiesByMode(capabilitiesForSection(catalog, "operations"));
  const diagnostics = app.diagnosticsSnapshot;
  const observability = readObject(diagnostics ?? {}, "observability");
  const modelProvider = readObject(diagnostics ?? {}, "model_provider");
  const authProfiles = readObject(diagnostics ?? {}, "auth_profiles");
  const browserd = readObject(diagnostics ?? {}, "browserd");
  const recentFailures = readJsonObjectArray(observability?.recent_failures);
  const connector = readObject(observability ?? {}, "connector");
  const browser = readObject(observability ?? {}, "browser");
  const browserFailureSamples = toStringArray(Array.isArray(browser?.recent_failure_samples) ? browser.recent_failure_samples : []);

  return (
    <main className="workspace-page">
      <WorkspacePageHeader
        eyebrow="Settings"
        title="Diagnostics"
        description="Technical detail now lives here instead of dominating Overview: runtime status, audit events, CLI handoffs, and bounded troubleshooting context."
        status={
          <>
            <WorkspaceStatusChip tone={workspaceToneForState(readString(modelProvider ?? {}, "state") ?? "unknown")}>
              Provider: {readString(modelProvider ?? {}, "state") ?? "unknown"}
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={recentFailures.length > 0 ? "warning" : "default"}>
              {recentFailures.length} recent failures
            </WorkspaceStatusChip>
          </>
        }
        actions={(
          <div className="workspace-inline">
            <button type="button" onClick={() => void app.refreshDiagnostics()} disabled={app.diagnosticsBusy}>{app.diagnosticsBusy ? "Refreshing..." : "Refresh diagnostics"}</button>
            <button type="button" className="secondary" onClick={() => void app.refreshAudit()} disabled={app.auditBusy}>{app.auditBusy ? "Refreshing..." : "Refresh audit"}</button>
          </div>
        )}
      />

      <section className="workspace-metric-grid">
        <WorkspaceMetricCard label="Model provider" value={readString(modelProvider ?? {}, "provider") ?? "n/a"} detail={readString(modelProvider ?? {}, "state") ?? "No provider state loaded."} tone={workspaceToneForState(readString(modelProvider ?? {}, "state") ?? "unknown")} />
        <WorkspaceMetricCard label="Auth state" value={readString(authProfiles ?? {}, "state") ?? "n/a"} detail={`${Array.isArray(authProfiles?.profiles) ? authProfiles.profiles.length : 0} profiles published`} tone={workspaceToneForState(readString(authProfiles ?? {}, "state") ?? "unknown")} />
        <WorkspaceMetricCard label="Browser service" value={readString(browserd ?? {}, "state") ?? "n/a"} detail={readString(browserd ?? {}, "engine_mode") ?? "No engine mode published."} tone={workspaceToneForState(readString(browserd ?? {}, "state") ?? "unknown")} />
        <WorkspaceMetricCard label="Connector dead letters" value={readString(connector ?? {}, "dead_letters") ?? "0"} detail={`${browserFailureSamples.length} browser relay failure samples published.`} tone={recentFailures.length > 0 ? "warning" : "default"} />
      </section>

      {recentFailures.length > 0 ? (
        <WorkspaceInlineNotice title="Recent failures" tone="warning">
          <ul className="console-compact-list">
            {recentFailures.slice(0, 4).map((failure, index) => (
              <li key={`${readString(failure, "operation") ?? "failure"}-${index}`}>
                <strong>{readString(failure, "failure_class") ?? "unknown"}</strong>: {readString(failure, "message") ?? readString(failure, "operation") ?? "No detail"}
              </li>
            ))}
          </ul>
        </WorkspaceInlineNotice>
      ) : null}

      <section className="workspace-aside-grid">
        <div className="workspace-stack">
          <WorkspaceSectionCard title="Audit events" description="Use quick filters near the table header and keep the actions column-free so the event stream stays readable.">
            <div className="workspace-form-grid">
              <label>Principal filter<input value={app.auditFilterPrincipal} onChange={(event) => app.setAuditFilterPrincipal(event.target.value)} /></label>
              <label>Payload contains<input value={app.auditFilterContains} onChange={(event) => app.setAuditFilterContains(event.target.value)} /></label>
            </div>
            {app.auditEvents.length === 0 ? (
              <WorkspaceEmptyState title="No audit events loaded" description="Refresh audit to load the current redacted event stream." compact />
            ) : (
              <WorkspaceTable ariaLabel="Audit events" columns={["When", "Event", "Principal", "Summary"]}>
                {app.auditEvents.map((event, index) => (
                  <tr key={`${readString(event, "event_type") ?? "event"}-${index}`}>
                    <td>{formatAuditTime(event)}</td>
                    <td>{formatAuditEventName(event)}</td>
                    <td>{readString(event, "principal") ?? "n/a"}</td>
                    <td>{formatAuditSummary(event)}</td>
                  </tr>
                ))}
              </WorkspaceTable>
            )}
          </WorkspaceSectionCard>
        </div>

        <div className="workspace-stack">
          <WorkspaceSectionCard title="CLI handoffs" description="Deeper troubleshooting remains explicit instead of hiding behind undocumented operator steps.">
            <CapabilityCardList entries={groupedCapabilities.cli_handoff} emptyMessage="No CLI handoffs are currently published for diagnostics." />
          </WorkspaceSectionCard>

          <WorkspaceSectionCard title="Internal notes" description="Keep internal-only capabilities visible so hidden power does not become accidental product surface.">
            <CapabilityCardList entries={groupedCapabilities.internal_only} emptyMessage="No internal-only capability notes are currently published for diagnostics." />
          </WorkspaceSectionCard>

          <WorkspaceSectionCard title="Diagnostics snapshot" description="Raw snapshot stays available as a secondary surface after the summary and tables.">
            {diagnostics === null ? (
              <WorkspaceEmptyState title="No diagnostics loaded" description="Refresh diagnostics to load the latest redacted snapshot." compact />
            ) : (
              <PrettyJsonBlock value={diagnostics} revealSensitiveValues={app.revealSensitiveValues} />
            )}
          </WorkspaceSectionCard>
        </div>
      </section>
    </main>
  );
}

function readCapabilityCatalog(value: JsonObject | null): CapabilityCatalog | null {
  return value !== null && Array.isArray(value.capabilities) ? value as unknown as CapabilityCatalog : null;
}

function readJsonObjectArray(value: unknown): JsonObject[] {
  return Array.isArray(value) ? value.filter((entry): entry is JsonObject => entry !== null && typeof entry === "object" && !Array.isArray(entry)) : [];
}

function formatAuditTime(event: JsonObject): string {
  return (
    formatUnixMs(
      readNumber(event, "timestamp_unix_ms") ??
      readNumber(event, "observed_at_unix_ms") ??
      readNumber(event, "created_at_unix_ms")
    ) ??
    readString(event, "occurred_at") ??
    readString(event, "created_at") ??
    "n/a"
  );
}

function formatAuditEventName(event: JsonObject): string {
  return (
    readString(event, "event_type") ??
    readString(event, "event") ??
    mapAuditKind(readNumber(event, "kind")) ??
    "unknown"
  );
}

function formatAuditSummary(event: JsonObject): string {
  const summary =
    readString(event, "message") ??
    readString(event, "summary") ??
    readString(event, "reason");
  if (summary !== null) {
    return summary;
  }

  if (event.payload !== undefined && event.payload !== null) {
    if (typeof event.payload === "string" || typeof event.payload === "number" || typeof event.payload === "boolean") {
      return String(event.payload);
    }
    if (typeof event.payload === "object" && !Array.isArray(event.payload)) {
      const entries = Object.entries(event.payload as Record<string, unknown>);
      if (entries.length > 0) {
        return entries.map(([key, value]) => `${key}: ${String(value)}`).join(", ");
      }
    }
  }

  return readString(event, "payload_json") ?? "No summary";
}

function mapAuditKind(kind: number | null): string | null {
  switch (kind) {
    case 1:
      return "message.received";
    case 2:
      return "model.token";
    case 3:
      return "tool.proposed";
    case 4:
      return "tool.executed";
    case 5:
      return "a2ui.updated";
    case 6:
      return "run.completed";
    case 7:
      return "run.failed";
    default:
      return null;
  }
}
