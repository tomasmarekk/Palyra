import type { CapabilityCatalog } from "../../consoleApi";
import { capabilitiesByMode, capabilitiesForSection } from "../capabilityCatalog";
import { CapabilityCardList } from "../components/CapabilityCards";
import { ConsoleSectionHeader } from "../components/ConsoleSectionHeader";
import {
  readObject,
  readString,
  toPrettyJson,
  type JsonObject,
} from "../shared";
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
  const modelProvider = readObject(diagnostics ?? {}, "model_provider");
  const rateLimits = readObject(diagnostics ?? {}, "rate_limits");
  const authProfiles = readObject(diagnostics ?? {}, "auth_profiles");
  const browserd = readObject(diagnostics ?? {}, "browserd");

  return (
    <main className="console-card">
      <ConsoleSectionHeader
        title="Diagnostics and Audit"
        description="Runtime status, audit browsing, developer diagnostics handoffs, and internal-only capability justifications live together in the operations domain."
        actions={(
          <div className="console-inline-actions">
            <button type="button" onClick={() => void app.refreshDiagnostics()} disabled={app.diagnosticsBusy}>
              {app.diagnosticsBusy ? "Refreshing diagnostics..." : "Refresh diagnostics"}
            </button>
            <button type="button" onClick={() => void app.refreshAudit()} disabled={app.auditBusy}>
              {app.auditBusy ? "Refreshing audit..." : "Refresh audit"}
            </button>
          </div>
        )}
      />

      <section className="console-grid-4 console-summary-grid">
        <article className="console-subpanel">
          <h3>Model provider</h3>
          <p><strong>State:</strong> {readString(modelProvider ?? {}, "state") ?? "n/a"}</p>
          <p><strong>Provider:</strong> {readString(modelProvider ?? {}, "provider") ?? "n/a"}</p>
        </article>
        <article className="console-subpanel">
          <h3>Rate limits</h3>
          <p><strong>Request budget:</strong> {readString(rateLimits ?? {}, "request_budget") ?? "n/a"}</p>
          <p><strong>Reset:</strong> {readString(rateLimits ?? {}, "reset_at_unix_ms") ?? "n/a"}</p>
        </article>
        <article className="console-subpanel">
          <h3>Auth health</h3>
          <p><strong>Summary state:</strong> {readString(authProfiles ?? {}, "state") ?? "n/a"}</p>
          <p><strong>Profiles:</strong> {Array.isArray(authProfiles?.profiles) ? authProfiles.profiles.length : 0}</p>
        </article>
        <article className="console-subpanel">
          <h3>Browser service</h3>
          <p><strong>State:</strong> {readString(browserd ?? {}, "state") ?? "n/a"}</p>
          <p><strong>Mode:</strong> {readString(browserd ?? {}, "engine_mode") ?? "n/a"}</p>
        </article>
      </section>

      <section className="console-grid-2">
        <article className="console-subpanel">
          <div className="console-subpanel__header">
            <div>
              <h3>Published CLI handoffs</h3>
              <p className="chat-muted">
                Low-level diagnostics and protocol utilities stay discoverable here even when execution remains outside the browser session.
              </p>
            </div>
          </div>
          <CapabilityCardList
            entries={groupedCapabilities.cli_handoff}
            emptyMessage="No CLI handoffs are currently published for operations."
          />
        </article>
        <article className="console-subpanel">
          <div className="console-subpanel__header">
            <div>
              <h3>Internal-only capability notes</h3>
              <p className="chat-muted">
                Internal-only capabilities remain visible so hidden operational power does not become accidental product surface.
              </p>
            </div>
          </div>
          <CapabilityCardList
            entries={groupedCapabilities.internal_only}
            emptyMessage="No internal-only capability notes are currently published for operations."
          />
        </article>
      </section>

      <section className="console-subpanel">
        <div className="console-subpanel__header">
          <div>
            <h3>Audit event browser</h3>
            <p className="chat-muted">
              Filter the journal view by principal or free-text containment. Results stay redacted unless the operator explicitly opts into revealing sensitive values.
            </p>
          </div>
        </div>
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
        {app.auditEvents.length === 0 ? (
          <p>No audit events loaded.</p>
        ) : (
          <pre>{toPrettyJson(app.auditEvents, app.revealSensitiveValues)}</pre>
        )}
      </section>

      <section className="console-subpanel">
        <div className="console-subpanel__header">
          <div>
            <h3>Redacted diagnostics snapshot</h3>
            <p className="chat-muted">
              Diagnostics stay redacted by default and mirror the backend troubleshooting snapshot rather than inventing parallel client-side heuristics.
            </p>
          </div>
        </div>
        {diagnostics === null ? (
          <p>No diagnostics loaded.</p>
        ) : (
          <pre>{toPrettyJson(diagnostics, app.revealSensitiveValues)}</pre>
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
