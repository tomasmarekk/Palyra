import type { CapabilityCatalog } from "../../consoleApi";
import { capabilitiesByMode, capabilitiesForSection } from "../capabilityCatalog";
import { CapabilityCardList } from "../components/CapabilityCards";
import { ConsoleSectionHeader } from "../components/ConsoleSectionHeader";
import {
  readNumber,
  readObject,
  readString,
  toPrettyJson,
  toStringArray,
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
  const observability = readObject(diagnostics ?? {}, "observability");
  const providerAuth = readObject(observability ?? {}, "provider_auth");
  const dashboard = readObject(observability ?? {}, "dashboard");
  const connector = readObject(observability ?? {}, "connector");
  const browser = readObject(observability ?? {}, "browser");
  const browserRelay = readObject(browser ?? {}, "relay_actions");
  const failureClasses = readObject(observability ?? {}, "failure_classes");
  const recentFailures = Array.isArray(observability?.recent_failures)
    ? observability.recent_failures.filter((entry): entry is JsonObject => entry !== null && typeof entry === "object" && !Array.isArray(entry))
    : [];
  const connectorErrors = Array.isArray(connector?.recent_errors)
    ? connector.recent_errors.filter((entry): entry is JsonObject => entry !== null && typeof entry === "object" && !Array.isArray(entry))
    : [];
  const browserFailureSamples = toStringArray(
    Array.isArray(browser?.recent_failure_samples) ? browser.recent_failure_samples : []
  );
  const triage = readObject(observability ?? {}, "triage");
  const triageOrder = toStringArray(Array.isArray(triage?.common_order) ? triage.common_order : []);

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

      <section className="console-grid-4 console-summary-grid">
        <article className="console-subpanel">
          <h3>Provider auth failures</h3>
          <p><strong>State:</strong> {readString(providerAuth ?? {}, "state") ?? "n/a"}</p>
          <p><strong>Failure rate:</strong> {formatRate(readNumber(providerAuth ?? {}, "failure_rate_bps"))}</p>
          <p><strong>Refresh failures:</strong> {readString(providerAuth ?? {}, "refresh_failures") ?? "0"}</p>
        </article>
        <article className="console-subpanel">
          <h3>Connector operations</h3>
          <p><strong>Queue depth:</strong> {readString(connector ?? {}, "queue_depth") ?? "0"}</p>
          <p><strong>Dead letters:</strong> {readString(connector ?? {}, "dead_letters") ?? "0"}</p>
          <p><strong>Upload failure rate:</strong> {formatRate(readNumber(connector ?? {}, "upload_failure_rate_bps"))}</p>
        </article>
        <article className="console-subpanel">
          <h3>Dashboard mutations</h3>
          <p><strong>Attempts:</strong> {readString(dashboard ?? {}, "attempts") ?? "0"}</p>
          <p><strong>Failures:</strong> {readString(dashboard ?? {}, "failures") ?? "0"}</p>
          <p><strong>Error rate:</strong> {formatRate(readNumber(dashboard ?? {}, "failure_rate_bps"))}</p>
        </article>
        <article className="console-subpanel">
          <h3>Browser relay</h3>
          <p><strong>Attempts:</strong> {readString(browserRelay ?? {}, "attempts") ?? "0"}</p>
          <p><strong>Failures:</strong> {readString(browserRelay ?? {}, "failures") ?? "0"}</p>
          <p><strong>Error rate:</strong> {formatRate(readNumber(browserRelay ?? {}, "failure_rate_bps"))}</p>
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

      <section className="console-grid-2">
        <article className="console-subpanel">
          <div className="console-subpanel__header">
            <div>
              <h3>Failure classification summary</h3>
              <p className="chat-muted">
                The backend keeps a redacted distinction between config issues, upstream provider failures, and product failures so operators can start triage in the right layer.
              </p>
            </div>
          </div>
          <p><strong>Config:</strong> {readString(failureClasses ?? {}, "config_failure") ?? "0"}</p>
          <p><strong>Upstream provider:</strong> {readString(failureClasses ?? {}, "upstream_provider_failure") ?? "0"}</p>
          <p><strong>Product:</strong> {readString(failureClasses ?? {}, "product_failure") ?? "0"}</p>
          <p><strong>Recent failures:</strong> {recentFailures.length}</p>
        </article>
        <article className="console-subpanel">
          <div className="console-subpanel__header">
            <div>
              <h3>Starter triage order</h3>
              <p className="chat-muted">
                Dashboard exposes the same redacted triage order used in support artifacts so incident response starts consistently.
              </p>
            </div>
          </div>
          {triageOrder.length === 0 ? (
            <p>No triage order published.</p>
          ) : (
            <ol className="console-compact-list">
              {triageOrder.map((step) => (
                <li key={step}>{step}</li>
              ))}
            </ol>
          )}
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

      <section className="console-grid-2">
        <article className="console-subpanel">
          <div className="console-subpanel__header">
            <div>
              <h3>Recent redacted failures</h3>
              <p className="chat-muted">
                Correlated failures stay redacted, but still expose enough operation and class detail to reduce guesswork during provider auth, dashboard, and connector incidents.
              </p>
            </div>
          </div>
          {recentFailures.length === 0 ? (
            <p>No recent failures recorded.</p>
          ) : (
            <ul className="console-compact-list">
              {recentFailures.map((entry, index) => (
                <li key={`${readString(entry, "operation") ?? "failure"}-${index}`}>
                  <strong>{readString(entry, "failure_class") ?? "unknown"}</strong>:{" "}
                  {readString(entry, "operation") ?? "operation unavailable"}{" "}
                  ({readString(entry, "message") ?? "no detail"})
                </li>
              ))}
            </ul>
          )}
        </article>
        <article className="console-subpanel">
          <div className="console-subpanel__header">
            <div>
              <h3>Connector and browser samples</h3>
              <p className="chat-muted">
                Samples stay bounded and redacted so operators can spot recurring symptoms without pulling raw logs into the browser.
              </p>
            </div>
          </div>
          <p><strong>Connector samples:</strong></p>
          {connectorErrors.length === 0 ? (
            <p>No connector error samples.</p>
          ) : (
            <ul className="console-compact-list">
              {connectorErrors.map((entry, index) => (
                <li key={`${readString(entry, "connector_id") ?? "connector"}-${index}`}>
                  {readString(entry, "connector_id") ?? "connector"}: {readString(entry, "message") ?? "no detail"}
                </li>
              ))}
            </ul>
          )}
          <p><strong>Browser relay samples:</strong></p>
          {browserFailureSamples.length === 0 ? (
            <p>No browser relay samples.</p>
          ) : (
            <ul className="console-compact-list">
              {browserFailureSamples.map((sample) => (
                <li key={sample}>{sample}</li>
              ))}
            </ul>
          )}
        </article>
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

function formatRate(value: number | null): string {
  if (value === null) {
    return "n/a";
  }
  return `${(value / 100).toFixed(2)}%`;
}

function readCapabilityCatalog(value: JsonObject | null): CapabilityCatalog | null {
  if (value === null || !Array.isArray(value.capabilities)) {
    return null;
  }
  return value as unknown as CapabilityCatalog;
}
