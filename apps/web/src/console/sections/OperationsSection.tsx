import type { CapabilityCatalog } from "../../consoleApi";
import { capabilitiesByMode, capabilitiesForSection } from "../capabilityCatalog";
import { CapabilityCardList } from "../components/CapabilityCards";
import { ActionButton, TextInputField } from "../components/ui";
import {
  WorkspaceMetricCard,
  WorkspacePageHeader,
  WorkspaceSectionCard,
  WorkspaceStatusChip,
} from "../components/workspace/WorkspaceChrome";
import {
  WorkspaceEmptyState,
  WorkspaceInlineNotice,
  WorkspaceTable,
  workspaceToneForState,
} from "../components/workspace/WorkspacePatterns";
import {
  PrettyJsonBlock,
  formatUnixMs,
  readNumber,
  readObject,
  readString,
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
    | "overviewUsageInsights"
    | "overviewCatalog"
    | "memoryStatus"
    | "refreshMemoryStatus"
    | "revealSensitiveValues"
  >;
};

export function OperationsSection({ app }: OperationsSectionProps) {
  const catalog = readCapabilityCatalog(app.overviewCatalog);
  const groupedCapabilities = capabilitiesByMode(capabilitiesForSection(catalog, "operations"));
  const diagnostics = app.diagnosticsSnapshot;
  const usageInsights = app.overviewUsageInsights;
  const learning = readObject(app.memoryStatus ?? {}, "learning");
  const learningCounters = readObject(learning ?? {}, "counters");
  const learningEnabled = typeof learning?.enabled === "boolean" ? learning.enabled : false;
  const observability = readObject(diagnostics ?? {}, "observability");
  const modelProvider = readObject(diagnostics ?? {}, "model_provider");
  const authProfiles = readObject(diagnostics ?? {}, "auth_profiles");
  const browserd = readObject(diagnostics ?? {}, "browserd");
  const recentFailures = readJsonObjectArray(observability?.recent_failures);
  const connector = readObject(observability ?? {}, "connector");
  const browser = readObject(observability ?? {}, "browser");
  const doctorRecovery = readObject(observability ?? {}, "doctor_recovery");
  const selfHealing = readObject(observability ?? {}, "self_healing");
  const selfHealingSummary = readObject(selfHealing ?? {}, "summary");
  const selfHealingSettings = readObject(selfHealing ?? {}, "settings");
  const latestDoctorRecovery = readObject(doctorRecovery ?? {}, "last_job");
  const activeIncidents = readJsonObjectArray(selfHealing?.active_incidents);
  const recentRemediationAttempts = readJsonObjectArray(selfHealing?.recent_remediation_attempts);
  const selfHealingHeartbeats = readJsonObjectArray(selfHealing?.heartbeats);
  const browserFailureSamples = toStringArray(
    Array.isArray(browser?.recent_failure_samples) ? browser.recent_failure_samples : [],
  );
  const usageAlertCount = Array.isArray(usageInsights?.alerts) ? usageInsights.alerts.length : 0;
  const usageBudgetEvaluations = Array.isArray(usageInsights?.budgets?.evaluations)
    ? usageInsights.budgets.evaluations.length
    : 0;
  const usageDefaultMode = usageInsights?.routing?.default_mode ?? "No routing posture loaded.";
  const usageRoutingOverrides = usageInsights?.routing?.overrides ?? 0;
  const usageProviderHealth = usageInsights?.health?.provider_state ?? "unknown";
  const usageProviderErrorRateBps = usageInsights?.health?.error_rate_bps ?? 0;

  return (
    <main className="workspace-page">
      <WorkspacePageHeader
        eyebrow="Settings"
        title="Diagnostics"
        description="Technical detail now lives here instead of dominating Overview: runtime status, audit events, CLI handoffs, and bounded troubleshooting context."
        status={
          <>
            <WorkspaceStatusChip
              tone={workspaceToneForState(readString(modelProvider ?? {}, "state") ?? "unknown")}
            >
              Provider: {readString(modelProvider ?? {}, "state") ?? "unknown"}
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={recentFailures.length > 0 ? "warning" : "default"}>
              {recentFailures.length} recent failures
            </WorkspaceStatusChip>
          </>
        }
        actions={
          <div className="workspace-inline">
            <ActionButton
              type="button"
              variant="primary"
              onPress={() => void app.refreshDiagnostics()}
              isDisabled={app.diagnosticsBusy}
            >
              {app.diagnosticsBusy ? "Refreshing..." : "Refresh diagnostics"}
            </ActionButton>
            <ActionButton
              type="button"
              variant="secondary"
              onPress={() => void app.refreshAudit()}
              isDisabled={app.auditBusy}
            >
              {app.auditBusy ? "Refreshing..." : "Refresh audit"}
            </ActionButton>
            <ActionButton
              type="button"
              variant="ghost"
              onPress={() => void app.refreshMemoryStatus()}
              isDisabled={app.diagnosticsBusy}
            >
              Refresh learning
            </ActionButton>
          </div>
        }
      />

      <section className="workspace-metric-grid">
        <WorkspaceMetricCard
          label="Model provider"
          value={readString(modelProvider ?? {}, "provider") ?? "n/a"}
          detail={readString(modelProvider ?? {}, "state") ?? "No provider state loaded."}
          tone={workspaceToneForState(readString(modelProvider ?? {}, "state") ?? "unknown")}
        />
        <WorkspaceMetricCard
          label="Auth state"
          value={readString(authProfiles ?? {}, "state") ?? "n/a"}
          detail={`${Array.isArray(authProfiles?.profiles) ? authProfiles.profiles.length : 0} profiles published`}
          tone={workspaceToneForState(readString(authProfiles ?? {}, "state") ?? "unknown")}
        />
        <WorkspaceMetricCard
          label="Browser service"
          value={readString(browserd ?? {}, "state") ?? "n/a"}
          detail={readString(browserd ?? {}, "engine_mode") ?? "No engine mode published."}
          tone={workspaceToneForState(readString(browserd ?? {}, "state") ?? "unknown")}
        />
        <WorkspaceMetricCard
          label="Connector dead letters"
          value={readString(connector ?? {}, "dead_letters") ?? "0"}
          detail={`${browserFailureSamples.length} browser relay failure samples published.`}
          tone={recentFailures.length > 0 ? "warning" : "default"}
        />
        <WorkspaceMetricCard
          label="Usage alerts"
          value={usageAlertCount}
          detail={usageDefaultMode}
          tone={usageAlertCount > 0 ? "warning" : "default"}
        />
        <WorkspaceMetricCard
          label="Self-healing incidents"
          value={readNumber(selfHealingSummary ?? {}, "active") ?? 0}
          detail={`${readNumber(selfHealingSummary ?? {}, "resolving") ?? 0} remediating · ${readNumber(selfHealingSummary ?? {}, "resolved") ?? 0} resolved`}
          tone={(readNumber(selfHealingSummary ?? {}, "active") ?? 0) > 0 ? "warning" : "default"}
        />
        <WorkspaceMetricCard
          label="Recovery jobs"
          value={readNumber(doctorRecovery ?? {}, "failed") ?? 0}
          detail={
            latestDoctorRecovery === null
              ? "No recovery summary published."
              : (readString(latestDoctorRecovery, "mode") ?? "Latest mode unavailable")
          }
          tone={workspaceToneForState(readString(latestDoctorRecovery ?? {}, "state") ?? "unknown")}
        />
        <WorkspaceMetricCard
          label="Learning reflections"
          value={readNumber(learningCounters ?? {}, "reflections_scheduled") ?? 0}
          detail={`${readNumber(learningCounters ?? {}, "candidates_created") ?? 0} candidates · ${readNumber(learningCounters ?? {}, "candidates_auto_applied") ?? 0} auto-applied`}
          tone={
            (readNumber(learningCounters ?? {}, "reflections_scheduled") ?? 0) > 0
              ? "accent"
              : "default"
          }
        />
      </section>

      {recentFailures.length > 0 ? (
        <WorkspaceInlineNotice title="Recent failures" tone="warning">
          <ul className="console-compact-list">
            {recentFailures.slice(0, 4).map((failure, index) => (
              <li key={`${readString(failure, "operation") ?? "failure"}-${index}`}>
                <strong>{readString(failure, "failure_class") ?? "unknown"}</strong>:{" "}
                {readString(failure, "message") ?? readString(failure, "operation") ?? "No detail"}
              </li>
            ))}
          </ul>
        </WorkspaceInlineNotice>
      ) : null}

      <section className="workspace-aside-grid">
        <div className="workspace-stack">
          <WorkspaceSectionCard
            title="Audit events"
            description="Use quick filters near the table header and keep the actions column-free so the event stream stays readable."
          >
            <div className="workspace-form-grid">
              <TextInputField
                label="Principal filter"
                value={app.auditFilterPrincipal}
                onChange={app.setAuditFilterPrincipal}
              />
              <TextInputField
                label="Payload contains"
                value={app.auditFilterContains}
                onChange={app.setAuditFilterContains}
              />
            </div>
            {app.auditEvents.length === 0 ? (
              <WorkspaceEmptyState
                title="No audit events loaded"
                description="Refresh audit to load the current redacted event stream."
                compact
              />
            ) : (
              <WorkspaceTable
                ariaLabel="Audit events"
                columns={["When", "Event", "Principal", "Summary"]}
              >
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
          <WorkspaceSectionCard
            title="Routing and budget telemetry"
            description="Phase 7 keeps routing recommendations, enforced overrides, and alerting visible from the diagnostics surface."
          >
            {usageInsights === null ? (
              <WorkspaceEmptyState
                compact
                title="No governance snapshot loaded"
                description="Refresh overview to load routing decisions, budget evaluations, and active alerts."
              />
            ) : (
              <WorkspaceTable ariaLabel="Routing telemetry" columns={["Metric", "Value", "Detail"]}>
                <tr>
                  <td>Default routing mode</td>
                  <td>{usageDefaultMode}</td>
                  <td>{usageRoutingOverrides} recent overrides</td>
                </tr>
                <tr>
                  <td>Provider health</td>
                  <td>{usageProviderHealth}</td>
                  <td>{usageProviderErrorRateBps} bps error rate</td>
                </tr>
                <tr>
                  <td>Budget evaluations</td>
                  <td>{usageBudgetEvaluations}</td>
                  <td>{usageAlertCount} active alerts</td>
                </tr>
              </WorkspaceTable>
            )}
          </WorkspaceSectionCard>

          <WorkspaceSectionCard
            title="Learning workload"
            description="Reflection stays visible as a separate background workload so operators can distinguish Phase 6 learning activity from user-facing runs."
          >
            {learning === null ? (
              <WorkspaceEmptyState
                compact
                title="No learning policy loaded"
                description="Refresh learning to load reflection policy and candidate counters."
              />
            ) : (
              <WorkspaceTable ariaLabel="Learning workload" columns={["Metric", "Value", "Detail"]}>
                <tr>
                  <td>Enabled</td>
                  <td>{learningEnabled ? "true" : "false"}</td>
                  <td>
                    {readNumber(learning, "sampling_percent") ?? 0}% sampled background reflections
                  </td>
                </tr>
                <tr>
                  <td>Cooldown</td>
                  <td>{readNumber(learning, "cooldown_ms") ?? 0} ms</td>
                  <td>{readNumber(learning, "budget_tokens") ?? 0} tokens per reflection task</td>
                </tr>
                <tr>
                  <td>Thresholds</td>
                  <td>
                    {readNumber(learning, "durable_fact_review_min_confidence_bps") ?? 0} bps facts
                    review
                  </td>
                  <td>
                    {readNumber(learning, "durable_fact_auto_write_threshold_bps") ?? 0} bps
                    auto-write · {readNumber(learning, "preference_review_min_confidence_bps") ?? 0}{" "}
                    bps preferences
                  </td>
                </tr>
                <tr>
                  <td>Throughput</td>
                  <td>
                    {readNumber(learningCounters ?? {}, "reflections_completed") ?? 0} completed
                  </td>
                  <td>
                    {readNumber(learningCounters ?? {}, "candidates_created") ?? 0} candidates
                    generated
                  </td>
                </tr>
                <tr>
                  <td>Procedure policy</td>
                  <td>{readNumber(learning, "procedure_min_occurrences") ?? 0} matching runs</td>
                  <td>
                    {readNumber(learning, "procedure_review_min_confidence_bps") ?? 0} bps review ·{" "}
                    {readNumber(learning, "max_candidates_per_run") ?? 0} max candidates/run
                  </td>
                </tr>
              </WorkspaceTable>
            )}
          </WorkspaceSectionCard>

          <WorkspaceSectionCard
            title="CLI handoffs"
            description="Deeper troubleshooting remains explicit instead of hiding behind undocumented operator steps."
          >
            <CapabilityCardList
              entries={groupedCapabilities.cli_handoff}
              emptyMessage="No CLI handoffs are currently published for diagnostics."
            />
          </WorkspaceSectionCard>

          <WorkspaceSectionCard
            title="Interop surfaces"
            description="ACP remains the stateful editor bridge, MCP stays the narrower stdio facade, and compat responses now carry `_palyra` metadata so run/session IDs stay explainable across transports."
          >
            <WorkspaceTable
              ariaLabel="Interop surfaces"
              columns={["Surface", "Primary use", "Operator note"]}
            >
              <tr>
                <td>ACP bridge</td>
                <td>IDE-style session control</td>
                <td>
                  Stable session binding and reconnect behavior share the native approval model.
                </td>
              </tr>
              <tr>
                <td>MCP facade</td>
                <td>Tool-oriented stdio integrations</td>
                <td>
                  Read-only by default, with explicit mutation tools gated by the same approvals.
                </td>
              </tr>
              <tr>
                <td>Compat API</td>
                <td>OpenAI-compatible clients</td>
                <td>
                  `_palyra.run_id` and `_palyra.session_id` help correlate interop traffic with
                  audit and transcript records.
                </td>
              </tr>
            </WorkspaceTable>
          </WorkspaceSectionCard>

          <WorkspaceSectionCard
            title="Internal notes"
            description="Keep internal-only capabilities visible so hidden power does not become accidental product surface."
          >
            <CapabilityCardList
              entries={groupedCapabilities.internal_only}
              emptyMessage="No internal-only capability notes are currently published for diagnostics."
            />
          </WorkspaceSectionCard>

          <WorkspaceSectionCard
            title="Self-healing"
            description="Phase 2 incident telemetry, watchdog output, and recent remediation attempts are summarized here before you fall back to the raw diagnostics snapshot."
          >
            {selfHealing === null ? (
              <WorkspaceEmptyState
                compact
                title="No self-healing snapshot loaded"
                description="Refresh diagnostics to load incident telemetry and remediation history."
              />
            ) : (
              <div className="workspace-stack">
                <WorkspaceTable
                  ariaLabel="Self-healing summary"
                  columns={["Metric", "Value", "Detail"]}
                >
                  <tr>
                    <td>Mode</td>
                    <td>{readString(selfHealingSettings ?? {}, "mode") ?? "n/a"}</td>
                    <td>{selfHealingHeartbeats.length} tracked heartbeats</td>
                  </tr>
                  <tr>
                    <td>Active incidents</td>
                    <td>{readNumber(selfHealingSummary ?? {}, "active") ?? 0}</td>
                    <td>{activeIncidents.length} incident records in the current snapshot</td>
                  </tr>
                  <tr>
                    <td>Recent remediations</td>
                    <td>{recentRemediationAttempts.length}</td>
                    <td>Most recent attempts remain redacted but operator-readable.</td>
                  </tr>
                </WorkspaceTable>

                {activeIncidents.length === 0 ? (
                  <WorkspaceEmptyState
                    compact
                    title="No active incidents"
                    description="The watchdog currently reports no open self-healing incidents."
                  />
                ) : (
                  <WorkspaceTable
                    ariaLabel="Active self-healing incidents"
                    columns={["Domain", "Severity", "State", "Summary", "Updated"]}
                  >
                    {activeIncidents.slice(0, 8).map((incident, index) => (
                      <tr key={`${readString(incident, "incident_id") ?? "incident"}-${index}`}>
                        <td>{readString(incident, "domain") ?? "unknown"}</td>
                        <td>{readString(incident, "severity") ?? "unknown"}</td>
                        <td>{readString(incident, "state") ?? "unknown"}</td>
                        <td>{readString(incident, "summary") ?? "No summary"}</td>
                        <td>{formatUnixMs(readNumber(incident, "updated_at_unix_ms")) ?? "n/a"}</td>
                      </tr>
                    ))}
                  </WorkspaceTable>
                )}

                {recentRemediationAttempts.length > 0 ? (
                  <WorkspaceTable
                    ariaLabel="Recent remediation attempts"
                    columns={["When", "Feature", "Status", "Incident", "Detail"]}
                  >
                    {recentRemediationAttempts.slice(0, 8).map((attempt, index) => (
                      <tr key={`${readString(attempt, "attempt_id") ?? "attempt"}-${index}`}>
                        <td>{formatUnixMs(readNumber(attempt, "recorded_at_unix_ms")) ?? "n/a"}</td>
                        <td>{readString(attempt, "feature") ?? "unknown"}</td>
                        <td>{readString(attempt, "status") ?? "unknown"}</td>
                        <td>{readString(attempt, "incident_id") ?? "n/a"}</td>
                        <td>{readString(attempt, "detail") ?? "No detail"}</td>
                      </tr>
                    ))}
                  </WorkspaceTable>
                ) : null}
              </div>
            )}
          </WorkspaceSectionCard>

          <WorkspaceSectionCard
            title="Recovery telemetry"
            description="The last queued doctor recovery job is summarized here so operations can verify preview/apply/rollback state without leaving diagnostics."
          >
            {latestDoctorRecovery === null ? (
              <WorkspaceEmptyState
                compact
                title="No recovery telemetry loaded"
                description="Queue a doctor preview from Support to publish recovery telemetry."
              />
            ) : (
              <PrettyJsonBlock
                value={latestDoctorRecovery}
                revealSensitiveValues={app.revealSensitiveValues}
              />
            )}
          </WorkspaceSectionCard>

          <WorkspaceSectionCard
            title="Diagnostics snapshot"
            description="Raw snapshot stays available as a secondary surface after the summary and tables."
          >
            {diagnostics === null ? (
              <WorkspaceEmptyState
                title="No diagnostics loaded"
                description="Refresh diagnostics to load the latest redacted snapshot."
                compact
              />
            ) : (
              <PrettyJsonBlock
                value={diagnostics}
                revealSensitiveValues={app.revealSensitiveValues}
              />
            )}
          </WorkspaceSectionCard>
        </div>
      </section>
    </main>
  );
}

function readCapabilityCatalog(value: JsonObject | null): CapabilityCatalog | null {
  return value !== null && Array.isArray(value.capabilities)
    ? (value as unknown as CapabilityCatalog)
    : null;
}

function readJsonObjectArray(value: unknown): JsonObject[] {
  return Array.isArray(value)
    ? value.filter(
        (entry): entry is JsonObject =>
          entry !== null && typeof entry === "object" && !Array.isArray(entry),
      )
    : [];
}

function formatAuditTime(event: JsonObject): string {
  return (
    formatUnixMs(
      readNumber(event, "timestamp_unix_ms") ??
        readNumber(event, "observed_at_unix_ms") ??
        readNumber(event, "created_at_unix_ms"),
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
    readString(event, "message") ?? readString(event, "summary") ?? readString(event, "reason");
  if (summary !== null) {
    return summary;
  }

  if (event.payload !== undefined && event.payload !== null) {
    if (
      typeof event.payload === "string" ||
      typeof event.payload === "number" ||
      typeof event.payload === "boolean"
    ) {
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
