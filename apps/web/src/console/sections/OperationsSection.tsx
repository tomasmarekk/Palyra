import { useNavigate } from "react-router-dom";

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
} from "../shared";
import { FlowOperationsPanel } from "./FlowOperationsPanel";
import {
  formatAuditEventName,
  formatAuditSummary,
  formatAuditTime,
  readCapabilityCatalog,
  readJsonObjectArray,
  shortDiagnosticId,
} from "./operationDiagnostics";
import type { ConsoleAppState } from "../useConsoleAppState";

type OperationsSectionProps = {
  app: Pick<
    ConsoleAppState,
    | "api"
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
    | "setError"
    | "setNotice"
  >;
};

export function OperationsSection({ app }: OperationsSectionProps) {
  const navigate = useNavigate();
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
  const runtimePreview = readObject(observability ?? {}, "runtime_preview");
  const delegation = readObject(diagnostics ?? {}, "delegation");
  const delegationParents = readJsonObjectArray(delegation?.parents);
  const delegationChildren = readJsonObjectArray(delegation?.recent_children);
  const previewMetrics = readObject(runtimePreview ?? {}, "metrics") ?? {};
  const previewGuardrails = readObject(runtimePreview ?? {}, "guardrails");
  const runtimePreviewCatalog = readJsonObjectArray(runtimePreview?.catalog);
  const runtimePreviewEvents = readJsonObjectArray(runtimePreview?.recent_events);
  const runtimeGuardrailRecommendations = toStringArray(
    Array.isArray(previewGuardrails?.recommendations) ? previewGuardrails.recommendations : [],
  );
  const runtimeGuardrailChecklist = toStringArray(
    Array.isArray(previewGuardrails?.rollout_checklist) ? previewGuardrails.rollout_checklist : [],
  );
  const runtimeFailureModes = toStringArray(
    Array.isArray(previewGuardrails?.failure_modes) ? previewGuardrails.failure_modes : [],
  );
  const configRefHealth = readObject(observability ?? {}, "config_ref_health");
  const configRefSummary = readObject(configRefHealth ?? {}, "summary");
  const configRefItems = readJsonObjectArray(configRefHealth?.items);
  const configRefRecommendations = toStringArray(
    Array.isArray(configRefHealth?.recommendations) ? configRefHealth.recommendations : [],
  );
  const selfHealing = readObject(observability ?? {}, "self_healing");
  const selfHealingSummary = readObject(selfHealing ?? {}, "summary");
  const selfHealingSettings = readObject(selfHealing ?? {}, "settings");
  const operatorInsights = readObject(observability ?? {}, "operator_insights");
  const operatorSummary = readObject(operatorInsights ?? {}, "summary");
  const operatorHotspots = readJsonObjectArray(operatorInsights?.hotspots);
  const operatorProvider = readObject(operatorInsights ?? {}, "provider_health");
  const operatorRecall = readObject(operatorInsights ?? {}, "recall");
  const operatorPlugins = readObject(operatorInsights ?? {}, "plugins");
  const operatorCron = readObject(operatorInsights ?? {}, "cron");
  const operatorReload = readObject(operatorInsights ?? {}, "reload");
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
          label="Operator hotspots"
          value={readNumber(operatorSummary ?? {}, "hotspot_count") ?? 0}
          detail={
            readString(operatorSummary ?? {}, "recommendation") ??
            "Refresh diagnostics to load operator hotspots."
          }
          tone={workspaceToneForState(
            readString(operatorSummary ?? {}, "severity") ??
              readString(operatorSummary ?? {}, "state") ??
              "unknown",
          )}
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
          label="Config ref health"
          value={readNumber(configRefSummary ?? {}, "blocking_refs") ?? 0}
          detail={`${readNumber(configRefSummary ?? {}, "warning_refs") ?? 0} warnings · ${readString(configRefHealth ?? {}, "state") ?? "No ref health loaded."}`}
          tone={workspaceToneForState(readString(configRefHealth ?? {}, "state") ?? "unknown")}
        />
        <WorkspaceMetricCard
          label="Runtime preview"
          value={readString(runtimePreview ?? {}, "state") ?? "n/a"}
          detail={`${readNumber(previewMetrics, "queue_average_depth") ?? 0} avg queue depth · ${readNumber(previewMetrics, "pruning_tokens_saved") ?? 0} pruning tokens saved`}
          tone={workspaceToneForState(readString(runtimePreview ?? {}, "state") ?? "unknown")}
        />
        <WorkspaceMetricCard
          label="Runtime guardrails"
          value={readString(previewGuardrails ?? {}, "state") ?? "n/a"}
          detail={`${readNumber(previewMetrics, "queue_steering_deferrals") ?? 0} steering deferrals · ${readNumber(previewMetrics, "queue_delivery_failures") ?? 0} delivery failures`}
          tone={workspaceToneForState(readString(previewGuardrails ?? {}, "state") ?? "unknown")}
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
            title="Config ref health"
            description="Configured secret sources, stale runtime snapshots, and reload blockers stay visible alongside the broader diagnostics stream."
          >
            {configRefItems.length === 0 ? (
              <WorkspaceEmptyState
                compact
                title="No config ref health published"
                description="Refresh diagnostics after the daemon publishes configured ref health."
              />
            ) : (
              <div className="workspace-stack">
                <WorkspaceTable
                  ariaLabel="Config ref health"
                  columns={["Config path", "State", "Reload", "Advice"]}
                >
                  {configRefItems.slice(0, 5).map((item, index) => (
                    <tr
                      key={`${readString(item, "ref_id") ?? readString(item, "config_path") ?? "ref"}-${index}`}
                    >
                      <td>{readString(item, "config_path") ?? "n/a"}</td>
                      <td>
                        <WorkspaceStatusChip
                          tone={workspaceToneForState(readString(item, "severity") ?? "unknown")}
                        >
                          {readString(item, "state") ?? "unknown"}
                        </WorkspaceStatusChip>
                      </td>
                      <td>{readString(item, "reload_mode") ?? "n/a"}</td>
                      <td>{readString(item, "advice") ?? "No operator advice published."}</td>
                    </tr>
                  ))}
                </WorkspaceTable>
                {configRefRecommendations.length > 0 ? (
                  <WorkspaceInlineNotice title="Recommended next steps" tone="warning">
                    <ul className="console-compact-list">
                      {configRefRecommendations.slice(0, 4).map((recommendation) => (
                        <li key={recommendation}>{recommendation}</li>
                      ))}
                    </ul>
                  </WorkspaceInlineNotice>
                ) : null}
              </div>
            )}
          </WorkspaceSectionCard>

          <WorkspaceSectionCard
            title="Runtime preview"
            description="Queue, pruning, retrieval, flow, delivery, and worker telemetry stays visible here while rollout-controlled runtime surfaces remain in preview."
          >
            {runtimePreviewCatalog.length === 0 ? (
              <WorkspaceEmptyState
                compact
                title="No runtime preview telemetry"
                description="Refresh diagnostics after the daemon records runtime preview events."
              />
            ) : (
              <div className="workspace-stack">
                <WorkspaceTable
                  ariaLabel="Runtime preview catalog"
                  columns={["Event", "Emitted", "Last seen", "Summary"]}
                >
                  {runtimePreviewCatalog.slice(0, 8).map((entry, index) => (
                    <tr
                      key={`${readString(entry, "event_type") ?? readString(entry, "journal_event") ?? "runtime"}-${index}`}
                    >
                      <td>{readString(entry, "event_type") ?? "unknown"}</td>
                      <td>{readNumber(entry, "emitted") ?? 0}</td>
                      <td>{formatUnixMs(readNumber(entry, "last_seen_at_unix_ms")) ?? "n/a"}</td>
                      <td>{readString(entry, "summary") ?? "No summary published."}</td>
                    </tr>
                  ))}
                </WorkspaceTable>
                <WorkspaceTable
                  ariaLabel="Runtime preview metrics"
                  columns={["Metric", "Value", "Detail"]}
                >
                  <tr>
                    <td>Queue depth</td>
                    <td>
                      {readNumber(previewMetrics, "queue_depth") ?? 0} /{" "}
                      {readNumber(previewMetrics, "queue_peak_depth") ?? 0}
                    </td>
                    <td>
                      Current / peak observed depth. Average{" "}
                      {readNumber(previewMetrics, "queue_average_depth") ?? 0} across{" "}
                      {readNumber(previewMetrics, "queue_depth_samples") ?? 0} samples.
                    </td>
                  </tr>
                  <tr>
                    <td>Queue coalescing</td>
                    <td>{readNumber(previewMetrics, "queue_coalescing_rate_bps") ?? 0} bps</td>
                    <td>
                      {readNumber(previewMetrics, "queue_merge_events") ?? 0} merge events from{" "}
                      {readNumber(previewMetrics, "queue_decision_events") ?? 0} queue decisions.
                    </td>
                  </tr>
                  <tr>
                    <td>Overflow summaries</td>
                    <td>
                      {readNumber(previewMetrics, "queue_overflow_summary_rate_bps") ?? 0} bps
                    </td>
                    <td>
                      {readNumber(previewMetrics, "queue_overflow_events") ?? 0} overflow summary
                      events; {readNumber(previewMetrics, "queue_steering_deferrals") ?? 0} steering
                      deferrals.
                    </td>
                  </tr>
                  <tr>
                    <td>Pruning savings</td>
                    <td>{readNumber(previewMetrics, "pruning_tokens_saved") ?? 0}</td>
                    <td>
                      Avg {readNumber(previewMetrics, "pruning_average_savings_tokens") ?? 0} tokens
                      across {readNumber(previewMetrics, "pruning_apply_events") ?? 0} pruning
                      decisions.
                    </td>
                  </tr>
                  <tr>
                    <td>Compaction avoidance</td>
                    <td>{readNumber(previewMetrics, "compaction_avoidance_rate_bps") ?? 0} bps</td>
                    <td>
                      {readNumber(previewMetrics, "compaction_avoidance_events") ?? 0} pruning
                      decisions saved enough context to avoid heavier compaction.
                    </td>
                  </tr>
                  <tr>
                    <td>Recall latency</td>
                    <td>{readNumber(previewMetrics, "retrieval_branch_latency_avg_ms") ?? 0} ms</td>
                    <td>
                      Max {readNumber(previewMetrics, "retrieval_branch_latency_max_ms") ?? 0} ms
                      across recall preview branches.
                    </td>
                  </tr>
                  <tr>
                    <td>Auxiliary spend</td>
                    <td>{readNumber(previewMetrics, "auxiliary_budget_tokens") ?? 0}</td>
                    <td>
                      {readNumber(previewMetrics, "auxiliary_task_events") ?? 0} task lifecycle
                      events captured.
                    </td>
                  </tr>
                  <tr>
                    <td>Delivery arbitration</td>
                    <td>{readNumber(previewMetrics, "arbitration_suppressions") ?? 0}</td>
                    <td>
                      Stale parent suppressions recorded while descendant-aware delivery decisions
                      keep full audit payloads in the runtime event stream.
                    </td>
                  </tr>
                  <tr>
                    <td>Worker orphan rate</td>
                    <td>{readNumber(previewMetrics, "worker_orphan_rate_bps") ?? 0}</td>
                    <td>
                      {readNumber(previewMetrics, "worker_orphaned_events") ?? 0} of{" "}
                      {readNumber(previewMetrics, "worker_events") ?? 0} worker lifecycle events.
                    </td>
                  </tr>
                </WorkspaceTable>
                {runtimePreviewEvents.length > 0 ? (
                  <WorkspaceTable
                    ariaLabel="Runtime preview recent events"
                    columns={["When", "Event", "Reason", "Principal"]}
                  >
                    {runtimePreviewEvents.slice(0, 5).map((event, index) => (
                      <tr
                        key={`${readString(event, "event_type") ?? readString(event, "reason") ?? "event"}-${index}`}
                      >
                        <td>{formatUnixMs(readNumber(event, "observed_at_unix_ms")) ?? "n/a"}</td>
                        <td>{readString(event, "event_type") ?? "unknown"}</td>
                        <td>{readString(event, "reason") ?? "No reason published."}</td>
                        <td>{readString(event, "principal") ?? "n/a"}</td>
                      </tr>
                    ))}
                  </WorkspaceTable>
                ) : null}
                {runtimeGuardrailRecommendations.length > 0 ? (
                  <WorkspaceInlineNotice
                    title="Queue and pruning guardrails"
                    tone={workspaceToneForState(
                      readString(previewGuardrails ?? {}, "state") ?? "unknown",
                    )}
                  >
                    <ul className="console-compact-list">
                      {runtimeGuardrailRecommendations.slice(0, 4).map((recommendation) => (
                        <li key={recommendation}>{recommendation}</li>
                      ))}
                    </ul>
                  </WorkspaceInlineNotice>
                ) : null}
                {runtimeGuardrailChecklist.length > 0 || runtimeFailureModes.length > 0 ? (
                  <WorkspaceTable
                    ariaLabel="Queue and pruning rollout checklist"
                    columns={["Area", "Operator signal"]}
                  >
                    {runtimeGuardrailChecklist.slice(0, 3).map((item) => (
                      <tr key={`checklist-${item}`}>
                        <td>Checklist</td>
                        <td>{item}</td>
                      </tr>
                    ))}
                    {runtimeFailureModes.slice(0, 3).map((item) => (
                      <tr key={`failure-${item}`}>
                        <td>Failure mode</td>
                        <td>{item}</td>
                      </tr>
                    ))}
                  </WorkspaceTable>
                ) : null}
              </div>
            )}
          </WorkspaceSectionCard>

          <WorkspaceSectionCard
            title="Operator hotspots"
            description="This is the aggregated operator surface: severity, next action, and the quickest jump into the right section stay together instead of being buried in logs."
          >
            {operatorHotspots.length === 0 ? (
              <WorkspaceEmptyState
                compact
                title="No operator hotspots published"
                description="Refresh diagnostics after the daemon publishes operator insights."
              />
            ) : (
              <div className="workspace-stack">
                <WorkspaceTable
                  ariaLabel="Operator hotspots"
                  columns={["Subsystem", "Severity", "Summary", "Action", "Open"]}
                >
                  {operatorHotspots.slice(0, 5).map((hotspot, index) => {
                    const drillDown = readObject(hotspot, "drill_down");
                    const consolePath = readString(drillDown ?? {}, "console_path");
                    return (
                      <tr
                        key={`${readString(hotspot, "hotspot_id") ?? readString(hotspot, "subsystem") ?? "hotspot"}-${index}`}
                      >
                        <td>{readString(hotspot, "subsystem") ?? "unknown"}</td>
                        <td>
                          <WorkspaceStatusChip
                            tone={workspaceToneForState(
                              readString(hotspot, "severity") ?? "unknown",
                            )}
                          >
                            {readString(hotspot, "severity") ?? "unknown"}
                          </WorkspaceStatusChip>
                        </td>
                        <td>{readString(hotspot, "summary") ?? "No hotspot summary published."}</td>
                        <td>
                          {readString(hotspot, "recommended_action") ??
                            "No operator action published."}
                        </td>
                        <td>
                          {consolePath === null ? (
                            "n/a"
                          ) : (
                            <ActionButton
                              size="sm"
                              variant="ghost"
                              onPress={() => void navigate(consolePath)}
                            >
                              Open
                            </ActionButton>
                          )}
                        </td>
                      </tr>
                    );
                  })}
                </WorkspaceTable>
                <WorkspaceTable
                  ariaLabel="Operator insight summary"
                  columns={["Metric", "Value", "Detail"]}
                >
                  <tr>
                    <td>Provider</td>
                    <td>{readString(operatorProvider ?? {}, "state") ?? "unknown"}</td>
                    <td>
                      {readString(operatorProvider ?? {}, "summary") ?? "No provider summary"}
                    </td>
                  </tr>
                  <tr>
                    <td>Recall</td>
                    <td>{readString(operatorRecall ?? {}, "state") ?? "unknown"}</td>
                    <td>{readString(operatorRecall ?? {}, "summary") ?? "No recall summary"}</td>
                  </tr>
                  <tr>
                    <td>Plugins</td>
                    <td>{readString(operatorPlugins ?? {}, "state") ?? "unknown"}</td>
                    <td>{readString(operatorPlugins ?? {}, "summary") ?? "No plugin summary"}</td>
                  </tr>
                  <tr>
                    <td>Cron / reload</td>
                    <td>
                      {readString(operatorCron ?? {}, "state") ?? "unknown"} /{" "}
                      {readString(operatorReload ?? {}, "state") ?? "unknown"}
                    </td>
                    <td>
                      {readString(operatorCron ?? {}, "summary") ?? "No cron summary"};{" "}
                      {readString(operatorReload ?? {}, "summary") ?? "No reload summary"}
                    </td>
                  </tr>
                </WorkspaceTable>
              </div>
            )}
          </WorkspaceSectionCard>

          <WorkspaceSectionCard
            title="Routing and budget telemetry"
            description="Routing recommendations, enforced overrides, and alerting stay visible from the diagnostics surface."
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

          <FlowOperationsPanel
            api={app.api}
            diagnostics={diagnostics}
            setError={app.setError}
            setNotice={app.setNotice}
          />

          <WorkspaceSectionCard
            title="Delegated children"
            description="Parent/child topology, scheduler backpressure, and child runtime limits stay visible without opening each run tape."
          >
            {delegation === null ? (
              <WorkspaceEmptyState
                compact
                title="No delegation snapshot loaded"
                description="Refresh diagnostics to load delegated child topology."
              />
            ) : (
              <div className="workspace-stack">
                <WorkspaceTable
                  ariaLabel="Delegation summary"
                  columns={["Metric", "Value", "Detail"]}
                >
                  <tr>
                    <td>Active children</td>
                    <td>{readNumber(delegation, "active_child_count") ?? 0}</td>
                    <td>
                      {readNumber(delegation, "running_children") ?? 0} running ·{" "}
                      {readNumber(delegation, "queued_children") ?? 0} queued ·{" "}
                      {readNumber(delegation, "waiting_children") ?? 0} waiting
                    </td>
                  </tr>
                  <tr>
                    <td>Parents</td>
                    <td>{readNumber(delegation, "parent_count") ?? delegationParents.length}</td>
                    <td>{readNumber(delegation, "failed_children") ?? 0} failed children</td>
                  </tr>
                </WorkspaceTable>
                {delegationParents.length > 0 ? (
                  <WorkspaceTable
                    ariaLabel="Delegation parents"
                    columns={["Parent run", "Children", "Limits"]}
                  >
                    {delegationParents.slice(0, 8).map((parent, index) => (
                      <tr key={`${readString(parent, "parent_run_id") ?? "parent"}-${index}`}>
                        <td>{shortDiagnosticId(readString(parent, "parent_run_id"))}</td>
                        <td>
                          {readNumber(parent, "running_children") ?? 0} running ·{" "}
                          {readNumber(parent, "queued_children") ?? 0} queued ·{" "}
                          {readNumber(parent, "waiting_children") ?? 0} waiting
                        </td>
                        <td>
                          max {readNumber(parent, "max_concurrent_children") ?? 0} concurrent ·{" "}
                          {readNumber(parent, "active_parallel_group_count") ?? 0}/
                          {readNumber(parent, "max_parallel_groups") ?? 0} groups · timeout{" "}
                          {readNumber(parent, "child_timeout_ms") ?? 0} ms
                        </td>
                      </tr>
                    ))}
                  </WorkspaceTable>
                ) : null}
                {delegationChildren.length > 0 ? (
                  <WorkspaceTable
                    ariaLabel="Delegation recent children"
                    columns={["Child", "State", "Diagnostics"]}
                  >
                    {delegationChildren.slice(0, 8).map((child, index) => (
                      <tr key={`${readString(child, "task_id") ?? "child"}-${index}`}>
                        <td>
                          {shortDiagnosticId(readString(child, "child_run_id"))} ·{" "}
                          {readString(child, "display_name") ??
                            readString(child, "profile_id") ??
                            "profile"}
                        </td>
                        <td>{readString(child, "state") ?? "unknown"}</td>
                        <td>
                          {readString(child, "waiting_reason") ??
                            readString(child, "last_error") ??
                            `group ${readString(child, "group_id") ?? "n/a"}`}
                        </td>
                      </tr>
                    ))}
                  </WorkspaceTable>
                ) : null}
              </div>
            )}
          </WorkspaceSectionCard>
          <WorkspaceSectionCard
            title="Learning workload"
            description="Reflection stays visible as a separate background workload so operators can distinguish learning activity from user-facing runs."
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
            description="Incident telemetry, watchdog output, and recent remediation attempts are summarized here before you fall back to the raw diagnostics snapshot."
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
