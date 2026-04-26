import { useMemo } from "react";
import { useNavigate } from "react-router-dom";

import type {
  UsageAgentRecord,
  UsageModelRecord,
  UsageSessionRecord,
  UsageTimelineBucket,
} from "../../consoleApi";
import { getSectionPath } from "../navigation";
import {
  ActionButton,
  EntityTable,
  SelectField,
  SwitchField,
  workspaceToneForState,
} from "../components/ui";
import {
  WorkspaceMetricCard,
  WorkspacePageHeader,
  WorkspaceSectionCard,
  WorkspaceStatusChip,
} from "../components/workspace/WorkspaceChrome";
import {
  WorkspaceEmptyState,
  WorkspaceInlineNotice,
} from "../components/workspace/WorkspacePatterns";
import { useUsageDomain } from "../hooks/useUsageDomain";
import { parseRoutingExplanation, readProviderRegistrySummary } from "../providerRegistry";
import { formatUnixMs, readNumber, readObject, readString } from "../shared";
import type { ConsoleAppState } from "../useConsoleAppState";

type UsageSectionProps = {
  app: Pick<
    ConsoleAppState,
    "api" | "setError" | "setNotice" | "diagnosticsSnapshot" | "memoryStatus"
  >;
};

export function UsageSection({ app }: UsageSectionProps) {
  const navigate = useNavigate();
  const usage = useUsageDomain(app);
  const detail = usage.selectedSessionDetail;
  const providerRegistry = readProviderRegistrySummary(app.diagnosticsSnapshot);
  const observability = readObject(app.diagnosticsSnapshot ?? {}, "observability");
  const leaseManager = readObject(observability ?? {}, "lease_manager");
  const learning = readObject(app.memoryStatus ?? {}, "learning");
  const learningCounters = readObject(learning ?? {}, "counters");
  const retrieval = readObject(app.memoryStatus ?? {}, "retrieval");
  const retrievalBackend = readObject(retrieval ?? {}, "backend");
  const externalIndex =
    readObject(retrieval ?? {}, "external_index") ??
    readObject(retrievalBackend ?? {}, "external_index");
  const externalIndexSlos = readObject(externalIndex ?? {}, "scale_slos");
  const externalIndexState = readString(externalIndex ?? {}, "state") ?? "unknown";
  const externalIndexGate =
    readString(externalIndexSlos ?? {}, "preview_gate_state") ?? "preview_blocked";
  const timelinePreview = useMemo(
    () => (usage.summary?.timeline ?? []).slice(-8).reverse(),
    [usage.summary?.timeline],
  );
  const operatorInsights = usage.insights?.operator ?? null;
  const recentRoutingDecisions = useMemo(
    () =>
      (usage.insights?.routing.recent_decisions ?? []).slice(0, 8).map((decision) => ({
        ...decision,
        explanation: parseRoutingExplanation(decision.explanation_json),
      })),
    [usage.insights?.routing.recent_decisions],
  );
  const operatorHotspots = useMemo(
    () => operatorInsights?.hotspots.slice(0, 5) ?? [],
    [operatorInsights?.hotspots],
  );
  const operatorDrilldownRows = useMemo(
    () =>
      operatorInsights === null
        ? []
        : [
            {
              key: "operations",
              label: "Operations overview",
              state: operatorInsights.operations.state,
              severity: operatorInsights.operations.severity,
              summary: operatorInsights.operations.summary,
              action: operatorInsights.operations.recommended_action,
              path: operatorInsights.operations.drill_down.console_path,
            },
            {
              key: "provider",
              label: "Provider health",
              state: operatorInsights.provider_health.state,
              severity: operatorInsights.provider_health.severity,
              summary: operatorInsights.provider_health.summary,
              action: operatorInsights.provider_health.recommended_action,
              path: operatorInsights.provider_health.drill_down.console_path,
            },
            {
              key: "security",
              label: "Security posture",
              state: operatorInsights.security.state,
              severity: operatorInsights.security.severity,
              summary: operatorInsights.security.summary,
              action: operatorInsights.security.recommended_action,
              path: operatorInsights.security.drill_down.console_path,
            },
            {
              key: "recall",
              label: "Recall quality",
              state: operatorInsights.recall.state,
              severity: operatorInsights.recall.severity,
              summary: operatorInsights.recall.summary,
              action: operatorInsights.recall.recommended_action,
              path: operatorInsights.recall.drill_down.console_path,
            },
            {
              key: "compaction",
              label: "Compaction efficiency",
              state: operatorInsights.compaction.state,
              severity: operatorInsights.compaction.severity,
              summary: operatorInsights.compaction.summary,
              action: operatorInsights.compaction.recommended_action,
              path: operatorInsights.compaction.drill_down.console_path,
            },
            {
              key: "safety",
              label: "Safety boundary",
              state: operatorInsights.safety_boundary.state,
              severity: operatorInsights.safety_boundary.severity,
              summary: operatorInsights.safety_boundary.summary,
              action: operatorInsights.safety_boundary.recommended_action,
              path: operatorInsights.safety_boundary.drill_down.console_path,
            },
            {
              key: "plugins",
              label: "Plugin operability",
              state: operatorInsights.plugins.state,
              severity: operatorInsights.plugins.severity,
              summary: operatorInsights.plugins.summary,
              action: operatorInsights.plugins.recommended_action,
              path: operatorInsights.plugins.drill_down.console_path,
            },
            {
              key: "cron",
              label: "Cron delivery",
              state: operatorInsights.cron.state,
              severity: operatorInsights.cron.severity,
              summary: operatorInsights.cron.summary,
              action: operatorInsights.cron.recommended_action,
              path: operatorInsights.cron.drill_down.console_path,
            },
            {
              key: "routines",
              label: "Routine delivery",
              state: operatorInsights.routines.state,
              severity: operatorInsights.routines.severity,
              summary: operatorInsights.routines.summary,
              action: operatorInsights.routines.recommended_action,
              path: operatorInsights.routines.drill_down.console_path,
            },
            {
              key: "memory-learning",
              label: "Memory learning",
              state: operatorInsights.memory_learning.state,
              severity: operatorInsights.memory_learning.severity,
              summary: operatorInsights.memory_learning.summary,
              action: operatorInsights.memory_learning.recommended_action,
              path: operatorInsights.memory_learning.drill_down.console_path,
            },
            {
              key: "reload",
              label: "Reload hotspots",
              state: operatorInsights.reload.state,
              severity: operatorInsights.reload.severity,
              summary: operatorInsights.reload.summary,
              action: operatorInsights.reload.recommended_action,
              path: operatorInsights.reload.drill_down.console_path,
            },
          ],
    [operatorInsights],
  );
  const operatorSamplePreview = useMemo(() => {
    if (operatorInsights === null) {
      return [];
    }
    return [
      ...operatorInsights.recall.samples.slice(0, 2).map((sample) => ({
        key: `recall-${sample.run_id}-${sample.kind}`,
        label: "Recall",
        detail: `${sample.kind} ${sample.query_preview} (${sample.total_hits} hits, memory ${sample.memory_hits})`,
      })),
      ...operatorInsights.compaction.samples.slice(0, 2).map((sample) => ({
        key: `compaction-${sample.run_id}-${sample.trigger}`,
        label: "Compaction",
        detail: `${sample.trigger} ${sample.run_id} delta ${sample.token_delta}, output ${sample.estimated_output_tokens}`,
      })),
      ...operatorInsights.safety_boundary.samples.slice(0, 1).map((sample) => ({
        key: `safety-${sample.run_id}-${sample.tool_name}`,
        label: "Safety",
        detail: `${sample.tool_name} ${sample.reason}${sample.approval_required ? " (approval required)" : ""}`,
      })),
      ...operatorInsights.plugins.samples.slice(0, 1).map((sample) => ({
        key: `plugin-${sample.plugin_id}`,
        label: "Plugin",
        detail: `${sample.plugin_id} ${sample.reasons.join(", ") || "no reasons published"}`,
      })),
      ...operatorInsights.cron.samples.slice(0, 1).map((sample) => ({
        key: `cron-${sample.run_id}`,
        label: "Cron",
        detail: `${sample.job_id} ${sample.status}${sample.error_kind ? ` (${sample.error_kind})` : ""}`,
      })),
      ...operatorInsights.reload.hotspots.slice(0, 1).map((sample) => ({
        key: `reload-${sample.ref_id}`,
        label: "Reload",
        detail: `${sample.config_path} ${sample.state}${sample.advice ? `: ${sample.advice}` : ""}`,
      })),
    ].slice(0, 6);
  }, [operatorInsights]);
  const credentialAttentionCount =
    providerRegistry?.credentials.filter(
      (credential) => credential.availabilityState !== "available",
    ).length ?? 0;
  const budgetSignalCount = (usage.insights?.budgets.evaluations ?? []).filter(
    (entry) => entry.status !== "ok",
  ).length;
  const sharedLeaseWaiters =
    (readNumber(leaseManager ?? {}, "foreground_waiters") ?? 0) +
    (readNumber(leaseManager ?? {}, "background_waiters") ?? 0);
  const sharedLeaseDeferred = readNumber(leaseManager ?? {}, "deferred_total") ?? 0;
  const runtimeCredentialLabel =
    providerRegistry?.credentialId ?? providerRegistry?.credentialSource ?? "n/a";

  return (
    <main className="workspace-page">
      <WorkspacePageHeader
        eyebrow="Control"
        title="Usage"
        description="Track token volume, run throughput, and latency posture across sessions, agents, and model profiles without rebuilding the aggregates in the browser."
        status={
          <>
            <WorkspaceStatusChip tone={usage.busy ? "warning" : "success"}>
              {usage.busy ? "Refreshing" : "Aggregates ready"}
            </WorkspaceStatusChip>
            <WorkspaceStatusChip
              tone={usage.insights?.cost_tracking_available ? "success" : "default"}
            >
              {usage.insights?.cost_tracking_available ? "Cost estimates live" : "Cost unavailable"}
            </WorkspaceStatusChip>
            <WorkspaceStatusChip
              tone={
                usage.insights?.health.provider_state === "ok"
                  ? "success"
                  : (usage.insights?.alerts.length ?? 0) > 0
                    ? "warning"
                    : "default"
              }
            >
              {usage.insights?.health.provider_state ?? "unknown"} provider
            </WorkspaceStatusChip>
          </>
        }
        actions={
          <ActionButton
            isDisabled={usage.busy}
            type="button"
            variant="primary"
            onPress={() => void usage.refreshUsage()}
          >
            {usage.busy ? "Refreshing..." : "Refresh usage"}
          </ActionButton>
        }
      />

      <section className="workspace-metric-grid">
        <WorkspaceMetricCard
          detail="All prompt and completion tokens in the selected window."
          label="Total tokens"
          value={usage.summary?.totals.total_tokens ?? 0}
        />
        <WorkspaceMetricCard
          detail="Total orchestrator runs seen in the selected window."
          label="Runs"
          value={usage.summary?.totals.runs ?? 0}
        />
        <WorkspaceMetricCard
          detail="Estimated cost is always marked as an estimate and never pretends to be exact."
          label="Estimated cost"
          value={formatUsd(
            usage.insights?.model_mix.reduce(
              (total, row) => total + (row.estimated_cost_usd ?? 0),
              0,
            ),
          )}
        />
        <WorkspaceMetricCard
          detail="Average completed-run latency. In-progress runs stay excluded."
          label="Avg latency"
          value={formatLatency(usage.summary?.totals.average_latency_ms)}
        />
        <WorkspaceMetricCard
          detail="Registry-backed failover posture for the active provider fleet."
          label="Failover"
          tone={providerRegistry?.failoverEnabled ? "accent" : "default"}
          value={
            providerRegistry === null
              ? "n/a"
              : providerRegistry.failoverEnabled
                ? "enabled"
                : "disabled"
          }
        />
        <WorkspaceMetricCard
          detail="Reflection workload is tracked separately from foreground runs."
          label="Learning reflections"
          tone={
            (readNumber(learningCounters ?? {}, "reflections_scheduled") ?? 0) > 0
              ? "accent"
              : "default"
          }
          value={readNumber(learningCounters ?? {}, "reflections_scheduled") ?? 0}
        />
        <WorkspaceMetricCard
          detail={`Gate ${externalIndexGate}, freshness ${
            readNumber(externalIndexSlos ?? {}, "freshness_lag_ms") ?? 0
          } ms, fallback ${
            readNumber(externalIndexSlos ?? {}, "degraded_fallback_rate_bps") ?? 0
          } bps.`}
          label="Retrieval index"
          tone={workspaceToneForState(externalIndexState)}
          value={externalIndexState}
        />
        <WorkspaceMetricCard
          detail={
            operatorInsights === null
              ? "Refresh usage to load operator hotspots."
              : operatorInsights.summary.recommendation
          }
          label="Operator hotspots"
          tone={workspaceToneForState(operatorInsights?.summary.severity ?? "unknown")}
          value={operatorInsights?.summary.hotspot_count ?? 0}
        />
        <WorkspaceMetricCard
          detail="Blocking hotspots are the issues that should stop routine rollout until resolved."
          label="Blocking hotspots"
          tone={(operatorInsights?.summary.blocking_hotspots ?? 0) > 0 ? "danger" : "default"}
          value={operatorInsights?.summary.blocking_hotspots ?? 0}
        />
      </section>

      <WorkspaceSectionCard
        description="Time filters are pushed to the backend so the page, exports, and future CLI views all ride the same contract."
        title="Filters"
        actions={
          <>
            <ActionButton
              type="button"
              variant="secondary"
              onPress={() => usage.exportDataset("timeline", "csv")}
            >
              Export timeline CSV
            </ActionButton>
            <ActionButton
              type="button"
              variant="ghost"
              onPress={() => usage.exportDataset("timeline", "json")}
            >
              Export timeline JSON
            </ActionButton>
          </>
        }
      >
        <div className="workspace-form-grid">
          <SelectField
            label="Window"
            options={[
              { key: "24h", label: "Last 24 hours" },
              { key: "7d", label: "Last 7 days" },
              { key: "30d", label: "Last 30 days" },
              { key: "90d", label: "Last 90 days" },
            ]}
            value={usage.windowKey}
            onChange={(value) => usage.setWindowKey(value as "24h" | "7d" | "30d" | "90d")}
          />
          <SelectField
            label="Bucket"
            options={[
              { key: "auto", label: "Auto" },
              { key: "hour", label: "Hourly" },
              { key: "day", label: "Daily" },
            ]}
            value={usage.bucket}
            onChange={(value) => usage.setBucket(value as "auto" | "hour" | "day")}
          />
          <SwitchField
            checked={usage.includeArchived}
            description="Include archived sessions in usage totals and tables."
            label="Show archived"
            onChange={usage.setIncludeArchived}
          />
        </div>
      </WorkspaceSectionCard>

      {operatorInsights !== null ? (
        <section className="workspace-two-column">
          <WorkspaceSectionCard
            title="Operator posture"
            description="One aggregate now ties provider health, recall, compaction, safety, plugins, cron, and reload health into a single operator narrative."
          >
            <dl className="workspace-key-value-grid">
              <div>
                <dt>State</dt>
                <dd>{operatorInsights.summary.state}</dd>
              </div>
              <div>
                <dt>Severity</dt>
                <dd>{operatorInsights.summary.severity}</dd>
              </div>
              <div>
                <dt>Hotspots</dt>
                <dd>{operatorInsights.summary.hotspot_count}</dd>
              </div>
              <div>
                <dt>Blocking</dt>
                <dd>{operatorInsights.summary.blocking_hotspots}</dd>
              </div>
              <div>
                <dt>Warnings</dt>
                <dd>{operatorInsights.summary.warning_hotspots}</dd>
              </div>
              <div>
                <dt>Window</dt>
                <dd>
                  {formatUnixMs(operatorInsights.retention.window_start_at_unix_ms)} to{" "}
                  {formatUnixMs(operatorInsights.retention.window_end_at_unix_ms)}
                </dd>
              </div>
            </dl>
            <WorkspaceInlineNotice
              title="Recommended next step"
              tone={operatorInsights.summary.blocking_hotspots > 0 ? "warning" : "default"}
            >
              <p>{operatorInsights.summary.recommendation}</p>
            </WorkspaceInlineNotice>
            {operatorHotspots.length === 0 ? (
              <WorkspaceEmptyState
                compact
                title="No hotspots in the current window"
                description="The selected usage window did not produce operator hotspots."
              />
            ) : (
              <EntityTable
                ariaLabel="Operator hotspots"
                columns={[
                  { key: "subsystem", label: "Subsystem", render: (row) => row.subsystem },
                  {
                    key: "severity",
                    label: "Severity",
                    render: (row) => (
                      <WorkspaceStatusChip tone={workspaceToneForState(row.severity)}>
                        {row.severity}
                      </WorkspaceStatusChip>
                    ),
                  },
                  { key: "summary", label: "Summary", render: (row) => row.summary },
                  { key: "action", label: "Action", render: (row) => row.recommended_action },
                ]}
                getRowId={(row) => row.hotspot_id}
                rows={operatorHotspots}
              />
            )}
          </WorkspaceSectionCard>

          <WorkspaceSectionCard
            title="Operator drill-down"
            description="Every aggregate keeps the next operator move and the destination section explicit, so the page can guide follow-up instead of leaving operators in raw JSON."
          >
            <EntityTable
              ariaLabel="Operator drill-down"
              columns={[
                { key: "surface", label: "Surface", render: (row) => row.label },
                {
                  key: "state",
                  label: "State",
                  render: (row) => (
                    <div className="workspace-inline">
                      <WorkspaceStatusChip tone={workspaceToneForState(row.state)}>
                        {row.state}
                      </WorkspaceStatusChip>
                      <WorkspaceStatusChip tone={workspaceToneForState(row.severity)}>
                        {row.severity}
                      </WorkspaceStatusChip>
                    </div>
                  ),
                },
                { key: "summary", label: "Summary", render: (row) => row.summary },
                { key: "next", label: "Next step", render: (row) => row.action },
                {
                  key: "open",
                  label: "Open",
                  render: (row) => (
                    <ActionButton
                      size="sm"
                      type="button"
                      variant="ghost"
                      onPress={() => void navigate(row.path)}
                    >
                      Open
                    </ActionButton>
                  ),
                },
              ]}
              getRowId={(row) => row.key}
              rows={operatorDrilldownRows}
            />
            {operatorSamplePreview.length > 0 ? (
              <WorkspaceInlineNotice title="Recent samples" tone="default">
                <ul className="console-compact-list">
                  {operatorSamplePreview.map((item) => (
                    <li key={item.key}>
                      <strong>{item.label}</strong>: {item.detail}
                    </li>
                  ))}
                </ul>
              </WorkspaceInlineNotice>
            ) : null}
          </WorkspaceSectionCard>
        </section>
      ) : null}

      {usage.insights !== null ? (
        <section className="workspace-two-column">
          <WorkspaceSectionCard
            title="Routing and budgets"
            description="Usage governance now shares one backend contract for routing posture, budget evaluations, and overrides."
          >
            <dl className="workspace-key-value-grid">
              <div>
                <dt>Default mode</dt>
                <dd>{usage.insights.routing.default_mode}</dd>
              </div>
              <div>
                <dt>Overrides</dt>
                <dd>{usage.insights.routing.overrides}</dd>
              </div>
              <div>
                <dt>Provider health</dt>
                <dd>{usage.insights.health.provider_state}</dd>
              </div>
              <div>
                <dt>Budget signals</dt>
                <dd>{budgetSignalCount}</dd>
              </div>
              <div>
                <dt>Runtime provider</dt>
                <dd>{providerRegistry?.providerId ?? providerRegistry?.providerKind ?? "n/a"}</dd>
              </div>
              <div>
                <dt>Default chat model</dt>
                <dd>{providerRegistry?.defaultChatModelId ?? "n/a"}</dd>
              </div>
              <div>
                <dt>Runtime credential</dt>
                <dd>{runtimeCredentialLabel}</dd>
              </div>
              <div>
                <dt>Shared lease waiters</dt>
                <dd>{sharedLeaseWaiters}</dd>
              </div>
              <div>
                <dt>Lease deferrals</dt>
                <dd>{sharedLeaseDeferred}</dd>
              </div>
            </dl>
            {providerRegistry !== null ? (
              <WorkspaceInlineNotice
                title="Registry posture"
                tone={
                  credentialAttentionCount > 0 || sharedLeaseWaiters > 0
                    ? "warning"
                    : providerRegistry.failoverEnabled
                      ? "accent"
                      : "default"
                }
              >
                <p>
                  {providerRegistry.providers.length} providers,{" "}
                  {providerRegistry.credentials.length} credentials,{" "}
                  {providerRegistry.models.length} models, failover{" "}
                  {providerRegistry.failoverEnabled ? "enabled" : "disabled"}, response cache{" "}
                  {providerRegistry.responseCacheEnabled ? "enabled" : "disabled"}, credential
                  attention {credentialAttentionCount}, shared lease waiters {sharedLeaseWaiters},
                  deferrals {sharedLeaseDeferred}.
                </p>
              </WorkspaceInlineNotice>
            ) : null}
            {usage.insights.budgets.evaluations.length === 0 ? (
              <WorkspaceEmptyState
                compact
                title="No budget policies loaded"
                description="Budget governance is active only where policies are configured."
              />
            ) : (
              <EntityTable
                ariaLabel="Budget evaluations"
                columns={[
                  {
                    key: "policy",
                    label: "Policy",
                    render: (row) => row.policy_id,
                  },
                  {
                    key: "scope",
                    label: "Scope",
                    render: (row) => `${row.scope_kind}:${row.scope_id}`,
                  },
                  {
                    key: "status",
                    label: "Status",
                    render: (row) => row.status,
                  },
                  {
                    key: "message",
                    label: "Message",
                    render: (row) => row.message,
                  },
                  {
                    key: "action",
                    label: "Action",
                    render: (row) =>
                      canRequestBudgetOverride(row.status) ? (
                        <ActionButton
                          type="button"
                          variant="secondary"
                          isDisabled={usage.busy}
                          onPress={() => void usage.requestBudgetOverride(row.policy_id)}
                        >
                          Request override
                        </ActionButton>
                      ) : row.status === "override_applied" ? (
                        "Approved"
                      ) : (
                        "None"
                      ),
                  },
                ]}
                getRowId={(row) => row.policy_id}
                rows={usage.insights.budgets.evaluations}
              />
            )}
          </WorkspaceSectionCard>

          <WorkspaceSectionCard
            title="Alerts"
            description="Anomalies stay actionable: every alert keeps a reason, a scope, and the next operator move."
          >
            {usage.insights.alerts.length === 0 ? (
              <WorkspaceEmptyState
                compact
                title="No active alerts"
                description="The selected window did not produce a budget, routing, or cost anomaly."
              />
            ) : (
              <EntityTable
                ariaLabel="Usage alerts"
                columns={[
                  {
                    key: "kind",
                    label: "Kind",
                    render: (row) => row.alert_kind,
                  },
                  {
                    key: "severity",
                    label: "Severity",
                    render: (row) => row.severity,
                  },
                  {
                    key: "scope",
                    label: "Scope",
                    render: (row) => `${row.scope_kind}:${row.scope_id}`,
                  },
                  {
                    key: "summary",
                    label: "Summary",
                    render: (row) => row.summary,
                  },
                ]}
                getRowId={(row) => row.alert_id}
                rows={usage.insights.alerts}
              />
            )}
          </WorkspaceSectionCard>
        </section>
      ) : null}

      <section className="workspace-two-column">
        <WorkspaceSectionCard
          description="Recent buckets make the volume trend visible even before we add a richer charting lane."
          title="Timeline"
        >
          {timelinePreview.length === 0 ? (
            <WorkspaceEmptyState
              compact
              description="No buckets are available for the selected time window."
              title="No timeline data"
            />
          ) : (
            <EntityTable
              ariaLabel="Usage timeline"
              columns={[
                {
                  key: "bucket",
                  label: "Bucket",
                  render: (row: UsageTimelineBucket) => (
                    <div className="workspace-stack">
                      <strong>{formatUnixMs(row.bucket_start_unix_ms)}</strong>
                      <small className="text-muted">{formatUnixMs(row.bucket_end_unix_ms)}</small>
                    </div>
                  ),
                },
                { key: "runs", label: "Runs", render: (row) => row.runs },
                {
                  key: "tokens",
                  label: "Tokens",
                  render: (row) => row.total_tokens,
                },
                {
                  key: "latency",
                  label: "Avg latency",
                  render: (row) => formatLatency(row.average_latency_ms),
                },
              ]}
              getRowId={(row) => String(row.bucket_start_unix_ms)}
              rows={timelinePreview}
            />
          )}
        </WorkspaceSectionCard>

        <WorkspaceSectionCard
          description="Select a session from the top list to inspect runs and jump directly into session or run detail."
          title="Session detail"
        >
          {detail === null ? (
            <WorkspaceEmptyState
              compact
              description="Pick a session row below to load its recent runs and scoped timeline."
              title="No session selected"
            />
          ) : (
            <div className="workspace-stack">
              <div className="workspace-inline">
                <ActionButton
                  type="button"
                  variant="secondary"
                  onPress={() =>
                    void navigate(
                      `${getSectionPath("sessions")}?sessionId=${detail.session.session_id}`,
                    )
                  }
                >
                  Open in sessions
                </ActionButton>
                <ActionButton
                  isDisabled={(detail.session.last_run_id?.length ?? 0) === 0}
                  type="button"
                  variant="ghost"
                  onPress={() => {
                    const search = new URLSearchParams();
                    search.set("sessionId", detail.session.session_id);
                    if (detail.session.last_run_id) {
                      search.set("runId", detail.session.last_run_id);
                    }
                    void navigate(`${getSectionPath("chat")}?${search.toString()}`);
                  }}
                >
                  Open latest run
                </ActionButton>
                <ActionButton
                  type="button"
                  variant="ghost"
                  onPress={() =>
                    void navigate(
                      `${getSectionPath("inventory")}?deviceId=${detail.session.device_id}`,
                    )
                  }
                >
                  Open inventory
                </ActionButton>
              </div>

              <dl className="workspace-key-value-grid">
                <div>
                  <dt>Session key</dt>
                  <dd>{detail.session.session_key}</dd>
                </div>
                <div>
                  <dt>Total tokens</dt>
                  <dd>{detail.session.total_tokens}</dd>
                </div>
                <div>
                  <dt>Runs</dt>
                  <dd>{detail.session.runs}</dd>
                </div>
                <div>
                  <dt>Avg latency</dt>
                  <dd>{formatLatency(detail.session.average_latency_ms)}</dd>
                </div>
              </dl>

              {usage.insights?.pricing.estimate_only ? (
                <WorkspaceInlineNotice title="Estimate only" tone="default">
                  <p>
                    Pricing uses the shared catalog and is intentionally marked as an estimate-only
                    surface.
                  </p>
                </WorkspaceInlineNotice>
              ) : null}

              <EntityTable
                ariaLabel="Recent usage runs"
                columns={[
                  { key: "run", label: "Run", render: (row) => row.run_id },
                  { key: "state", label: "State", render: (row) => row.state },
                  {
                    key: "tokens",
                    label: "Tokens",
                    render: (row) => row.total_tokens,
                  },
                  {
                    key: "latency",
                    label: "Latency",
                    render: (row) => formatLatency(row.average_latency_ms),
                  },
                ]}
                emptyDescription="This session has no runs in the selected window."
                emptyTitle="No runs in scope"
                getRowId={(row) => row.run_id}
                rows={detail.runs.map((run) => ({
                  ...run,
                  average_latency_ms: run.completed_at_unix_ms
                    ? run.completed_at_unix_ms - run.started_at_unix_ms
                    : undefined,
                }))}
              />
            </div>
          )}
        </WorkspaceSectionCard>
      </section>

      <section className="workspace-three-column">
        <UsageTableCard
          description="Top token spenders are the fastest path into the session catalog."
          rows={usage.sessions}
          title="Top sessions"
          onExportCsv={() => usage.exportDataset("sessions", "csv")}
          onExportJson={() => usage.exportDataset("sessions", "json")}
          columns={[
            {
              key: "session",
              label: "Session",
              render: (row: UsageSessionRecord) => (
                <button
                  className="workspace-link-button"
                  onClick={() => usage.setSelectedSessionId(row.session_id)}
                  type="button"
                >
                  {row.session_label ?? row.session_key}
                </button>
              ),
            },
            {
              key: "tokens",
              label: "Tokens",
              render: (row) => row.total_tokens,
            },
            { key: "runs", label: "Runs", render: (row) => row.runs },
            {
              key: "latency",
              label: "Avg latency",
              render: (row) => formatLatency(row.average_latency_ms),
            },
          ]}
          getRowId={(row) => row.session_id}
        />

        <UsageTableCard
          description="Agent breakdowns stay inferred from session bindings and default model profiles."
          rows={usage.agents}
          title="Top agents"
          onExportCsv={() => usage.exportDataset("agents", "csv")}
          onExportJson={() => usage.exportDataset("agents", "json")}
          columns={[
            {
              key: "agent",
              label: "Agent",
              render: (row: UsageAgentRecord) => (
                <div className="workspace-stack">
                  <strong>{row.display_name}</strong>
                  <small className="text-muted">{row.agent_id}</small>
                </div>
              ),
            },
            {
              key: "tokens",
              label: "Tokens",
              render: (row) => row.total_tokens,
            },
            {
              key: "sessions",
              label: "Sessions",
              render: (row) => row.session_count,
            },
            {
              key: "latency",
              label: "Avg latency",
              render: (row) => formatLatency(row.average_latency_ms),
            },
          ]}
          getRowId={(row) => row.agent_id}
        />

        <UsageTableCard
          description="Model breakdowns are powered by the same backend contract used for export."
          rows={usage.models}
          title="Top models"
          onExportCsv={() => usage.exportDataset("models", "csv")}
          onExportJson={() => usage.exportDataset("models", "json")}
          columns={[
            {
              key: "model",
              label: "Model",
              render: (row: UsageModelRecord) => (
                <div className="workspace-stack">
                  <strong>{row.display_name}</strong>
                  <small className="text-muted">{row.model_source}</small>
                </div>
              ),
            },
            {
              key: "tokens",
              label: "Tokens",
              render: (row) => row.total_tokens,
            },
            {
              key: "agents",
              label: "Agents",
              render: (row) => row.agent_count,
            },
            {
              key: "latency",
              label: "Avg latency",
              render: (row) => formatLatency(row.average_latency_ms),
            },
          ]}
          getRowId={(row) => row.model_id}
        />
      </section>

      {usage.insights !== null ? (
        <section className="workspace-two-column">
          <UsageTableCard
            description="Model mix now includes estimate-only cost so routing drift is visible before it hurts the bill."
            rows={usage.insights.model_mix}
            title="Model mix"
            onExportCsv={() => usage.exportDataset("models", "csv")}
            onExportJson={() => usage.exportDataset("models", "json")}
            columns={[
              { key: "model", label: "Model", render: (row) => row.model_id },
              { key: "source", label: "Source", render: (row) => row.source },
              { key: "runs", label: "Runs", render: (row) => row.runs },
              {
                key: "cost",
                label: "Est. cost",
                render: (row) => formatUsd(row.estimated_cost_usd),
              },
            ]}
            getRowId={(row) => `${row.model_id}-${row.source}`}
          />

          <UsageTableCard
            description="Foreground vs. background usage stays explicit so routines and queue work do not disappear inside aggregate totals."
            rows={usage.insights.scope_mix}
            title="Foreground / background"
            onExportCsv={() => usage.exportDataset("timeline", "csv")}
            onExportJson={() => usage.exportDataset("timeline", "json")}
            columns={[
              { key: "scope", label: "Scope", render: (row) => row.scope },
              { key: "runs", label: "Runs", render: (row) => row.runs },
              {
                key: "tokens",
                label: "Tokens",
                render: (row) => row.total_tokens,
              },
              {
                key: "cost",
                label: "Est. cost",
                render: (row) => formatUsd(row.estimated_cost_usd),
              },
            ]}
            getRowId={(row) => row.scope}
          />
        </section>
      ) : null}

      {usage.insights !== null ? (
        <section className="workspace-two-column">
          <UsageTableCard
            description="Tool proposal volume is reconstructed from run transcripts so automation-heavy paths stay visible alongside model and scope mix."
            rows={usage.insights.tool_mix}
            title="Tool mix"
            onExportCsv={() => usage.exportDataset("timeline", "csv")}
            onExportJson={() => usage.exportDataset("timeline", "json")}
            columns={[
              { key: "tool", label: "Tool", render: (row) => row.tool_name },
              {
                key: "proposals",
                label: "Proposals",
                render: (row) => row.proposals,
              },
            ]}
            getRowId={(row) => row.tool_name}
          />

          <WorkspaceSectionCard
            title="Recent routing decisions"
            description="Stored routing decisions now surface provider, model delta, and a compact routing explanation instead of leaving operators in raw JSON."
          >
            {recentRoutingDecisions.length === 0 ? (
              <WorkspaceEmptyState
                compact
                title="No routing decisions loaded"
                description="The selected window did not produce stored routing decisions yet."
              />
            ) : (
              <EntityTable
                ariaLabel="Recent routing decisions"
                columns={[
                  { key: "run", label: "Run", render: (row) => row.run_id },
                  { key: "mode", label: "Mode", render: (row) => row.mode },
                  {
                    key: "task",
                    label: "Task",
                    render: (row) => row.explanation.taskClass ?? "primary_interactive",
                  },
                  {
                    key: "provider",
                    label: "Provider",
                    render: (row) => `${row.provider_kind}:${row.provider_id}`,
                  },
                  {
                    key: "model",
                    label: "Model",
                    render: (row) => (
                      <div className="workspace-stack">
                        <strong>{row.actual_model_id}</strong>
                        <small className="text-muted">default {row.default_model_id}</small>
                      </div>
                    ),
                  },
                  {
                    key: "explanation",
                    label: "Explanation",
                    render: (row) => {
                      const summary = row.explanation.explanation.slice(0, 2).join(" / ");
                      const reasons = row.explanation.reasonCodes.slice(0, 2).join(", ");
                      const lease = row.explanation.lease;
                      const leaseSummary =
                        lease?.state === "waiting"
                          ? `lease wait ${lease.estimatedWaitMs ?? 0} ms`
                          : lease?.state === "deferred"
                            ? "lease deferred"
                            : "";
                      return summary || reasons || leaseSummary || "No explanation";
                    },
                  },
                  {
                    key: "budget",
                    label: "Budget",
                    render: (row) => row.explanation.budgetOutcome ?? row.budget_outcome ?? "ok",
                  },
                ]}
                getRowId={(row) => row.decision_id}
                rows={recentRoutingDecisions}
              />
            )}
          </WorkspaceSectionCard>
        </section>
      ) : null}
    </main>
  );
}

type UsageTableCardProps<T extends object> = {
  title: string;
  description: string;
  rows: readonly T[];
  columns: Parameters<typeof EntityTable<T>>[0]["columns"];
  getRowId: (row: T, index: number) => string;
  onExportCsv: () => void;
  onExportJson: () => void;
};

function UsageTableCard<T extends object>({
  title,
  description,
  rows,
  columns,
  getRowId,
  onExportCsv,
  onExportJson,
}: UsageTableCardProps<T>) {
  return (
    <WorkspaceSectionCard
      description={description}
      title={title}
      actions={
        <>
          <ActionButton type="button" variant="secondary" onPress={onExportCsv}>
            CSV
          </ActionButton>
          <ActionButton type="button" variant="ghost" onPress={onExportJson}>
            JSON
          </ActionButton>
        </>
      }
    >
      {rows.length === 0 ? (
        <WorkspaceEmptyState
          compact
          description="No records are available for the selected window."
          title="No data"
        />
      ) : (
        <EntityTable ariaLabel={title} columns={columns} getRowId={getRowId} rows={rows} />
      )}
    </WorkspaceSectionCard>
  );
}

function formatLatency(value: number | undefined): string {
  if (value === undefined || !Number.isFinite(value)) {
    return "n/a";
  }
  if (value < 1000) {
    return `${Math.round(value)} ms`;
  }
  return `${(value / 1000).toFixed(1)} s`;
}

function formatUsd(value: number | undefined): string {
  if (value === undefined || !Number.isFinite(value)) {
    return "n/a";
  }
  return `$${value.toFixed(2)}`;
}

function canRequestBudgetOverride(status: string): boolean {
  return status === "approval_required" || status === "blocked" || status === "hard_limit";
}
