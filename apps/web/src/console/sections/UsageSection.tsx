import { useMemo } from "react";
import { useNavigate } from "react-router-dom";

import type {
  UsageAgentRecord,
  UsageModelRecord,
  UsageSessionRecord,
  UsageTimelineBucket,
} from "../../consoleApi";
import { getSectionPath } from "../navigation";
import { ActionButton, EntityTable, SelectField, SwitchField } from "../components/ui";
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
import { formatUnixMs, readNumber, readObject } from "../shared";
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
  const learning = readObject(app.memoryStatus ?? {}, "learning");
  const learningCounters = readObject(learning ?? {}, "counters");
  const timelinePreview = useMemo(
    () => (usage.summary?.timeline ?? []).slice(-8).reverse(),
    [usage.summary?.timeline],
  );
  const recentRoutingDecisions = useMemo(
    () =>
      (usage.insights?.routing.recent_decisions ?? []).slice(0, 8).map((decision) => ({
        ...decision,
        explanation: parseRoutingExplanation(decision.explanation_json),
      })),
    [usage.insights?.routing.recent_decisions],
  );

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
                <dd>
                  {
                    usage.insights.budgets.evaluations.filter((entry) => entry.status !== "ok")
                      .length
                  }
                </dd>
              </div>
              <div>
                <dt>Runtime provider</dt>
                <dd>{providerRegistry?.providerId ?? providerRegistry?.providerKind ?? "n/a"}</dd>
              </div>
              <div>
                <dt>Default chat model</dt>
                <dd>{providerRegistry?.defaultChatModelId ?? "n/a"}</dd>
              </div>
            </dl>
            {providerRegistry !== null ? (
              <WorkspaceInlineNotice
                title="Registry posture"
                tone={providerRegistry.failoverEnabled ? "accent" : "default"}
              >
                <p>
                  {providerRegistry.providers.length} providers, {providerRegistry.models.length}{" "}
                  models, failover {providerRegistry.failoverEnabled ? "enabled" : "disabled"},
                  response cache {providerRegistry.responseCacheEnabled ? "enabled" : "disabled"}.
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
                    render: (row) =>
                      row.explanation.explanation.slice(0, 2).join(" / ") || "No explanation",
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
