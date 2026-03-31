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
import { formatUnixMs } from "../shared";
import type { ConsoleAppState } from "../useConsoleAppState";

type UsageSectionProps = {
  app: Pick<ConsoleAppState, "api" | "setError" | "setNotice">;
};

export function UsageSection({ app }: UsageSectionProps) {
  const navigate = useNavigate();
  const usage = useUsageDomain(app);
  const detail = usage.selectedSessionDetail;
  const timelinePreview = useMemo(
    () => (usage.summary?.timeline ?? []).slice(-8).reverse(),
    [usage.summary?.timeline],
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
              tone={usage.summary?.cost_tracking_available ? "success" : "default"}
            >
              {usage.summary?.cost_tracking_available ? "Cost tracking live" : "Cost unavailable"}
            </WorkspaceStatusChip>
            <WorkspaceStatusChip
              tone={(usage.summary?.totals.active_runs ?? 0) > 0 ? "accent" : "default"}
            >
              {usage.summary?.totals.active_runs ?? 0} active runs
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
          detail="Distinct sessions with activity in the selected window."
          label="Active sessions"
          value={usage.summary?.totals.session_count ?? 0}
        />
        <WorkspaceMetricCard
          detail="Average completed-run latency. In-progress runs stay excluded."
          label="Avg latency"
          value={formatLatency(usage.summary?.totals.average_latency_ms)}
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
                { key: "tokens", label: "Tokens", render: (row) => row.total_tokens },
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

              {detail.cost_tracking_available ? null : (
                <WorkspaceInlineNotice title="Cost placeholder" tone="default">
                  <p>
                    Pricing metadata is not persisted yet, so cost fields remain unavailable by
                    contract.
                  </p>
                </WorkspaceInlineNotice>
              )}

              <EntityTable
                ariaLabel="Recent usage runs"
                columns={[
                  { key: "run", label: "Run", render: (row) => row.run_id },
                  { key: "state", label: "State", render: (row) => row.state },
                  { key: "tokens", label: "Tokens", render: (row) => row.total_tokens },
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
            { key: "tokens", label: "Tokens", render: (row) => row.total_tokens },
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
            { key: "tokens", label: "Tokens", render: (row) => row.total_tokens },
            { key: "sessions", label: "Sessions", render: (row) => row.session_count },
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
            { key: "tokens", label: "Tokens", render: (row) => row.total_tokens },
            { key: "agents", label: "Agents", render: (row) => row.agent_count },
            {
              key: "latency",
              label: "Avg latency",
              render: (row) => formatLatency(row.average_latency_ms),
            },
          ]}
          getRowId={(row) => row.model_id}
        />
      </section>
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
