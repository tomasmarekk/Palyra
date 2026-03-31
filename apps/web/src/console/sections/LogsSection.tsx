import { useNavigate } from "react-router-dom";

import { getSectionPath } from "../navigation";
import { ActionButton, SelectField, SwitchField, TextInputField } from "../components/ui";
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
} from "../components/workspace/WorkspacePatterns";
import { useLogsDomain } from "../hooks/useLogsDomain";
import { PrettyJsonBlock, formatUnixMs } from "../shared";
import type { ConsoleAppState } from "../useConsoleAppState";

type LogsSectionProps = {
  app: Pick<ConsoleAppState, "api" | "setError" | "setNotice" | "revealSensitiveValues">;
};

export function LogsSection({ app }: LogsSectionProps) {
  const navigate = useNavigate();
  const logs = useLogsDomain(app);
  const errorCount = logs.records.filter((record) => record.severity === "error").length;
  const warningCount = logs.records.filter((record) => record.severity === "warning").length;

  return (
    <main className="workspace-page">
      <WorkspacePageHeader
        eyebrow="Control"
        title="Logs"
        description="Query the shared log stream for palyrad, browserd, and channel activity with the same filter contract that export and future CLI follow mode use."
        status={
          <>
            <WorkspaceStatusChip tone={logs.busy ? "warning" : "success"}>
              {logs.busy ? "Refreshing" : "Stream ready"}
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={logs.follow ? "accent" : "default"}>
              {logs.follow ? "Auto-follow on" : "Manual refresh"}
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={logs.page?.has_more ? "warning" : "default"}>
              {logs.page?.has_more ? "Showing latest slice" : "Full slice loaded"}
            </WorkspaceStatusChip>
          </>
        }
        actions={
          <div className="workspace-inline">
            <ActionButton type="button" variant="secondary" onPress={() => logs.exportLogs("csv")}>
              Export CSV
            </ActionButton>
            <ActionButton type="button" variant="ghost" onPress={() => logs.exportLogs("json")}>
              Export JSON
            </ActionButton>
            <ActionButton
              isDisabled={logs.busy}
              type="button"
              variant="primary"
              onPress={() => void logs.refreshLogs()}
            >
              {logs.busy ? "Refreshing..." : "Refresh logs"}
            </ActionButton>
          </div>
        }
      />

      <section className="workspace-metric-grid">
        <WorkspaceMetricCard
          detail="Records visible in the current filtered slice."
          label="Visible records"
          value={logs.records.length}
        />
        <WorkspaceMetricCard
          detail="Distinct sources published in the current response."
          label="Sources"
          value={logs.availableSources.length}
        />
        <WorkspaceMetricCard
          detail="Error-class events in the current filtered slice."
          label="Errors"
          tone={errorCount > 0 ? "warning" : "default"}
          value={errorCount}
        />
        <WorkspaceMetricCard
          detail="Warnings stay separate from errors so noisy but non-fatal posture is visible."
          label="Warnings"
          tone={warningCount > 0 ? "accent" : "default"}
          value={warningCount}
        />
      </section>

      <WorkspaceSectionCard
        description="Source, severity, text, and time filters stay backend-owned so exports and polling do not drift from the UI."
        title="Filters"
      >
        <div className="workspace-form-grid">
          <SelectField
            label="Window"
            options={[
              { key: "15m", label: "Last 15 minutes" },
              { key: "1h", label: "Last hour" },
              { key: "24h", label: "Last 24 hours" },
              { key: "7d", label: "Last 7 days" },
            ]}
            value={logs.windowKey}
            onChange={(value) => logs.setWindowKey(value as "15m" | "1h" | "24h" | "7d")}
          />
          <SelectField
            label="Source"
            options={[
              { key: "", label: "All sources" },
              ...logs.availableSources.map((value) => ({ key: value, label: value })),
            ]}
            value={logs.source}
            onChange={logs.setSource}
          />
          <SelectField
            label="Severity"
            options={[
              { key: "", label: "All severities" },
              { key: "error", label: "Error" },
              { key: "warning", label: "Warning" },
              { key: "info", label: "Info" },
              { key: "debug", label: "Debug" },
            ]}
            value={logs.severity}
            onChange={logs.setSeverity}
          />
          <TextInputField
            label="Contains"
            placeholder="message or structured payload text"
            value={logs.query}
            onChange={logs.setQuery}
          />
          <SwitchField
            checked={logs.follow}
            description="Poll newer records using the newest cursor in the current slice."
            label="Auto-follow"
            onChange={logs.setFollow}
          />
        </div>
      </WorkspaceSectionCard>

      {logs.page?.has_more ? (
        <WorkspaceInlineNotice title="Latest slice only" tone="warning">
          <p>
            The current result is truncated to the newest {logs.page.limit} records. Narrow filters
            or export the slice if you need an external handoff.
          </p>
        </WorkspaceInlineNotice>
      ) : null}

      <section className="workspace-two-column">
        <WorkspaceSectionCard
          description="Select a row to inspect structured payload and jump into the related console surface."
          title="Stream"
        >
          {logs.records.length === 0 ? (
            <WorkspaceEmptyState
              description="Adjust filters or wait for more activity to populate the shared log stream."
              title="No records in scope"
            />
          ) : (
            <WorkspaceTable
              ariaLabel="Shared log stream"
              columns={["When", "Source", "Severity", "Message"]}
            >
              {logs.records.map((record) => {
                const selected = logs.selectedCursor === record.cursor;
                return (
                  <tr
                    key={record.cursor}
                    className={selected ? "bg-content2/60" : undefined}
                    onClick={() => logs.setSelectedCursor(record.cursor)}
                  >
                    <td>{formatUnixMs(record.timestamp_unix_ms)}</td>
                    <td>{record.source}</td>
                    <td>{record.severity}</td>
                    <td>{record.message}</td>
                  </tr>
                );
              })}
            </WorkspaceTable>
          )}
        </WorkspaceSectionCard>

        <WorkspaceSectionCard
          description="Record detail stays redacted and explicit so support handoffs can quote exactly what the operator saw."
          title="Detail"
        >
          {logs.selectedRecord === null ? (
            <WorkspaceEmptyState
              compact
              description="Select a log row from the shared stream to inspect payload and related identifiers."
              title="No record selected"
            />
          ) : (
            <div className="workspace-stack">
              <div className="workspace-inline">
                {logs.selectedRecord.session_id ? (
                  <ActionButton
                    type="button"
                    variant="secondary"
                    onPress={() =>
                      void navigate(
                        `${getSectionPath("sessions")}?sessionId=${logs.selectedRecord?.session_id ?? ""}`,
                      )
                    }
                  >
                    Open session
                  </ActionButton>
                ) : null}
                {logs.selectedRecord.connector_id ? (
                  <ActionButton
                    type="button"
                    variant="secondary"
                    onPress={() => void navigate(getSectionPath("channels"))}
                  >
                    Open channel
                  </ActionButton>
                ) : null}
                {logs.selectedRecord.device_id ? (
                  <ActionButton
                    type="button"
                    variant="ghost"
                    onPress={() =>
                      void navigate(
                        `${getSectionPath("inventory")}?deviceId=${logs.selectedRecord?.device_id ?? ""}`,
                      )
                    }
                  >
                    Open inventory
                  </ActionButton>
                ) : null}
              </div>

              <dl className="workspace-key-value-grid">
                <div>
                  <dt>Cursor</dt>
                  <dd>{logs.selectedRecord.cursor}</dd>
                </div>
                <div>
                  <dt>Event</dt>
                  <dd>{logs.selectedRecord.event_name ?? "n/a"}</dd>
                </div>
                <div>
                  <dt>Session</dt>
                  <dd>{logs.selectedRecord.session_id ?? "n/a"}</dd>
                </div>
                <div>
                  <dt>Run</dt>
                  <dd>{logs.selectedRecord.run_id ?? "n/a"}</dd>
                </div>
                <div>
                  <dt>Device</dt>
                  <dd>{logs.selectedRecord.device_id ?? "n/a"}</dd>
                </div>
                <div>
                  <dt>Connector</dt>
                  <dd>{logs.selectedRecord.connector_id ?? "n/a"}</dd>
                </div>
              </dl>

              {logs.selectedRecord.structured_payload === undefined ? (
                <WorkspaceEmptyState
                  compact
                  description="This record does not currently publish structured detail."
                  title="No structured payload"
                />
              ) : (
                <PrettyJsonBlock
                  revealSensitiveValues={app.revealSensitiveValues}
                  value={logs.selectedRecord.structured_payload}
                />
              )}
            </div>
          )}
        </WorkspaceSectionCard>
      </section>
    </main>
  );
}
