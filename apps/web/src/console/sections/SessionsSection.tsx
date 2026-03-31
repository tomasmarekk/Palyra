import { Chip } from "@heroui/react";
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
  workspaceToneForState,
} from "../components/workspace/WorkspacePatterns";
import { useSessionCatalogDomain } from "../hooks/useSessionCatalogDomain";
import { formatUnixMs } from "../shared";
import type { ConsoleAppState } from "../useConsoleAppState";

type SessionsSectionProps = {
  app: Pick<ConsoleAppState, "api" | "setError" | "setNotice">;
};

export function SessionsSection({ app }: SessionsSectionProps) {
  const navigate = useNavigate();
  const catalog = useSessionCatalogDomain(app);
  const selected = catalog.selectedSession;

  return (
    <main className="workspace-page">
      <WorkspacePageHeader
        eyebrow="Control"
        title="Sessions"
        description="Search session history, inspect latest run posture, and drive lifecycle actions without leaving the operator console."
        status={
          <>
            <WorkspaceStatusChip tone={catalog.busy ? "warning" : "success"}>
              {catalog.busy ? "Refreshing" : "Catalog ready"}
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={selected?.pending_approvals ? "warning" : "default"}>
              {selected?.pending_approvals ?? 0} pending approvals
            </WorkspaceStatusChip>
            <WorkspaceStatusChip
              tone={workspaceToneForState(selected?.last_run_state ?? "unknown")}
            >
              {selected?.last_run_state ?? "No run selected"}
            </WorkspaceStatusChip>
          </>
        }
        actions={
          <ActionButton
            isDisabled={catalog.busy}
            type="button"
            variant="primary"
            onPress={() => void catalog.refreshSessions()}
          >
            {catalog.busy ? "Refreshing..." : "Refresh sessions"}
          </ActionButton>
        }
      />

      <section className="workspace-metric-grid">
        <WorkspaceMetricCard
          detail="Visible non-archived sessions in the current scoped catalog."
          label="Active sessions"
          value={catalog.summary?.active_sessions ?? 0}
        />
        <WorkspaceMetricCard
          detail="Archived records stay queryable without reopening the chat rail."
          label="Archived sessions"
          value={catalog.summary?.archived_sessions ?? 0}
        />
        <WorkspaceMetricCard
          detail="Sessions currently waiting on sensitive-action decisions."
          label="Pending approvals"
          tone={(catalog.summary?.sessions_with_pending_approvals ?? 0) > 0 ? "warning" : "default"}
          value={catalog.summary?.sessions_with_pending_approvals ?? 0}
        />
        <WorkspaceMetricCard
          detail="Latest known run is still accepted or in progress."
          label="Active runs"
          tone={(catalog.summary?.sessions_with_active_runs ?? 0) > 0 ? "accent" : "default"}
          value={catalog.summary?.sessions_with_active_runs ?? 0}
        />
      </section>

      <WorkspaceSectionCard
        description="Catalog filters stay server-backed so chat, web, and future operator surfaces do not invent separate session logic."
        title="Filters"
      >
        <div className="workspace-form-grid">
          <TextInputField
            label="Search"
            placeholder="title, key, preview, or run state"
            value={catalog.query}
            onChange={catalog.setQuery}
          />
          <SelectField
            label="Sort"
            options={[
              { key: "updated_desc", label: "Updated (newest)" },
              { key: "updated_asc", label: "Updated (oldest)" },
              { key: "created_desc", label: "Created (newest)" },
              { key: "created_asc", label: "Created (oldest)" },
              { key: "title_asc", label: "Title (A-Z)" },
            ]}
            value={catalog.sort}
            onChange={(value) =>
              catalog.setSort(
                value as
                  | "updated_desc"
                  | "updated_asc"
                  | "created_desc"
                  | "created_asc"
                  | "title_asc",
              )
            }
          />
          <SwitchField
            checked={catalog.includeArchived}
            description="Include archived records in the current list."
            label="Show archived"
            onChange={catalog.setIncludeArchived}
          />
        </div>
      </WorkspaceSectionCard>

      <section className="workspace-two-column">
        <WorkspaceSectionCard
          description="Pick a session to inspect its latest activity, preview, and lifecycle state."
          title="Catalog"
        >
          {catalog.entries.length === 0 ? (
            <WorkspaceEmptyState
              description="Adjust filters or create activity in chat to populate the session catalog."
              title="No sessions match the current query"
            />
          ) : (
            <WorkspaceTable
              ariaLabel="Session catalog"
              columns={["Title", "Updated", "Run state", "Approvals", "Preview"]}
            >
              {catalog.entries.map((entry) => {
                const selectedRow = entry.session_id === catalog.selectedSessionId;
                return (
                  <tr
                    key={entry.session_id}
                    className={selectedRow ? "bg-content2/60" : undefined}
                    onClick={() => catalog.setSelectedSessionId(entry.session_id)}
                  >
                    <td>
                      <div className="workspace-stack">
                        <strong>{entry.title}</strong>
                        <small className="text-muted">
                          {entry.title_source} · {entry.archived ? "archived" : "active"}
                        </small>
                      </div>
                    </td>
                    <td>{formatUnixMs(entry.updated_at_unix_ms)}</td>
                    <td>
                      <Chip size="sm" variant="secondary">
                        {entry.last_run_state ?? "none"}
                      </Chip>
                    </td>
                    <td>{entry.pending_approvals}</td>
                    <td>{entry.preview ?? "No preview"}</td>
                  </tr>
                );
              })}
            </WorkspaceTable>
          )}
        </WorkspaceSectionCard>

        <WorkspaceSectionCard
          description="Lifecycle actions here reuse the same backend mutations as chat instead of inventing a separate control path."
          title="Detail"
        >
          {selected === null ? (
            <WorkspaceEmptyState
              compact
              description="Select a row from the session catalog to inspect details and actions."
              title="No session selected"
            />
          ) : (
            <div className="workspace-stack">
              <div className="workspace-panel__intro">
                <p className="workspace-kicker">Selected session</p>
                <h3>{selected.title}</h3>
                <p className="chat-muted">
                  {selected.preview ?? "No preview was derivable from existing run history."}
                </p>
              </div>

              <TextInputField
                disabled={catalog.busy}
                label="Session label"
                value={catalog.renameDraft}
                onChange={catalog.setRenameDraft}
              />

              <div className="workspace-inline">
                <ActionButton
                  isDisabled={catalog.busy}
                  type="button"
                  variant="primary"
                  onPress={() => void catalog.renameSelectedSession()}
                >
                  Rename
                </ActionButton>
                <ActionButton
                  isDisabled={catalog.busy}
                  type="button"
                  variant="secondary"
                  onPress={() => void catalog.resetSelectedSession()}
                >
                  Reset
                </ActionButton>
                <ActionButton
                  isDisabled={catalog.busy}
                  type="button"
                  variant="danger"
                  onPress={() => void catalog.archiveSelectedSession()}
                >
                  Archive
                </ActionButton>
                <ActionButton
                  isDisabled={catalog.busy || !selected.last_run_id}
                  type="button"
                  variant="ghost"
                  onPress={() => void catalog.abortSelectedRun()}
                >
                  Abort run
                </ActionButton>
              </div>

              <ActionButton
                type="button"
                variant="secondary"
                onPress={() => {
                  const search = new URLSearchParams();
                  search.set("sessionId", selected.session_id);
                  if (selected.last_run_id) {
                    search.set("runId", selected.last_run_id);
                  }
                  void navigate(`${getSectionPath("chat")}?${search.toString()}`);
                }}
              >
                Open in chat
              </ActionButton>
              <ActionButton
                type="button"
                variant="ghost"
                onPress={() =>
                  void navigate(`${getSectionPath("inventory")}?deviceId=${selected.device_id}`)
                }
              >
                Open inventory
              </ActionButton>

              <dl className="workspace-key-value-grid">
                <div>
                  <dt>Session key</dt>
                  <dd>{selected.session_key}</dd>
                </div>
                <div>
                  <dt>Created</dt>
                  <dd>{formatUnixMs(selected.created_at_unix_ms)}</dd>
                </div>
                <div>
                  <dt>Updated</dt>
                  <dd>{formatUnixMs(selected.updated_at_unix_ms)}</dd>
                </div>
                <div>
                  <dt>Run state</dt>
                  <dd>{selected.last_run_state ?? "none"}</dd>
                </div>
                <div>
                  <dt>Total tokens</dt>
                  <dd>{selected.total_tokens}</dd>
                </div>
                <div>
                  <dt>Pending approvals</dt>
                  <dd>{selected.pending_approvals}</dd>
                </div>
              </dl>

              {selected.last_intent || selected.last_summary ? (
                <WorkspaceInlineNotice title="Latest activity" tone="default">
                  <p>
                    <strong>Last intent:</strong> {selected.last_intent ?? "Missing"}
                  </p>
                  <p>
                    <strong>Last summary:</strong> {selected.last_summary ?? "Missing"}
                  </p>
                </WorkspaceInlineNotice>
              ) : null}
            </div>
          )}
        </WorkspaceSectionCard>
      </section>
    </main>
  );
}
