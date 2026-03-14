import { Button } from "@heroui/react";

import { WorkspaceMetricCard, WorkspacePageHeader, WorkspaceSectionCard, WorkspaceStatusChip } from "../components/workspace/WorkspaceChrome";
import { PrettyJsonBlock, formatUnixMs, readObject, readString, type JsonObject } from "../shared";
import type { ConsoleAppState } from "../useConsoleAppState";

type ApprovalsSectionProps = {
  app: Pick<
    ConsoleAppState,
    | "approvalsBusy"
    | "approvals"
    | "approvalId"
    | "setApprovalId"
    | "approvalReason"
    | "setApprovalReason"
    | "approvalScope"
    | "setApprovalScope"
    | "refreshApprovals"
    | "decideApproval"
    | "revealSensitiveValues"
  >;
};

export function ApprovalsSection({ app }: ApprovalsSectionProps) {
  const pendingApprovals = app.approvals.filter((approval) => readString(approval, "decision") === null);
  const selectedApproval =
    app.approvals.find((approval) => readString(approval, "approval_id") === app.approvalId) ??
    app.approvals[0] ??
    null;
  const selectedApprovalId = readString(selectedApproval ?? {}, "approval_id") ?? "";
  const prompt = readObject(selectedApproval ?? {}, "prompt");
  const policySnapshot = readObject(selectedApproval ?? {}, "policy_snapshot");

  return (
    <main className="workspace-page">
      <WorkspacePageHeader
        eyebrow="Control"
        title="Approvals"
        description="Review sensitive actions in context, keep the queue readable, and make explicit allow or deny decisions without leaving the workspace."
        status={
          <>
            <WorkspaceStatusChip tone={pendingApprovals.length > 0 ? "warning" : "success"}>
              {pendingApprovals.length} pending
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={selectedApproval === null ? "default" : "success"}>
              {selectedApproval === null ? "Nothing selected" : "Detail panel ready"}
            </WorkspaceStatusChip>
          </>
        }
        actions={
          <Button
            variant="secondary"
            onPress={() => void app.refreshApprovals()}
            isDisabled={app.approvalsBusy}
          >
            {app.approvalsBusy ? "Refreshing..." : "Refresh approvals"}
          </Button>
        }
      />

      <section className="workspace-metric-grid workspace-metric-grid--compact">
        <WorkspaceMetricCard
          label="Review queue"
          value={pendingApprovals.length}
          detail={pendingApprovals[0] === undefined ? "No pending approvals" : readString(pendingApprovals[0], "request_summary") ?? "Approval queued"}
          tone={pendingApprovals.length > 0 ? "warning" : "success"}
        />
        <WorkspaceMetricCard
          label="Resolved"
          value={app.approvals.length - pendingApprovals.length}
          detail="Previously handled approval records still stay visible for context."
        />
        <WorkspaceMetricCard
          label="Selected"
          value={selectedApprovalId.length > 0 ? selectedApprovalId : "None"}
          detail={selectedApproval === null ? "Choose an item from the queue." : readString(selectedApproval, "subject_type") ?? "Unknown subject"}
        />
      </section>

      <section className="workspace-two-column workspace-two-column--queue">
        <WorkspaceSectionCard
          title="Approval queue"
          description="Pending work stays easy to scan, with resolved items still available for follow-up context."
        >
          <div className="workspace-list workspace-list--queue">
            {app.approvals.length === 0 ? (
              <p className="chat-muted">No approval records loaded.</p>
            ) : (
              app.approvals.map((approval) => {
                const approvalId = readString(approval, "approval_id") ?? "unknown";
                const decision = readString(approval, "decision");
                const isActive = approvalId === selectedApprovalId;
                return (
                  <button
                    key={approvalId}
                    type="button"
                    className={`workspace-list-button${isActive ? " is-active" : ""}`}
                    onClick={() => app.setApprovalId(approvalId)}
                  >
                    <div>
                      <strong>{readString(approval, "request_summary") ?? approvalId}</strong>
                      <p className="chat-muted">
                        {readString(approval, "subject_type") ?? "unknown subject"} ·{" "}
                        {formatUnixMs(readUnixMillis(approval, "requested_at_unix_ms"))}
                      </p>
                    </div>
                    <WorkspaceStatusChip tone={decision === null ? "warning" : "default"}>
                      {decision ?? "pending"}
                    </WorkspaceStatusChip>
                  </button>
                );
              })
            )}
          </div>
        </WorkspaceSectionCard>

        <WorkspaceSectionCard
          title="Approval detail"
          description="Keep the selected request, context, and decision controls on one surface."
        >
          {selectedApproval === null ? (
            <p className="chat-muted">Select an approval to inspect request context and decide it.</p>
          ) : (
            <div className="workspace-stack">
              <div className="workspace-callout">
                <div className="workspace-list-item">
                  <div>
                    <p className="console-label">Selected approval</p>
                    <strong>{selectedApprovalId}</strong>
                  </div>
                  <WorkspaceStatusChip
                    tone={readString(selectedApproval, "decision") === null ? "warning" : "default"}
                  >
                    {readString(selectedApproval, "decision") ?? "pending"}
                  </WorkspaceStatusChip>
                </div>
                <p className="chat-muted">
                  {readString(selectedApproval, "request_summary") ?? "No summary published."}
                </p>
              </div>

              <dl className="workspace-key-value-grid">
                <div>
                  <dt>Subject type</dt>
                  <dd>{readString(selectedApproval, "subject_type") ?? "n/a"}</dd>
                </div>
                <div>
                  <dt>Subject ID</dt>
                  <dd>{readString(selectedApproval, "subject_id") ?? "n/a"}</dd>
                </div>
                <div>
                  <dt>Principal</dt>
                  <dd>{readString(selectedApproval, "principal") ?? "n/a"}</dd>
                </div>
                <div>
                  <dt>Requested</dt>
                  <dd>{formatUnixMs(readUnixMillis(selectedApproval, "requested_at_unix_ms"))}</dd>
                </div>
                <div>
                  <dt>Session</dt>
                  <dd>{readString(selectedApproval, "session_id") ?? "n/a"}</dd>
                </div>
                <div>
                  <dt>Run</dt>
                  <dd>{readString(selectedApproval, "run_id") ?? "n/a"}</dd>
                </div>
              </dl>

              {prompt !== null && (
                <div className="workspace-callout">
                  <p className="console-label">Prompt context</p>
                  <strong>{readString(prompt, "title") ?? "Untitled approval prompt"}</strong>
                  <p className="chat-muted">
                    {readString(prompt, "summary") ?? "No prompt summary published."}
                  </p>
                  <div className="workspace-inline">
                    <WorkspaceStatusChip tone="warning">
                      Risk {readString(prompt, "risk_level") ?? "unspecified"}
                    </WorkspaceStatusChip>
                    <WorkspaceStatusChip tone="default">
                      Timeout {readString(prompt, "timeout_seconds") ?? "n/a"}s
                    </WorkspaceStatusChip>
                  </div>
                </div>
              )}

              <div className="workspace-form-grid">
                <label>
                  Approval ID
                  <input value={selectedApprovalId} readOnly />
                </label>
                <label>
                  Reason
                  <input
                    value={app.approvalReason}
                    onChange={(event) => app.setApprovalReason(event.target.value)}
                    placeholder="Optional operator note"
                  />
                </label>
                <label>
                  Decision scope
                  <select
                    value={app.approvalScope}
                    onChange={(event) => app.setApprovalScope(event.target.value)}
                  >
                    <option value="once">once</option>
                    <option value="session">session</option>
                    <option value="timeboxed">timeboxed</option>
                  </select>
                </label>
              </div>

              <div className="console-inline-actions">
                <Button
                  onPress={() => {
                    if (selectedApprovalId.length > 0) {
                      app.setApprovalId(selectedApprovalId);
                    }
                    void app.decideApproval(true);
                  }}
                  isDisabled={app.approvalsBusy}
                >
                  Approve
                </Button>
                <Button
                  variant="danger-soft"
                  onPress={() => {
                    if (selectedApprovalId.length > 0) {
                      app.setApprovalId(selectedApprovalId);
                    }
                    void app.decideApproval(false);
                  }}
                  isDisabled={app.approvalsBusy}
                >
                  Deny
                </Button>
              </div>

              {policySnapshot !== null && (
                <WorkspaceSectionCard
                  title="Policy snapshot"
                  description="Redacted policy context stays close to the decision controls."
                  className="workspace-section-card--nested"
                >
                  <PrettyJsonBlock
                    value={policySnapshot}
                    revealSensitiveValues={app.revealSensitiveValues}
                  />
                </WorkspaceSectionCard>
              )}
            </div>
          )}
        </WorkspaceSectionCard>
      </section>
    </main>
  );
}

function readUnixMillis(record: JsonObject, key: string): number | null {
  const value = record[key];
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}
