import { ConsoleSectionHeader } from "../components/ConsoleSectionHeader";
import { readString } from "../shared";
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
  >;
};

export function ApprovalsSection({ app }: ApprovalsSectionProps) {
  return (
    <main className="console-card">
      <ConsoleSectionHeader
        title="Approvals"
        description="Sensitive action requests remain first-class operator work. Decisions stay explicit, reasoned, and scope-bounded."
        actions={(
          <button type="button" onClick={() => void app.refreshApprovals()} disabled={app.approvalsBusy}>
            {app.approvalsBusy ? "Refreshing..." : "Refresh approvals"}
          </button>
        )}
      />

      <div className="console-table-wrap">
        <table className="console-table">
          <thead>
            <tr>
              <th>Approval ID</th>
              <th>Subject</th>
              <th>Decision</th>
              <th>Requested</th>
              <th>Action</th>
            </tr>
          </thead>
          <tbody>
            {app.approvals.length === 0 && (
              <tr>
                <td colSpan={5}>No approvals found.</td>
              </tr>
            )}
            {app.approvals.map((approval) => {
              const id = readString(approval, "approval_id") ?? "(missing)";
              return (
                <tr key={id}>
                  <td>{id}</td>
                  <td>{readString(approval, "subject_type") ?? "-"}</td>
                  <td>{readString(approval, "decision") ?? "-"}</td>
                  <td>{readString(approval, "requested_at_unix_ms") ?? "-"}</td>
                  <td>
                    <button type="button" className="secondary" onClick={() => app.setApprovalId(id)}>
                      Select
                    </button>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      <section className="console-subpanel">
        <h3>Decision</h3>
        <div className="console-grid-3">
          <label>
            Approval ID
            <input value={app.approvalId} onChange={(event) => app.setApprovalId(event.target.value)} />
          </label>
          <label>
            Reason
            <input value={app.approvalReason} onChange={(event) => app.setApprovalReason(event.target.value)} />
          </label>
          <label>
            Scope
            <select value={app.approvalScope} onChange={(event) => app.setApprovalScope(event.target.value)}>
              <option value="once">once</option>
              <option value="session">session</option>
              <option value="timeboxed">timeboxed</option>
            </select>
          </label>
        </div>
        <div className="console-inline-actions">
          <button type="button" onClick={() => void app.decideApproval(true)}>Approve</button>
          <button type="button" className="button--warn" onClick={() => void app.decideApproval(false)}>Reject</button>
        </div>
      </section>
    </main>
  );
}
