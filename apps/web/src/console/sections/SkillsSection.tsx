import { useState } from "react";

import { WorkspaceMetricCard, WorkspacePageHeader, WorkspaceSectionCard, WorkspaceStatusChip } from "../components/workspace/WorkspaceChrome";
import { WorkspaceConfirmDialog, WorkspaceEmptyState, WorkspaceInlineNotice, WorkspaceTable, workspaceToneForState } from "../components/workspace/WorkspacePatterns";
import { readString, skillMetadata, toPrettyJson, type JsonObject } from "../shared";
import type { ConsoleAppState } from "../useConsoleAppState";

type SkillAction = "verify" | "audit" | "quarantine" | "enable";

type SkillsSectionProps = {
  app: Pick<
    ConsoleAppState,
    | "skillsBusy"
    | "skillsEntries"
    | "skillArtifactPath"
    | "setSkillArtifactPath"
    | "skillAllowTofu"
    | "setSkillAllowTofu"
    | "skillAllowUntrusted"
    | "setSkillAllowUntrusted"
    | "skillReason"
    | "setSkillReason"
    | "refreshSkills"
    | "installSkill"
    | "executeSkillAction"
    | "revealSensitiveValues"
  >;
};

export function SkillsSection({ app }: SkillsSectionProps) {
  const [pendingAction, setPendingAction] = useState<{
    entry: JsonObject;
    action: Extract<SkillAction, "quarantine" | "enable">;
    skillId: string;
  } | null>(null);
  const quarantinedCount = app.skillsEntries.filter((entry) => readString(entry, "status") === "quarantined").length;

  return (
    <main className="workspace-page">
      <WorkspacePageHeader
        eyebrow="Agent"
        title="Skills"
        description="Track installed skills, keep trust posture readable, and force risky state changes like quarantine or re-enable through one consistent confirmation flow."
        status={
          <>
            <WorkspaceStatusChip tone={app.skillsEntries.length > 0 ? "success" : "default"}>
              {app.skillsEntries.length} installed
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={quarantinedCount > 0 ? "danger" : "default"}>
              {quarantinedCount} quarantined
            </WorkspaceStatusChip>
          </>
        }
        actions={(
          <button type="button" onClick={() => void app.refreshSkills()} disabled={app.skillsBusy}>
            {app.skillsBusy ? "Refreshing..." : "Refresh skills"}
          </button>
        )}
      />

      <section className="workspace-metric-grid workspace-metric-grid--compact">
        <WorkspaceMetricCard
          label="Installed"
          value={app.skillsEntries.length}
          detail="Verified or not, only actually installed skills are shown here."
          tone={app.skillsEntries.length > 0 ? "success" : "default"}
        />
        <WorkspaceMetricCard
          label="Quarantined"
          value={quarantinedCount}
          detail="Quarantined skills are hard-stop runtime entries until re-enabled."
          tone={quarantinedCount > 0 ? "danger" : "default"}
        />
        <WorkspaceMetricCard
          label="Install posture"
          value={app.skillAllowUntrusted ? "Untrusted allowed" : app.skillAllowTofu ? "TOFU allowed" : "Strict"}
          detail="Keep trust exceptions visible instead of burying them in the install form."
          tone={app.skillAllowUntrusted ? "danger" : app.skillAllowTofu ? "warning" : "success"}
        />
      </section>

      <section className="workspace-aside-grid">
        <div className="workspace-stack">
          <WorkspaceSectionCard
            title="Installed skills"
            description="The table is primary so operator state stays readable. Raw JSON remains available only as supporting detail."
          >
            {app.skillsEntries.length === 0 ? (
              <WorkspaceEmptyState
                title="No skills installed"
                description="Install a signed artifact to see trust posture, lifecycle actions, and runtime status here."
              />
            ) : (
              <WorkspaceTable
                ariaLabel="Installed skills"
                columns={["Skill", "Status", "Version", "Publisher state", "Actions"]}
              >
                {app.skillsEntries.map((entry, index) => {
                  const metadata = skillMetadata(entry);
                  const skillId = metadata?.skillId ?? readString(entry, "skill_id") ?? `skill-${index + 1}`;
                  const version = metadata?.version ?? readString(entry, "version") ?? "unknown";
                  const status = readString(entry, "status") ?? readString(entry, "state") ?? "unknown";
                  const publisherState =
                    readString(entry, "trust_state") ??
                    readString(entry, "publisher") ??
                    readString(readRecord(entry), "publisher") ??
                    "No publisher detail";

                  return (
                    <tr key={`${skillId}-${version}`}>
                      <td>
                        <div className="workspace-table__meta">
                          <strong>{skillId}</strong>
                          <span className="chat-muted">{readString(readRecord(entry), "description") ?? "Installed skill artifact"}</span>
                        </div>
                      </td>
                      <td>
                        <div className="workspace-table__status">
                          <WorkspaceStatusChip tone={workspaceToneForState(status)}>
                            {status}
                          </WorkspaceStatusChip>
                        </div>
                      </td>
                      <td>{version}</td>
                      <td>{publisherState}</td>
                      <td>
                        <div className="workspace-table__actions">
                          <button type="button" className="secondary" onClick={() => void app.executeSkillAction(entry, "verify")} disabled={app.skillsBusy}>
                            Verify
                          </button>
                          <button type="button" className="secondary" onClick={() => void app.executeSkillAction(entry, "audit")} disabled={app.skillsBusy}>
                            Audit
                          </button>
                          <button
                            type="button"
                            className="button--warn"
                            onClick={() => setPendingAction({ entry, action: "quarantine", skillId })}
                            disabled={app.skillsBusy}
                          >
                            Quarantine
                          </button>
                          <button
                            type="button"
                            className="secondary"
                            onClick={() => setPendingAction({ entry, action: "enable", skillId })}
                            disabled={app.skillsBusy}
                          >
                            Enable
                          </button>
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </WorkspaceTable>
            )}
          </WorkspaceSectionCard>
        </div>

        <div className="workspace-stack">
          <WorkspaceSectionCard
            title="Install artifact"
            description="Install stays compact and operational. This page should not feel like an app store."
          >
            <form className="workspace-stack" onSubmit={(event) => void app.installSkill(event)}>
              <label>
                Artifact path
                <input
                  value={app.skillArtifactPath}
                  onChange={(event) => app.setSkillArtifactPath(event.target.value)}
                />
              </label>
              <label>
                Operator reason
                <input
                  value={app.skillReason}
                  onChange={(event) => app.setSkillReason(event.target.value)}
                />
              </label>
              <div className="workspace-inline">
                <label className="console-checkbox-inline">
                  <input
                    type="checkbox"
                    checked={app.skillAllowTofu}
                    onChange={(event) => app.setSkillAllowTofu(event.target.checked)}
                  />
                  Allow TOFU
                </label>
                <label className="console-checkbox-inline">
                  <input
                    type="checkbox"
                    checked={app.skillAllowUntrusted}
                    onChange={(event) => app.setSkillAllowUntrusted(event.target.checked)}
                  />
                  Allow untrusted
                </label>
                <button type="submit" disabled={app.skillsBusy}>
                  {app.skillsBusy ? "Installing..." : "Install skill"}
                </button>
              </div>
            </form>
          </WorkspaceSectionCard>

          <WorkspaceSectionCard
            title="Selected raw detail"
            description="Raw entry data is still available for operator inspection, but it no longer dominates the page."
          >
            {app.skillsEntries.length === 0 ? (
              <WorkspaceEmptyState
                title="No detail to inspect"
                description="Once a skill is installed, its record stays available here as a supporting diagnostic surface."
                compact
              />
            ) : (
              <pre className="workspace-code-panel">
                {toPrettyJson(app.skillsEntries[0], app.revealSensitiveValues)}
              </pre>
            )}
          </WorkspaceSectionCard>

          <WorkspaceInlineNotice title="Trust posture" tone="warning">
            <p>
              Quarantine and enable are operational safety actions, not casual toggles. The page keeps
              those mutations explicit and records an operator reason alongside the workflow.
            </p>
          </WorkspaceInlineNotice>
        </div>
      </section>

      <WorkspaceConfirmDialog
        isOpen={pendingAction !== null}
        onOpenChange={(isOpen) => {
          if (!isOpen) {
            setPendingAction(null);
          }
        }}
        title={pendingAction?.action === "enable" ? "Enable skill" : "Quarantine skill"}
        description={
          pendingAction === null
            ? ""
            : pendingAction.action === "enable"
              ? `Re-enable ${pendingAction.skillId} so it can run again.`
              : `Quarantine ${pendingAction.skillId} and force a runtime stop until an operator re-enables it.`
        }
        confirmLabel={pendingAction?.action === "enable" ? "Enable skill" : "Quarantine skill"}
        confirmTone={pendingAction?.action === "enable" ? "warning" : "danger"}
        isBusy={app.skillsBusy}
        onConfirm={() => {
          if (pendingAction === null) {
            return;
          }
          const action = pendingAction.action;
          const entry = pendingAction.entry;
          setPendingAction(null);
          void app.executeSkillAction(entry, action);
        }}
      />
    </main>
  );
}

function readRecord(entry: JsonObject): JsonObject {
  const record = entry.record;
  return typeof record === "object" && record !== null && !Array.isArray(record) ? record as JsonObject : entry;
}
