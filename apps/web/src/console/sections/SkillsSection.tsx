import { useState } from "react";

import {
  ActionButton,
  AppForm,
  CheckboxField,
  TextAreaField,
  TextInputField,
} from "../components/ui";
import {
  WorkspaceMetricCard,
  WorkspacePageHeader,
  WorkspaceSectionCard,
  WorkspaceStatusChip,
} from "../components/workspace/WorkspaceChrome";
import {
  WorkspaceConfirmDialog,
  WorkspaceEmptyState,
  WorkspaceInlineNotice,
  WorkspaceTable,
  workspaceToneForState,
} from "../components/workspace/WorkspacePatterns";
import { readNumber, readString, skillMetadata, toPrettyJson, type JsonObject } from "../shared";
import type { ConsoleAppState } from "../useConsoleAppState";

type SkillAction = "verify" | "audit" | "quarantine" | "enable";

type SkillsSectionProps = {
  app: Pick<
    ConsoleAppState,
    | "skillsBusy"
    | "skillsEntries"
    | "skillProcedureCandidates"
    | "skillBuilderCandidates"
    | "lastSkillPromotion"
    | "skillArtifactPath"
    | "setSkillArtifactPath"
    | "skillAllowTofu"
    | "setSkillAllowTofu"
    | "skillAllowUntrusted"
    | "setSkillAllowUntrusted"
    | "skillReason"
    | "setSkillReason"
    | "skillBuilderPrompt"
    | "setSkillBuilderPrompt"
    | "skillBuilderName"
    | "setSkillBuilderName"
    | "refreshSkills"
    | "installSkill"
    | "executeSkillAction"
    | "promoteProcedureCandidate"
    | "createSkillBuilderCandidate"
    | "revealSensitiveValues"
  >;
};

export function SkillsSection({ app }: SkillsSectionProps) {
  const [pendingAction, setPendingAction] = useState<{
    entry: JsonObject;
    action: Extract<SkillAction, "quarantine" | "enable">;
    skillId: string;
  } | null>(null);
  const quarantinedCount = app.skillsEntries.filter(
    (entry) => readString(entry, "status") === "quarantined",
  ).length;
  const promotableProcedureCount = app.skillProcedureCandidates.filter(
    (candidate) => !["rejected", "suppressed"].includes(readString(candidate, "status") ?? ""),
  ).length;
  const builderCandidateCount = app.skillBuilderCandidates.length;

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
            <WorkspaceStatusChip tone={promotableProcedureCount > 0 ? "accent" : "default"}>
              {promotableProcedureCount} procedure candidates
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={builderCandidateCount > 0 ? "warning" : "default"}>
              {builderCandidateCount} builder candidates
            </WorkspaceStatusChip>
          </>
        }
        actions={
          <ActionButton
            type="button"
            variant="primary"
            onPress={() => void app.refreshSkills()}
            isDisabled={app.skillsBusy}
          >
            {app.skillsBusy ? "Refreshing..." : "Refresh skills"}
          </ActionButton>
        }
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
          value={
            app.skillAllowUntrusted
              ? "Untrusted allowed"
              : app.skillAllowTofu
                ? "TOFU allowed"
                : "Strict"
          }
          detail="Keep trust exceptions visible instead of burying them in the install form."
          tone={app.skillAllowUntrusted ? "danger" : app.skillAllowTofu ? "warning" : "success"}
        />
        <WorkspaceMetricCard
          label="Procedure queue"
          value={app.skillProcedureCandidates.length}
          detail="Only reusable procedure candidates can be promoted into quarantined skill scaffolds."
          tone={promotableProcedureCount > 0 ? "accent" : "default"}
        />
        <WorkspaceMetricCard
          label="Builder queue"
          value={builderCandidateCount}
          detail="Experimental builder outputs stay separate from installed skills and remain quarantined by design."
          tone={builderCandidateCount > 0 ? "warning" : "default"}
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
                  const skillId =
                    metadata?.skillId ?? readString(entry, "skill_id") ?? `skill-${index + 1}`;
                  const version = metadata?.version ?? readString(entry, "version") ?? "unknown";
                  const status =
                    readString(entry, "status") ?? readString(entry, "state") ?? "unknown";
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
                          <span className="chat-muted">
                            {readString(readRecord(entry), "description") ??
                              "Installed skill artifact"}
                          </span>
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
                          <ActionButton
                            type="button"
                            variant="secondary"
                            onPress={() => void app.executeSkillAction(entry, "verify")}
                            isDisabled={app.skillsBusy}
                          >
                            Verify
                          </ActionButton>
                          <ActionButton
                            type="button"
                            variant="secondary"
                            onPress={() => void app.executeSkillAction(entry, "audit")}
                            isDisabled={app.skillsBusy}
                          >
                            Audit
                          </ActionButton>
                          <ActionButton
                            type="button"
                            variant="danger"
                            onPress={() =>
                              setPendingAction({ entry, action: "quarantine", skillId })
                            }
                            isDisabled={app.skillsBusy}
                          >
                            Quarantine
                          </ActionButton>
                          <ActionButton
                            type="button"
                            variant="secondary"
                            onPress={() => setPendingAction({ entry, action: "enable", skillId })}
                            isDisabled={app.skillsBusy}
                          >
                            Enable
                          </ActionButton>
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
            <AppForm className="workspace-stack" onSubmit={(event) => void app.installSkill(event)}>
              <TextInputField
                label="Artifact path"
                value={app.skillArtifactPath}
                onChange={app.setSkillArtifactPath}
              />
              <TextInputField
                label="Operator reason"
                value={app.skillReason}
                onChange={app.setSkillReason}
              />
              <div className="workspace-inline">
                <CheckboxField
                  checked={app.skillAllowTofu}
                  label="Allow TOFU"
                  onChange={app.setSkillAllowTofu}
                />
                <CheckboxField
                  checked={app.skillAllowUntrusted}
                  label="Allow untrusted"
                  onChange={app.setSkillAllowUntrusted}
                />
                <ActionButton type="submit" variant="primary" isDisabled={app.skillsBusy}>
                  {app.skillsBusy ? "Installing..." : "Install skill"}
                </ActionButton>
              </div>
            </AppForm>
          </WorkspaceSectionCard>

          <WorkspaceSectionCard
            title="Create builder candidate"
            description="Explicit prompt-based builder output stays experimental, opt-in, and quarantined until packaging, signing, and review are complete."
          >
            <AppForm
              className="workspace-stack"
              onSubmit={(event) => void app.createSkillBuilderCandidate(event)}
            >
              <TextAreaField
                label="Builder prompt"
                rows={4}
                value={app.skillBuilderPrompt}
                onChange={app.setSkillBuilderPrompt}
              />
              <TextInputField
                label="Candidate name"
                value={app.skillBuilderName}
                onChange={app.setSkillBuilderName}
              />
              <TextInputField
                label="Operator reason"
                value={app.skillReason}
                onChange={app.setSkillReason}
              />
              <ActionButton type="submit" variant="primary" isDisabled={app.skillsBusy}>
                {app.skillsBusy ? "Creating..." : "Create builder candidate"}
              </ActionButton>
            </AppForm>
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

          <WorkspaceSectionCard
            title="Procedure promotion"
            description="Phase 6 promotion now feeds the same experimental builder loop so procedure-derived outputs and prompt-derived outputs share one quarantine and review model."
          >
            {app.skillProcedureCandidates.length === 0 ? (
              <WorkspaceEmptyState
                compact
                title="No procedure candidates"
                description="Reflection has not proposed any reusable procedure candidates for skill promotion yet."
              />
            ) : (
              <WorkspaceTable
                ariaLabel="Procedure skill candidates"
                columns={["Candidate", "Confidence", "Status", "Risk", "Actions"]}
              >
                {app.skillProcedureCandidates.map((candidate, index) => {
                  const candidateId =
                    readString(candidate, "candidate_id") ?? `procedure-${index + 1}`;
                  const candidateStatus = readString(candidate, "status") ?? "unknown";
                  return (
                    <tr key={candidateId}>
                      <td>
                        <div className="workspace-table__meta">
                          <strong>{readString(candidate, "title") ?? candidateId}</strong>
                          <span className="chat-muted">
                            {readString(candidate, "summary") ?? "No summary"}
                          </span>
                        </div>
                      </td>
                      <td>
                        {typeof readNumber(candidate, "confidence") === "number"
                          ? readNumber(candidate, "confidence")?.toFixed(2)
                          : "n/a"}
                      </td>
                      <td>{candidateStatus}</td>
                      <td>{readString(candidate, "risk_level") ?? "unknown"}</td>
                      <td>
                        <ActionButton
                          type="button"
                          variant="primary"
                          onPress={() => void app.promoteProcedureCandidate(candidateId)}
                          isDisabled={
                            app.skillsBusy || ["rejected", "suppressed"].includes(candidateStatus)
                          }
                        >
                          Build candidate
                        </ActionButton>
                      </td>
                    </tr>
                  );
                })}
              </WorkspaceTable>
            )}
          </WorkspaceSectionCard>

          <WorkspaceSectionCard
            title="Builder candidates"
            description="Builder outputs stay visibly separate from installed skills so provenance, capability declaration, and test harness files can be reviewed before any packaging or signing step."
          >
            {app.skillBuilderCandidates.length === 0 ? (
              <WorkspaceEmptyState
                compact
                title="No builder candidates"
                description="Use a reusable procedure candidate or an explicit builder prompt to create the first quarantined candidate."
              />
            ) : (
              <WorkspaceTable
                ariaLabel="Builder candidates"
                columns={["Candidate", "Source", "Status", "Review files", "Scaffold root"]}
              >
                {app.skillBuilderCandidates.map((candidate, index) => {
                  const candidateId =
                    readString(candidate, "candidate_id") ?? `builder-${index + 1}`;
                  return (
                    <tr key={candidateId}>
                      <td>
                        <div className="workspace-table__meta">
                          <strong>{readString(candidate, "skill_id") ?? candidateId}</strong>
                          <span className="chat-muted">
                            {readString(candidate, "summary") ?? "No summary"}
                          </span>
                        </div>
                      </td>
                      <td>
                        {readString(candidate, "source_kind") ?? "unknown"} ·{" "}
                        {readString(candidate, "source_ref") ?? "n/a"}
                      </td>
                      <td>
                        <WorkspaceStatusChip
                          tone={workspaceToneForState(readString(candidate, "status") ?? "unknown")}
                        >
                          {readString(candidate, "status") ?? "unknown"}
                        </WorkspaceStatusChip>
                      </td>
                      <td>
                        <div className="workspace-table__meta">
                          <strong>Capability / provenance / test</strong>
                          <span className="chat-muted">
                            {[
                              readString(candidate, "capability_declaration_path"),
                              readString(candidate, "provenance_path"),
                              readString(candidate, "test_harness_path"),
                            ]
                              .filter((value): value is string => value !== null)
                              .join(" · ")}
                          </span>
                        </div>
                      </td>
                      <td>{readString(candidate, "scaffold_root") ?? "n/a"}</td>
                    </tr>
                  );
                })}
              </WorkspaceTable>
            )}
          </WorkspaceSectionCard>

          <WorkspaceSectionCard
            title="Last promotion"
            description="Scaffold output stays explicit so operators can inspect the generated path and quarantine posture before packaging or signing anything."
          >
            {app.lastSkillPromotion === null ? (
              <WorkspaceEmptyState
                compact
                title="No promotion result"
                description="Promote a procedure candidate to inspect the generated scaffold metadata here."
              />
            ) : (
              <pre className="workspace-code-panel">
                {toPrettyJson(app.lastSkillPromotion, app.revealSensitiveValues)}
              </pre>
            )}
          </WorkspaceSectionCard>

          <WorkspaceInlineNotice title="Trust posture" tone="warning">
            <p>
              Quarantine and enable are operational safety actions, not casual toggles. The page
              keeps those mutations explicit and records an operator reason alongside the workflow.
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
  return typeof record === "object" && record !== null && !Array.isArray(record)
    ? (record as JsonObject)
    : entry;
}
