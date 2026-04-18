import { useEffect, useState } from "react";

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
import {
  PrettyJsonBlock,
  formatUnixMs,
  readBool,
  readNumber,
  readObject,
  readString,
  readStringList,
  skillMetadata,
  toJsonObjectArray,
  type JsonObject,
} from "../shared";
import type { ConsoleAppState } from "../useConsoleAppState";

type SkillAction = "verify" | "audit" | "quarantine" | "enable";

type SkillsSectionProps = {
  app: Pick<
    ConsoleAppState,
    | "skillsBusy"
    | "skillsEntries"
    | "pluginEntries"
    | "selectedPluginId"
    | "selectedPluginDetail"
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
    | "selectPlugin"
    | "checkPlugin"
    | "savePluginConfig"
    | "clearPluginConfig"
    | "togglePluginEnabled"
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
  const [pluginConfigDraft, setPluginConfigDraft] = useState("");

  useEffect(() => {
    if (app.selectedPluginDetail === null) {
      setPluginConfigDraft("");
      return;
    }
    setPluginConfigDraft(editablePluginConfig(app.selectedPluginDetail));
  }, [app.selectedPluginDetail]);

  const healthyPluginCount = app.pluginEntries.filter((entry) => pluginReady(entry)).length;
  const unhealthyPluginCount = app.pluginEntries.length - healthyPluginCount;
  const quarantinedCount = app.skillsEntries.filter(
    (entry) => readString(entry, "status") === "quarantined",
  ).length;
  const promotableProcedureCount = app.skillProcedureCandidates.filter(
    (candidate) => !["rejected", "suppressed"].includes(readString(candidate, "status") ?? ""),
  ).length;
  const builderCandidateCount = app.skillBuilderCandidates.length;

  const selectedPlugin = app.selectedPluginDetail;
  const selectedPluginBinding = selectedPlugin === null ? null : pluginBinding(selectedPlugin);
  const selectedPluginId =
    selectedPluginBinding === null ? null : readString(selectedPluginBinding, "plugin_id");
  const pluginEnabled = selectedPluginBinding?.["enabled"] === true;
  const selectedPluginCheck = selectedPlugin === null ? null : pluginCheck(selectedPlugin);
  const selectedPluginConfig = selectedPlugin === null ? null : pluginConfig(selectedPlugin);
  const selectedPluginValidation =
    selectedPlugin === null ? null : pluginValidation(selectedPlugin);
  const selectedPluginReasons =
    selectedPluginCheck === null ? [] : readStringList(selectedPluginCheck, "reasons");
  const selectedPluginRemediation =
    selectedPluginCheck === null ? [] : readStringList(selectedPluginCheck, "remediation");
  const selectedPluginCapabilityEntries =
    selectedPluginCheck === null ? [] : pluginCapabilityEntries(selectedPluginCheck);
  const selectedPluginFilesystemIssues =
    selectedPlugin === null ? [] : pluginFilesystemIssues(selectedPlugin);
  const selectedPluginInstalledSkill =
    selectedPlugin === null ? null : readObject(selectedPlugin, "installed_skill");
  const selectedPluginRedactedFields =
    selectedPluginValidation === null
      ? []
      : readStringList(selectedPluginValidation, "redacted_fields");

  return (
    <main className="workspace-page">
      <WorkspacePageHeader
        eyebrow="Agent"
        title="Skills & plugins"
        description="Operate the signed skill inventory and the bound plugin surface from one page, with explicit trust, config, grant, and runtime states instead of a single misleading green light."
        status={
          <>
            <WorkspaceStatusChip tone={app.skillsEntries.length > 0 ? "success" : "default"}>
              {app.skillsEntries.length} skills
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={app.pluginEntries.length > 0 ? "accent" : "default"}>
              {app.pluginEntries.length} plugins
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={healthyPluginCount > 0 ? "success" : "default"}>
              {healthyPluginCount} plugin ready
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={unhealthyPluginCount > 0 ? "danger" : "default"}>
              {unhealthyPluginCount} plugin blocked
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
            {app.skillsBusy ? "Refreshing..." : "Refresh inventory"}
          </ActionButton>
        }
      />

      <section className="workspace-metric-grid workspace-metric-grid--compact">
        <WorkspaceMetricCard
          label="Installed skills"
          value={app.skillsEntries.length}
          detail="Signed skill artifacts that are present in the runtime inventory."
          tone={app.skillsEntries.length > 0 ? "success" : "default"}
        />
        <WorkspaceMetricCard
          label="Bound plugins"
          value={app.pluginEntries.length}
          detail="Manifest-first bindings currently visible to the operator surface."
          tone={app.pluginEntries.length > 0 ? "accent" : "default"}
        />
        <WorkspaceMetricCard
          label="Plugin ready"
          value={healthyPluginCount}
          detail="Bindings whose artifact, config, grants, and runtime checks currently pass."
          tone={healthyPluginCount > 0 ? "success" : "default"}
        />
        <WorkspaceMetricCard
          label="Plugin blocked"
          value={unhealthyPluginCount}
          detail="Any trust, config, grant, filesystem, or runtime issue keeps the plugin here."
          tone={unhealthyPluginCount > 0 ? "danger" : "default"}
        />
        <WorkspaceMetricCard
          label="Quarantined skills"
          value={quarantinedCount}
          detail="Quarantined skills stay hard-stopped until an operator explicitly re-enables them."
          tone={quarantinedCount > 0 ? "danger" : "default"}
        />
        <WorkspaceMetricCard
          label="Builder queue"
          value={builderCandidateCount}
          detail="Prompt and procedure builder outputs remain separate from installed skills until review."
          tone={builderCandidateCount > 0 ? "warning" : "default"}
        />
      </section>

      <section className="workspace-aside-grid">
        <div className="workspace-stack">
          <WorkspaceSectionCard
            title="Plugin inventory"
            description="Keep the list compact, then drive deeper diagnosis from the selected detail panel."
          >
            {app.pluginEntries.length === 0 ? (
              <WorkspaceEmptyState
                title="No plugins bound"
                description="Bind a plugin to turn discovery, config validation, and capability drift into an operator workflow."
              />
            ) : (
              <WorkspaceTable
                ariaLabel="Plugin inventory"
                columns={["Plugin", "Artifact", "Binding", "Config", "Runtime", "Actions"]}
              >
                {app.pluginEntries.map((entry, index) => {
                  const binding = pluginBinding(entry);
                  const pluginId = readString(binding, "plugin_id") ?? `plugin-${index + 1}`;
                  const skillId = readString(binding, "skill_id") ?? "unknown";
                  const version = readString(binding, "skill_version") ?? "current";
                  const artifactState = pluginArtifactState(entry);
                  const bindingState = pluginBindingState(entry);
                  const configState = pluginConfigState(entry);
                  const runtimeState = pluginReady(entry) ? "ready" : "blocked";
                  const selected = pluginId === app.selectedPluginId;

                  return (
                    <tr key={pluginId}>
                      <td>
                        <div className="workspace-table__meta">
                          <strong>{pluginId}</strong>
                          <span className="chat-muted">
                            {skillId} · {version}
                          </span>
                        </div>
                      </td>
                      <td>
                        <WorkspaceStatusChip tone={toneForPluginState(artifactState)}>
                          {humanizeState(artifactState)}
                        </WorkspaceStatusChip>
                      </td>
                      <td>
                        <WorkspaceStatusChip tone={toneForPluginState(bindingState)}>
                          {humanizeState(bindingState)}
                        </WorkspaceStatusChip>
                      </td>
                      <td>
                        <WorkspaceStatusChip tone={toneForPluginState(configState)}>
                          {humanizeState(configState)}
                        </WorkspaceStatusChip>
                      </td>
                      <td>
                        <WorkspaceStatusChip tone={toneForPluginState(runtimeState)}>
                          {humanizeState(runtimeState)}
                        </WorkspaceStatusChip>
                      </td>
                      <td>
                        <div className="workspace-table__actions">
                          <ActionButton
                            type="button"
                            variant={selected ? "primary" : "secondary"}
                            onPress={() => void app.selectPlugin(pluginId)}
                            isDisabled={app.skillsBusy}
                          >
                            {selected ? "Selected" : "Inspect"}
                          </ActionButton>
                          <ActionButton
                            type="button"
                            variant="secondary"
                            onPress={() => void app.checkPlugin(pluginId)}
                            isDisabled={app.skillsBusy}
                          >
                            Check
                          </ActionButton>
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </WorkspaceTable>
            )}
          </WorkspaceSectionCard>

          <WorkspaceSectionCard
            title="Installed skills"
            description="Skill packaging and trust lifecycle still matter, but raw plugin operability now gets its own primary surface."
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
            title="Plugin health detail"
            description="Keep artifact trust, binding validity, config validity, and runtime runnable state visible as separate operator decisions."
          >
            {selectedPlugin === null || selectedPluginBinding === null || selectedPluginCheck === null ? (
              <WorkspaceEmptyState
                title="Select a plugin"
                description="Choose a plugin from the inventory to inspect discovery, config contract, capability drift, and remediation guidance."
              />
            ) : (
              <div className="workspace-stack">
                <section className="workspace-metric-grid workspace-metric-grid--compact">
                  <WorkspaceMetricCard
                    label="Artifact"
                    value={humanizeState(pluginArtifactState(selectedPlugin))}
                    detail={
                      readString(selectedPluginInstalledSkill ?? {}, "trust_decision") ??
                      "No installed artifact metadata"
                    }
                    tone={toneForPluginState(pluginArtifactState(selectedPlugin))}
                  />
                  <WorkspaceMetricCard
                    label="Binding"
                    value={humanizeState(pluginBindingState(selectedPlugin))}
                    detail={
                      pluginEnabled
                        ? "Binding is enabled."
                        : "Binding is disabled and cannot run until re-enabled."
                    }
                    tone={toneForPluginState(pluginBindingState(selectedPlugin))}
                  />
                  <WorkspaceMetricCard
                    label="Config"
                    value={humanizeState(pluginConfigState(selectedPlugin))}
                    detail={
                      readString(selectedPluginConfig ?? {}, "path") ??
                      "Manifest does not declare config."
                    }
                    tone={toneForPluginState(pluginConfigState(selectedPlugin))}
                  />
                  <WorkspaceMetricCard
                    label="Runtime"
                    value={pluginReady(selectedPlugin) ? "ready" : "blocked"}
                    detail={
                      pluginReady(selectedPlugin)
                        ? "All current checks pass."
                        : "One or more trust, config, grant, or filesystem checks still block runtime."
                    }
                    tone={toneForPluginState(pluginReady(selectedPlugin) ? "ready" : "blocked")}
                  />
                </section>

                <div className="workspace-table__meta">
                  <strong>{selectedPluginId}</strong>
                  <span className="chat-muted">
                    {readString(selectedPluginBinding, "skill_id") ?? "unknown skill"} ·{" "}
                    {readString(selectedPluginBinding, "skill_version") ?? "current version"} ·{" "}
                    {readString(selectedPluginBinding, "module_path") ?? "auto module"} ·{" "}
                    {readString(selectedPluginBinding, "entrypoint") ?? "run"}
                  </span>
                </div>

                {selectedPluginReasons.length > 0 && (
                  <WorkspaceInlineNotice title="Why it is blocked" tone="danger">
                    <ul>
                      {selectedPluginReasons.map((reason) => (
                        <li key={reason}>{reason}</li>
                      ))}
                    </ul>
                  </WorkspaceInlineNotice>
                )}

                {selectedPluginRemediation.length > 0 && (
                  <WorkspaceInlineNotice title="Remediation" tone="warning">
                    <ul>
                      {selectedPluginRemediation.map((step) => (
                        <li key={step}>{step}</li>
                      ))}
                    </ul>
                  </WorkspaceInlineNotice>
                )}

                <div className="workspace-inline">
                  <ActionButton
                    type="button"
                    variant="secondary"
                    onPress={() => void app.checkPlugin(selectedPluginId ?? undefined)}
                    isDisabled={app.skillsBusy}
                  >
                    Check now
                  </ActionButton>
                  <ActionButton
                    type="button"
                    variant={pluginEnabled ? "danger-soft" : "primary"}
                    onPress={() =>
                      selectedPluginId !== null &&
                      void app.togglePluginEnabled(selectedPluginId, !pluginEnabled)
                    }
                    isDisabled={app.skillsBusy || selectedPluginId === null}
                  >
                    {pluginEnabled ? "Disable plugin" : "Enable plugin"}
                  </ActionButton>
                </div>

                <WorkspaceSectionCard
                  title="Config remediation"
                  description="Paste the full config object when editing. Redacted fields are intentionally not round-trippable."
                >
                  <div className="workspace-stack">
                    <PrettyJsonBlock
                      value={{
                        path: readString(selectedPluginConfig ?? {}, "path"),
                        validation: selectedPluginValidation ?? {},
                        configured: readObject(selectedPluginConfig ?? {}, "configured"),
                        effective: readObject(selectedPluginConfig ?? {}, "effective"),
                      }}
                      revealSensitiveValues={app.revealSensitiveValues}
                      className="workspace-code-panel"
                    />
                    {selectedPluginRedactedFields.length > 0 && (
                      <WorkspaceInlineNotice title="Redacted fields" tone="warning">
                        <p>
                          Hidden values are present for: {selectedPluginRedactedFields.join(", ")}.
                          Paste the full object again when updating config so those values are not
                          dropped.
                        </p>
                      </WorkspaceInlineNotice>
                    )}
                    <AppForm
                      className="workspace-stack"
                      onSubmit={(event) => {
                        event.preventDefault();
                        if (selectedPluginId === null) {
                          return;
                        }
                        void app.savePluginConfig(selectedPluginId, pluginConfigDraft);
                      }}
                    >
                      <TextAreaField
                        label="Config JSON"
                        rows={8}
                        value={pluginConfigDraft}
                        onChange={setPluginConfigDraft}
                        placeholder='{"api_base_url":"https://api.example.com","api_token":"secret-token"}'
                      />
                      <div className="workspace-inline">
                        <ActionButton
                          type="submit"
                          variant="primary"
                          isDisabled={app.skillsBusy || selectedPluginId === null}
                        >
                          Save config
                        </ActionButton>
                        <ActionButton
                          type="button"
                          variant="secondary"
                          onPress={() =>
                            setPluginConfigDraft(
                              selectedPlugin === null ? "" : editablePluginConfig(selectedPlugin),
                            )
                          }
                          isDisabled={app.skillsBusy}
                        >
                          Reset editor
                        </ActionButton>
                        <ActionButton
                          type="button"
                          variant="danger"
                          onPress={() =>
                            selectedPluginId !== null && void app.clearPluginConfig(selectedPluginId)
                          }
                          isDisabled={app.skillsBusy || selectedPluginId === null}
                        >
                          Clear config
                        </ActionButton>
                      </div>
                    </AppForm>
                  </div>
                </WorkspaceSectionCard>

                {selectedPluginCapabilityEntries.length > 0 && (
                  <WorkspaceSectionCard
                    title="Capability diff"
                    description="Diagnose grant drift and policy restriction before touching the binding."
                  >
                    <WorkspaceTable
                      ariaLabel="Plugin capability diff"
                      columns={["Category", "Capability", "Value", "Detail"]}
                    >
                      {selectedPluginCapabilityEntries.map((entry, index) => {
                        const category = readString(entry, "category") ?? `category-${index + 1}`;
                        const capabilityKind = readString(entry, "capability_kind") ?? "unknown";
                        const value = readString(entry, "value") ?? "n/a";
                        const message = readString(entry, "message") ?? "No detail";
                        return (
                          <tr key={`${category}-${capabilityKind}-${value}-${index}`}>
                            <td>
                              <WorkspaceStatusChip tone={toneForPluginState(category)}>
                                {humanizeState(category)}
                              </WorkspaceStatusChip>
                            </td>
                            <td>{capabilityKind}</td>
                            <td>{value}</td>
                            <td>{message}</td>
                          </tr>
                        );
                      })}
                    </WorkspaceTable>
                  </WorkspaceSectionCard>
                )}

                {selectedPluginFilesystemIssues.length > 0 && (
                  <WorkspaceSectionCard
                    title="Filesystem safety"
                    description="Path safety remains fail-closed and should be treated as an operator-visible blocker."
                  >
                    <WorkspaceTable
                      ariaLabel="Plugin filesystem issues"
                      columns={["Code", "Severity", "Message", "Remediation"]}
                    >
                      {selectedPluginFilesystemIssues.map((issue, index) => {
                        const code = readString(issue, "code") ?? `issue-${index + 1}`;
                        return (
                          <tr key={code}>
                            <td>{code}</td>
                            <td>
                              <WorkspaceStatusChip
                                tone={toneForPluginState(readString(issue, "severity") ?? "unknown")}
                              >
                                {readString(issue, "severity") ?? "unknown"}
                              </WorkspaceStatusChip>
                            </td>
                            <td>{readString(issue, "message") ?? "No detail"}</td>
                            <td>{readString(issue, "remediation") ?? "No remediation"}</td>
                          </tr>
                        );
                      })}
                    </WorkspaceTable>
                  </WorkspaceSectionCard>
                )}

                <WorkspaceSectionCard
                  title="Installed artifact"
                  description="Artifact provenance stays visible, but it is not allowed to hide binding or config problems."
                >
                  <PrettyJsonBlock
                    value={{
                      trust_decision: readString(
                        selectedPluginInstalledSkill ?? {},
                        "trust_decision",
                      ),
                      source: readObject(selectedPluginInstalledSkill ?? {}, "source"),
                      payload_sha256: readString(
                        selectedPluginInstalledSkill ?? {},
                        "payload_sha256",
                      ),
                      signature_key_id: readString(
                        selectedPluginInstalledSkill ?? {},
                        "signature_key_id",
                      ),
                      installed_at: formatUnixMs(
                        readNumber(selectedPluginInstalledSkill ?? {}, "installed_at_unix_ms"),
                      ),
                    }}
                    revealSensitiveValues={app.revealSensitiveValues}
                    className="workspace-code-panel"
                  />
                </WorkspaceSectionCard>

                <WorkspaceSectionCard
                  title="Selected raw detail"
                  description="Raw payload stays as a supporting diagnostic surface, not the main workflow."
                >
                  <PrettyJsonBlock
                    value={selectedPlugin}
                    revealSensitiveValues={app.revealSensitiveValues}
                    className="workspace-code-panel"
                  />
                </WorkspaceSectionCard>
              </div>
            )}
          </WorkspaceSectionCard>

          <WorkspaceSectionCard
            title="Install artifact"
            description="Install stays compact and operational. This page still avoids turning into an app store."
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
            title="Procedure promotion"
            description="Procedure promotion feeds the same experimental builder loop so procedure-derived outputs and prompt-derived outputs share one quarantine and review model."
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
              <PrettyJsonBlock
                value={app.lastSkillPromotion}
                revealSensitiveValues={app.revealSensitiveValues}
                className="workspace-code-panel"
              />
            )}
          </WorkspaceSectionCard>

          <WorkspaceInlineNotice title="Trust posture" tone="warning">
            <p>
              Plugin health is only green when artifact trust, binding resolution, config contract,
              grant posture, and filesystem safety all agree. This page keeps those states separate
              on purpose.
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

function pluginBinding(entry: JsonObject): JsonObject {
  return readObject(entry, "binding") ?? {};
}

function pluginCheck(entry: JsonObject): JsonObject {
  return readObject(entry, "check") ?? {};
}

function pluginDiscovery(entry: JsonObject): JsonObject {
  return readObject(pluginCheck(entry), "discovery") ?? {};
}

function pluginConfig(entry: JsonObject): JsonObject {
  return readObject(pluginCheck(entry), "config") ?? {};
}

function pluginValidation(entry: JsonObject): JsonObject {
  return readObject(pluginConfig(entry), "validation") ?? {};
}

function pluginReady(entry: JsonObject): boolean {
  return readBool(pluginCheck(entry), "ready");
}

function pluginArtifactState(entry: JsonObject): string {
  const installedSkill = readObject(entry, "installed_skill");
  const trustDecision = readString(installedSkill ?? {}, "trust_decision");
  if (trustDecision === "untrusted_override") {
    return trustDecision;
  }
  if (readString(pluginDiscovery(entry), "state") === "signature_failed") {
    return "signature_failed";
  }
  if (readObject(pluginCheck(entry), "resolved") !== null) {
    return "installed";
  }
  return readString(pluginDiscovery(entry), "state") ?? "unknown";
}

function pluginBindingState(entry: JsonObject): string {
  const binding = pluginBinding(entry);
  if (binding["enabled"] !== true) {
    return "disabled";
  }
  const discoveryState = readString(pluginDiscovery(entry), "state");
  if (discoveryState === "missing_module" || discoveryState === "filesystem_unsafe") {
    return discoveryState;
  }
  if (readObject(pluginCheck(entry), "resolved") !== null) {
    return "resolved";
  }
  return discoveryState ?? "unknown";
}

function pluginConfigState(entry: JsonObject): string {
  return readString(pluginValidation(entry), "state") ?? "unknown";
}

function pluginCapabilityEntries(check: JsonObject): JsonObject[] {
  const capabilityPayload = readObject(check, "capabilities");
  const entries = capabilityPayload?.["entries"];
  return Array.isArray(entries) ? toJsonObjectArray(entries) : [];
}

function pluginFilesystemIssues(detail: JsonObject): JsonObject[] {
  const discovery = pluginDiscovery(detail);
  const filesystem = readObject(discovery, "filesystem");
  const issues = filesystem?.["issues"];
  return Array.isArray(issues) ? toJsonObjectArray(issues) : [];
}

function editablePluginConfig(detail: JsonObject): string {
  const validation = pluginValidation(detail);
  if (readStringList(validation, "redacted_fields").length > 0) {
    return "";
  }
  const configured = readObject(pluginConfig(detail), "configured");
  return configured === null ? "" : JSON.stringify(configured, null, 2);
}

function humanizeState(value: string | null): string {
  return (value ?? "unknown").replace(/_/g, " ");
}

function toneForPluginState(state: string): "default" | "success" | "warning" | "danger" | "accent" {
  switch (state) {
    case "installed":
    case "resolved":
    case "valid":
    case "ready":
    case "tofu_pinned":
      return "success";
    case "requires_migration":
    case "missing":
    case "review":
      return "warning";
    case "blocked":
    case "disabled":
    case "invalid":
    case "signature_failed":
    case "untrusted_override":
    case "missing_module":
    case "filesystem_unsafe":
    case "missing_grant":
    case "policy_restricted":
    case "excess_grant":
    case "wildcard_risk":
      return "danger";
    default:
      return workspaceToneForState(state) as "default" | "success" | "warning" | "danger" | "accent";
  }
}

function readRecord(entry: JsonObject): JsonObject {
  const record = entry.record;
  return typeof record === "object" && record !== null && !Array.isArray(record)
    ? (record as JsonObject)
    : entry;
}
