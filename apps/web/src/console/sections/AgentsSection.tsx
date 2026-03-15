import { Modal } from "@heroui/react";
import { useEffect, useMemo, useState } from "react";

import type {
  AgentCreateRequest,
  AgentEnvelope,
  AgentListEnvelope,
  AgentRecord
} from "../../consoleApi";
import {
  ActionButton,
  ActionCluster,
  CheckboxField,
  EmptyState,
  EntityTable,
  InlineNotice,
  KeyValueList,
  TextAreaField,
  TextInputField
} from "../components/ui";
import {
  WorkspaceMetricCard,
  WorkspacePageHeader,
  WorkspaceSectionCard,
  WorkspaceStatusChip
} from "../components/workspace/WorkspaceChrome";
import type { ConsoleAppState } from "../useConsoleAppState";

type AgentsSectionProps = {
  app: Pick<ConsoleAppState, "api" | "setError" | "setNotice">;
};

type WizardStep = 0 | 1 | 2 | 3;

type AgentDraft = {
  agentId: string;
  displayName: string;
  agentDir: string;
  workspaceRoots: string;
  defaultModelProfile: string;
  defaultToolAllowlist: string;
  defaultSkillAllowlist: string;
  setDefault: boolean;
  allowAbsolutePaths: boolean;
};

type AgentRow = AgentRecord & {
  isDefault: boolean;
  isSelected: boolean;
};

const WIZARD_STEPS: ReadonlyArray<{
  id: WizardStep;
  label: string;
  description: string;
}> = [
  { id: 0, label: "Identity", description: "Choose a stable id and an operator-friendly display name." },
  { id: 1, label: "Storage", description: "Keep workspace paths local unless you explicitly opt into absolute paths." },
  { id: 2, label: "Defaults", description: "Set the default model and any explicit tool or skill allowlists." },
  { id: 3, label: "Review", description: "Confirm the new registry entry before submitting it." }
];

function createDefaultDraft(): AgentDraft {
  return {
    agentId: "",
    displayName: "",
    agentDir: "",
    workspaceRoots: "workspace",
    defaultModelProfile: "gpt-4o-mini",
    defaultToolAllowlist: "",
    defaultSkillAllowlist: "",
    setDefault: false,
    allowAbsolutePaths: false
  };
}

function parseTextList(value: string): string[] {
  const entries = value
    .split(/[\r\n,]+/)
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0);
  return Array.from(new Set(entries));
}

function resolveWorkspaceRoots(draft: AgentDraft): string[] {
  const roots = parseTextList(draft.workspaceRoots);
  return roots.length > 0 ? roots : ["workspace"];
}

function validationMessageForStep(step: WizardStep, draft: AgentDraft): string | null {
  if (step === 0) {
    if (!/^[a-z0-9][a-z0-9-]*$/.test(draft.agentId.trim())) {
      return "Agent ID must use lowercase letters, numbers, and hyphens only.";
    }
    if (draft.displayName.trim().length === 0) {
      return "Display name is required.";
    }
  }

  if (step === 1 && resolveWorkspaceRoots(draft).length === 0) {
    return "At least one workspace root is required.";
  }

  return null;
}

function buildCreatePayload(draft: AgentDraft): AgentCreateRequest {
  const agentDir = draft.agentDir.trim();
  const defaultModelProfile = draft.defaultModelProfile.trim();

  return {
    agent_id: draft.agentId.trim(),
    display_name: draft.displayName.trim(),
    agent_dir: agentDir.length > 0 ? agentDir : undefined,
    workspace_roots: resolveWorkspaceRoots(draft),
    default_model_profile: defaultModelProfile.length > 0 ? defaultModelProfile : undefined,
    default_tool_allowlist: parseTextList(draft.defaultToolAllowlist),
    default_skill_allowlist: parseTextList(draft.defaultSkillAllowlist),
    set_default: draft.setDefault,
    allow_absolute_paths: draft.allowAbsolutePaths
  };
}

function formatUnixMs(value: number): string {
  return new Intl.DateTimeFormat("sv-SE", {
    dateStyle: "short",
    timeStyle: "short",
    timeZone: "UTC"
  })
    .format(new Date(value))
    .replace(",", "");
}

export function AgentsSection({ app }: AgentsSectionProps) {
  const [agentsBusy, setAgentsBusy] = useState(false);
  const [detailBusy, setDetailBusy] = useState(false);
  const [filter, setFilter] = useState("");
  const [agents, setAgents] = useState<AgentRecord[]>([]);
  const [defaultAgentId, setDefaultAgentId] = useState<string | null>(null);
  const [selectedAgentId, setSelectedAgentId] = useState("");
  const [selectedAgent, setSelectedAgent] = useState<AgentEnvelope | null>(null);
  const [wizardOpen, setWizardOpen] = useState(false);
  const [wizardStep, setWizardStep] = useState<WizardStep>(0);
  const [draft, setDraft] = useState<AgentDraft>(createDefaultDraft);

  async function loadAgent(agentId: string): Promise<void> {
    if (agentId.trim().length === 0) {
      setSelectedAgent(null);
      return;
    }

    setDetailBusy(true);
    app.setError(null);
    try {
      const envelope = await app.api.getAgent(agentId);
      setSelectedAgent(envelope);
    } catch (error) {
      app.setError(error instanceof Error ? error.message : "Failed to load agent detail.");
    } finally {
      setDetailBusy(false);
    }
  }

  async function refreshAgents(preferredAgentId?: string): Promise<void> {
    setAgentsBusy(true);
    app.setError(null);
    try {
      const envelope: AgentListEnvelope = await app.api.listAgents();
      setAgents(envelope.agents);
      setDefaultAgentId(envelope.default_agent_id ?? null);

      const nextSelectedId =
        preferredAgentId ??
        (selectedAgentId.length > 0 && envelope.agents.some((agent) => agent.agent_id === selectedAgentId)
          ? selectedAgentId
          : envelope.default_agent_id ?? envelope.agents[0]?.agent_id ?? "");

      setSelectedAgentId(nextSelectedId);
      if (nextSelectedId.length === 0) {
        setSelectedAgent(null);
      } else {
        await loadAgent(nextSelectedId);
      }
    } catch (error) {
      app.setError(error instanceof Error ? error.message : "Failed to load agents.");
    } finally {
      setAgentsBusy(false);
    }
  }

  useEffect(() => {
    void refreshAgents();
  }, []);

  useEffect(() => {
    if (selectedAgentId.length === 0 || selectedAgent?.agent.agent_id === selectedAgentId) {
      return;
    }
    void loadAgent(selectedAgentId);
  }, [selectedAgentId]);

  const filteredAgents = useMemo(() => {
    const query = filter.trim().toLowerCase();
    const agentRows = agents.map((agent) => ({
      ...agent,
      isDefault: agent.agent_id === defaultAgentId,
      isSelected: agent.agent_id === selectedAgentId
    }));
    if (query.length === 0) {
      return agentRows;
    }
    return agentRows.filter((agent) =>
      `${agent.display_name} ${agent.agent_id} ${agent.default_model_profile}`.toLowerCase().includes(query)
    );
  }, [agents, defaultAgentId, filter, selectedAgentId]);

  const validationMessage = validationMessageForStep(wizardStep, draft);
  const detailRecord = selectedAgent?.agent ?? null;

  function closeWizard(): void {
    setWizardOpen(false);
    setWizardStep(0);
    setDraft(createDefaultDraft());
  }

  async function handleCreateAgent(): Promise<void> {
    setAgentsBusy(true);
    app.setError(null);
    try {
      const created = await app.api.createAgent(buildCreatePayload(draft));
      app.setNotice(`Agent '${created.agent.display_name}' created.`);
      closeWizard();
      await refreshAgents(created.agent.agent_id);
    } catch (error) {
      app.setError(error instanceof Error ? error.message : "Failed to create agent.");
    } finally {
      setAgentsBusy(false);
    }
  }

  async function handleSetDefault(agentId: string): Promise<void> {
    setAgentsBusy(true);
    app.setError(null);
    try {
      const result = await app.api.setDefaultAgent(agentId);
      app.setNotice(`Default agent set to '${result.default_agent_id}'.`);
      await refreshAgents(result.default_agent_id);
    } catch (error) {
      app.setError(error instanceof Error ? error.message : "Failed to set default agent.");
    } finally {
      setAgentsBusy(false);
    }
  }

  return (
    <main className="workspace-page">
      <WorkspacePageHeader
        eyebrow="Agent"
        title="Agents"
        description="Work against the real agent registry, create new agents with safe defaults, and promote a default agent without dropping to the CLI."
        status={
          <>
            <WorkspaceStatusChip tone={agents.length > 0 ? "success" : "default"}>
              {agents.length} registered
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={defaultAgentId !== null ? "success" : "warning"}>
              {defaultAgentId !== null ? `Default ${defaultAgentId}` : "No default agent"}
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={detailRecord !== null ? "accent" : "default"}>
              {detailRecord?.default_model_profile ?? "No model selected"}
            </WorkspaceStatusChip>
          </>
        }
        actions={
          <ActionCluster>
            <ActionButton
              variant="secondary"
              onPress={() => void refreshAgents()}
              isDisabled={agentsBusy}
            >
              {agentsBusy ? "Refreshing..." : "Refresh agents"}
            </ActionButton>
            <ActionButton onPress={() => setWizardOpen(true)}>Create agent</ActionButton>
          </ActionCluster>
        }
      />

      <section className="workspace-metric-grid workspace-metric-grid--compact">
        <WorkspaceMetricCard
          label="Registry size"
          value={agents.length}
          detail={
            agents.length > 0
              ? `${agents[0]?.display_name ?? "Agent"} is ready for review.`
              : "Create the first agent to establish a default registry."
          }
          tone={agents.length > 0 ? "success" : "warning"}
        />
        <WorkspaceMetricCard
          label="Workspace roots"
          value={detailRecord?.workspace_roots.length ?? 0}
          detail={detailRecord?.workspace_roots[0] ?? "Workspace defaults stay local and explicit."}
        />
        <WorkspaceMetricCard
          label="Default model"
          value={detailRecord?.default_model_profile ?? "n/a"}
          detail="Each agent keeps its own runtime defaults and allowlists."
          tone={detailRecord !== null ? "accent" : "default"}
        />
      </section>

      <section className="workspace-two-column">
        <WorkspaceSectionCard
          title="Registry"
          description="Search the registry, scan default state quickly, and select an agent for detail review."
        >
          <div className="workspace-stack">
            <TextInputField
              label="Search agents"
              value={filter}
              onChange={setFilter}
              placeholder="main, review, gpt-4o-mini"
            />

            <EntityTable
              ariaLabel="Agent registry"
              columns={[
                {
                  key: "agent",
                  label: "Agent",
                  isRowHeader: true,
                  render: (agent: AgentRow) => (
                    <div className="workspace-stack">
                      <strong>{agent.display_name}</strong>
                      <span className="chat-muted">{agent.agent_id}</span>
                    </div>
                  )
                },
                {
                  key: "state",
                  label: "State",
                  render: (agent: AgentRow) => (
                    <div className="workspace-inline">
                      <WorkspaceStatusChip tone={agent.isDefault ? "success" : "default"}>
                        {agent.isDefault ? "default" : "registered"}
                      </WorkspaceStatusChip>
                      <WorkspaceStatusChip tone="accent">
                        {agent.default_model_profile}
                      </WorkspaceStatusChip>
                      {agent.isSelected ? (
                        <WorkspaceStatusChip tone="accent">selected</WorkspaceStatusChip>
                      ) : null}
                    </div>
                  )
                },
                {
                  key: "actions",
                  label: "Actions",
                  align: "end",
                  render: (agent: AgentRow) => (
                    <ActionCluster>
                      <ActionButton
                        aria-label={`Inspect ${agent.display_name}`}
                        variant="secondary"
                        size="sm"
                        onPress={() => setSelectedAgentId(agent.agent_id)}
                      >
                        Inspect
                      </ActionButton>
                      {!agent.isDefault ? (
                        <ActionButton
                          aria-label={`Set ${agent.display_name} as default`}
                          size="sm"
                          onPress={() => void handleSetDefault(agent.agent_id)}
                          isDisabled={agentsBusy}
                        >
                          Set default
                        </ActionButton>
                      ) : null}
                    </ActionCluster>
                  )
                }
              ]}
              rows={filteredAgents}
              getRowId={(agent: AgentRow) => agent.agent_id}
              emptyTitle={agents.length === 0 ? "No agents registered" : "No matching agents"}
              emptyDescription={
                agents.length === 0
                  ? "Open the setup wizard to create the first agent."
                  : "Adjust the current filter to find a matching agent."
              }
            />
          </div>
        </WorkspaceSectionCard>

        <WorkspaceSectionCard
          title="Selected agent"
          description="Inspect directories, workspace roots, and allowlists before promoting a default."
        >
          {detailBusy ? (
            <p className="chat-muted">Loading agent detail...</p>
          ) : detailRecord === null ? (
            <EmptyState
              compact
              title="No agent selected"
              description="Select an agent from the registry to inspect its detail."
            />
          ) : (
            <div className="workspace-stack">
              <div className="workspace-inline">
                <WorkspaceStatusChip tone={selectedAgent?.is_default ? "success" : "default"}>
                  {selectedAgent?.is_default ? "Default agent" : "Registered agent"}
                </WorkspaceStatusChip>
                <WorkspaceStatusChip tone="accent">
                  {detailRecord.default_model_profile}
                </WorkspaceStatusChip>
              </div>

              <KeyValueList
                items={[
                  { label: "Display name", value: detailRecord.display_name },
                  { label: "Agent ID", value: detailRecord.agent_id },
                  { label: "Agent dir", value: detailRecord.agent_dir },
                  { label: "Created", value: formatUnixMs(detailRecord.created_at_unix_ms) },
                  { label: "Updated", value: formatUnixMs(detailRecord.updated_at_unix_ms) }
                ]}
              />

              <div className="workspace-two-column">
                <WorkspaceSectionCard
                  title="Workspace roots"
                  description="Keep the execution boundary visible and operator-reviewable."
                  className="workspace-section-card--nested"
                >
                  <ul className="workspace-bullet-list">
                    {detailRecord.workspace_roots.map((root) => (
                      <li key={root}>{root}</li>
                    ))}
                  </ul>
                </WorkspaceSectionCard>

                <WorkspaceSectionCard
                  title="Allowlists"
                  description="No speculative permissions are injected by the wizard."
                  className="workspace-section-card--nested"
                >
                  <KeyValueList
                    items={[
                      {
                        label: "Tools",
                        value:
                          detailRecord.default_tool_allowlist.join(", ") ||
                          "No explicit tool allowlist"
                      },
                      {
                        label: "Skills",
                        value:
                          detailRecord.default_skill_allowlist.join(", ") ||
                          "No explicit skill allowlist"
                      }
                    ]}
                  />
                </WorkspaceSectionCard>
              </div>

              {!selectedAgent?.is_default ? (
                <ActionButton
                  onPress={() => void handleSetDefault(detailRecord.agent_id)}
                  isDisabled={agentsBusy}
                >
                  {agentsBusy ? "Applying..." : "Set as default"}
                </ActionButton>
              ) : null}
            </div>
          )}
        </WorkspaceSectionCard>
      </section>

      <Modal isOpen={wizardOpen} onOpenChange={setWizardOpen}>
        <Modal.Trigger aria-hidden="true" className="sr-only">
          Open agent wizard
        </Modal.Trigger>
        <Modal.Backdrop />
        <Modal.Container placement="center" size="lg">
          <Modal.Dialog>
            <Modal.Header>
              <div className="workspace-stack">
                <h3>Create agent</h3>
                <p className="chat-muted">
                  Create a real registry entry backed by the daemon, not local mock state.
                </p>
              </div>
            </Modal.Header>
            <Modal.Body>
              <ActionCluster className="workspace-tab-row">
                {WIZARD_STEPS.map((step) => (
                  <ActionButton
                    key={step.id}
                    type="button"
                    variant={wizardStep === step.id ? "primary" : "ghost"}
                    onPress={() => setWizardStep(step.id)}
                  >
                    {step.label}
                  </ActionButton>
                ))}
              </ActionCluster>

              <WorkspaceSectionCard
                title={WIZARD_STEPS[wizardStep].label}
                description={WIZARD_STEPS[wizardStep].description}
                className="workspace-section-card--nested"
              >
                {wizardStep === 0 ? (
                  <div className="workspace-form-grid">
                    <TextInputField
                      label="Agent ID"
                      value={draft.agentId}
                      onChange={(agentId) => setDraft((current) => ({ ...current, agentId }))}
                      placeholder="review-agent"
                    />
                    <TextInputField
                      label="Display name"
                      value={draft.displayName}
                      onChange={(displayName) =>
                        setDraft((current) => ({ ...current, displayName }))
                      }
                      placeholder="Review Agent"
                    />
                  </div>
                ) : null}

                {wizardStep === 1 ? (
                  <div className="workspace-stack">
                    <TextInputField
                      label="Agent dir"
                      value={draft.agentDir}
                      onChange={(agentDir) => setDraft((current) => ({ ...current, agentDir }))}
                      placeholder="Leave blank for safe state-root defaults"
                    />
                    <TextAreaField
                      label="Workspace roots"
                      rows={4}
                      value={draft.workspaceRoots}
                      onChange={(workspaceRoots) =>
                        setDraft((current) => ({ ...current, workspaceRoots }))
                      }
                      placeholder={"workspace\nworkspace-review"}
                    />
                    <CheckboxField
                      label="Allow absolute paths"
                      checked={draft.allowAbsolutePaths}
                      onChange={(allowAbsolutePaths) =>
                        setDraft((current) => ({ ...current, allowAbsolutePaths }))
                      }
                    />
                  </div>
                ) : null}

                {wizardStep === 2 ? (
                  <div className="workspace-stack">
                    <div className="workspace-form-grid">
                      <TextInputField
                        label="Default model profile"
                        value={draft.defaultModelProfile}
                        onChange={(defaultModelProfile) =>
                          setDraft((current) => ({ ...current, defaultModelProfile }))
                        }
                        placeholder="gpt-4o-mini"
                      />
                      <CheckboxField
                        label="Set as default agent"
                        checked={draft.setDefault}
                        onChange={(setDefault) =>
                          setDraft((current) => ({ ...current, setDefault }))
                        }
                      />
                    </div>
                    <TextAreaField
                      label="Tool allowlist"
                      rows={3}
                      value={draft.defaultToolAllowlist}
                      onChange={(defaultToolAllowlist) =>
                        setDraft((current) => ({ ...current, defaultToolAllowlist }))
                      }
                      placeholder={"palyra.echo\npalyra.http.fetch"}
                    />
                    <TextAreaField
                      label="Skill allowlist"
                      rows={3}
                      value={draft.defaultSkillAllowlist}
                      onChange={(defaultSkillAllowlist) =>
                        setDraft((current) => ({ ...current, defaultSkillAllowlist }))
                      }
                      placeholder={"acme.echo\nacme.review"}
                    />
                  </div>
                ) : null}

                {wizardStep === 3 ? (
                  <KeyValueList
                    items={[
                      { label: "Agent ID", value: draft.agentId.trim() || "n/a" },
                      { label: "Display name", value: draft.displayName.trim() || "n/a" },
                      { label: "Agent dir", value: draft.agentDir.trim() || "Auto under state root" },
                      { label: "Workspace roots", value: resolveWorkspaceRoots(draft).join(", ") },
                      {
                        label: "Model profile",
                        value: draft.defaultModelProfile.trim() || "Backend default"
                      },
                      {
                        label: "Tool allowlist",
                        value: parseTextList(draft.defaultToolAllowlist).join(", ") || "none"
                      },
                      {
                        label: "Skill allowlist",
                        value: parseTextList(draft.defaultSkillAllowlist).join(", ") || "none"
                      },
                      {
                        label: "Default selection",
                        value: draft.setDefault ? "Set as default" : "Keep current default"
                      },
                      {
                        label: "Absolute paths",
                        value: draft.allowAbsolutePaths ? "Allowed" : "Disabled"
                      }
                    ]}
                  />
                ) : null}

                {validationMessage !== null ? (
                  <InlineNotice title="Validation" tone="warning">
                    {validationMessage}
                  </InlineNotice>
                ) : null}
              </WorkspaceSectionCard>
            </Modal.Body>
            <Modal.Footer>
              <ActionCluster>
                <ActionButton variant="secondary" onPress={closeWizard}>
                  Cancel
                </ActionButton>
                {wizardStep > 0 ? (
                  <ActionButton
                    variant="secondary"
                    onPress={() => setWizardStep((wizardStep - 1) as WizardStep)}
                  >
                    Back
                  </ActionButton>
                ) : null}
                {wizardStep < 3 ? (
                  <ActionButton
                    onPress={() => setWizardStep((wizardStep + 1) as WizardStep)}
                    isDisabled={validationMessage !== null}
                  >
                    Next
                  </ActionButton>
                ) : (
                  <ActionButton
                    onPress={() => void handleCreateAgent()}
                    isDisabled={validationMessage !== null || agentsBusy}
                  >
                    {agentsBusy ? "Creating..." : "Create agent"}
                  </ActionButton>
                )}
              </ActionCluster>
            </Modal.Footer>
          </Modal.Dialog>
        </Modal.Container>
      </Modal>
    </main>
  );
}
