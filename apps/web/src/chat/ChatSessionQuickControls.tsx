import type { AgentRecord, SessionCatalogRecord } from "../consoleApi";
import {
  ActionButton,
  EmptyState,
  SelectField,
  StatusChip,
  SwitchField,
  TextInputField,
} from "../console/components/ui";

type ChatSessionQuickControlSharedProps = {
  readonly session: SessionCatalogRecord | null;
  readonly agents: readonly AgentRecord[];
  readonly busy: boolean;
  readonly modelDraft: string;
  readonly setModelDraft: (value: string) => void;
  readonly onSelectAgent: (agentId: string | null) => void;
  readonly onApplyModel: () => void;
  readonly onClearModel: () => void;
  readonly onToggleThinking: (next: boolean) => void;
  readonly onToggleTrace: (next: boolean) => void;
  readonly onToggleVerbose: (next: boolean) => void;
  readonly onReset: () => void;
};

export function ChatSessionQuickControlHeader({
  session,
  busy,
  onToggleThinking,
  onToggleTrace,
  onToggleVerbose,
  onReset,
}: Pick<
  ChatSessionQuickControlSharedProps,
  "session" | "busy" | "onToggleThinking" | "onToggleTrace" | "onToggleVerbose" | "onReset"
>) {
  if (session === null) {
    return null;
  }

  return (
    <div className="workspace-inline-actions">
      <StatusChip tone={session.quick_controls.agent.override_active ? "accent" : "default"}>
        Agent · {session.quick_controls.agent.display_value}
      </StatusChip>
      <StatusChip tone={session.quick_controls.model.override_active ? "accent" : "default"}>
        Model · {session.quick_controls.model.display_value}
      </StatusChip>
      <ActionButton
        isDisabled={busy}
        size="sm"
        type="button"
        variant={session.quick_controls.thinking.value ? "primary" : "secondary"}
        onPress={() => onToggleThinking(!session.quick_controls.thinking.value)}
      >
        Thinking {session.quick_controls.thinking.value ? "on" : "off"}
      </ActionButton>
      <ActionButton
        isDisabled={busy}
        size="sm"
        type="button"
        variant={session.quick_controls.trace.value ? "primary" : "secondary"}
        onPress={() => onToggleTrace(!session.quick_controls.trace.value)}
      >
        Trace {session.quick_controls.trace.value ? "on" : "off"}
      </ActionButton>
      <ActionButton
        isDisabled={busy}
        size="sm"
        type="button"
        variant={session.quick_controls.verbose.value ? "primary" : "secondary"}
        onPress={() => onToggleVerbose(!session.quick_controls.verbose.value)}
      >
        Verbose {session.quick_controls.verbose.value ? "on" : "off"}
      </ActionButton>
      <ActionButton
        isDisabled={busy || !session.quick_controls.reset_to_default_available}
        size="sm"
        type="button"
        variant="ghost"
        onPress={onReset}
      >
        Reset defaults
      </ActionButton>
    </div>
  );
}

export function ChatSessionQuickControlPanel({
  session,
  agents,
  busy,
  modelDraft,
  setModelDraft,
  onSelectAgent,
  onApplyModel,
  onClearModel,
  onToggleThinking,
  onToggleTrace,
  onToggleVerbose,
  onReset,
}: ChatSessionQuickControlSharedProps) {
  if (session === null) {
    return (
      <EmptyState
        compact
        description="Select an active session to adjust agent, model, and transcript visibility overrides."
        title="No session selected"
      />
    );
  }

  const agentValue =
    session.quick_controls.agent.source === "session_binding"
      ? (session.quick_controls.agent.value ?? "")
      : "";
  const modelPlaceholder = session.quick_controls.model.inherited_value ?? "Inherited default";

  return (
    <div className="grid gap-4">
      <div className="workspace-tag-row">
        <StatusChip tone={toneForSource(session.quick_controls.agent.source)}>
          Agent source · {describeControlSource(session.quick_controls.agent.source)}
        </StatusChip>
        <StatusChip tone={toneForSource(session.quick_controls.model.source)}>
          Model source · {describeControlSource(session.quick_controls.model.source)}
        </StatusChip>
        <StatusChip tone={session.quick_controls.reset_to_default_available ? "accent" : "default"}>
          {session.quick_controls.reset_to_default_available
            ? "Session overrides active"
            : "Inherited defaults only"}
        </StatusChip>
      </div>

      <div className="workspace-field-grid workspace-field-grid--double">
        <SelectField
          description={
            session.quick_controls.agent.inherited_value
              ? `Inherited agent: ${session.quick_controls.agent.inherited_value}`
              : "Leave empty to follow the inherited/default agent."
          }
          disabled={busy}
          label="Agent binding"
          options={[
            {
              key: "",
              label: "Inherited default",
              description: "Clear the session binding and fall back to the inherited agent.",
            },
            ...agents.map((agent) => ({
              key: agent.agent_id,
              label: agent.display_name,
              description: `${agent.agent_id} · default model ${agent.default_model_profile}`,
            })),
          ]}
          placeholder="Inherited default"
          value={agentValue}
          onChange={(value) => onSelectAgent(value.trim().length === 0 ? null : value)}
        />
        <div className="workspace-stack workspace-stack--compact">
          <TextInputField
            description={`Current inherited model: ${modelPlaceholder}`}
            disabled={busy}
            label="Model override"
            placeholder={modelPlaceholder}
            value={modelDraft}
            onChange={setModelDraft}
          />
          <div className="workspace-inline-actions">
            <ActionButton
              isDisabled={busy}
              size="sm"
              type="button"
              variant="secondary"
              onPress={onApplyModel}
            >
              Apply model
            </ActionButton>
            <ActionButton
              isDisabled={busy || session.quick_controls.model.source !== "session_override"}
              size="sm"
              type="button"
              variant="ghost"
              onPress={onClearModel}
            >
              Clear override
            </ActionButton>
          </div>
        </div>
      </div>

      <div className="workspace-field-grid workspace-field-grid--triple">
        <SwitchField
          checked={session.quick_controls.thinking.value}
          description={describeToggleDescription(
            session.quick_controls.thinking.inherited_value,
            session.quick_controls.thinking.source,
          )}
          disabled={busy}
          label="Thinking and status"
          onChange={onToggleThinking}
        />
        <SwitchField
          checked={session.quick_controls.trace.value}
          description={describeToggleDescription(
            session.quick_controls.trace.inherited_value,
            session.quick_controls.trace.source,
          )}
          disabled={busy}
          label="Trace and tool cards"
          onChange={onToggleTrace}
        />
        <SwitchField
          checked={session.quick_controls.verbose.value}
          description={describeToggleDescription(
            session.quick_controls.verbose.inherited_value,
            session.quick_controls.verbose.source,
          )}
          disabled={busy}
          label="Verbose timeline"
          onChange={onToggleVerbose}
        />
      </div>

      <div className="workspace-inline-actions">
        <ActionButton
          isDisabled={busy || !session.quick_controls.reset_to_default_available}
          size="sm"
          type="button"
          variant="ghost"
          onPress={onReset}
        >
          Reset all session overrides
        </ActionButton>
      </div>
    </div>
  );
}

function describeControlSource(source: string): string {
  switch (source) {
    case "session_binding":
      return "session binding";
    case "session_override":
      return "session override";
    case "agent_default_model_profile":
      return "bound agent default";
    case "default_agent_model_profile":
      return "default agent";
    case "surface_default":
      return "surface default";
    case "unassigned":
      return "unassigned";
    default:
      return source.replaceAll("_", " ");
  }
}

function describeToggleDescription(inheritedValue: boolean, source: string): string {
  return `${describeControlSource(source)} · inherited ${inheritedValue ? "on" : "off"}`;
}

function toneForSource(source: string): "default" | "accent" | "success" | "warning" | "danger" {
  switch (source) {
    case "session_binding":
    case "session_override":
      return "accent";
    case "agent_default_model_profile":
    case "default_agent_model_profile":
      return "success";
    case "unassigned":
      return "warning";
    default:
      return "default";
  }
}
