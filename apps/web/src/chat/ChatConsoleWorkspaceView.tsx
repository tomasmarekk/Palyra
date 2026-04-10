import { Chip } from "@heroui/react";
import type { ComponentProps } from "react";

import {
  ActionButton,
  PageHeader,
  SectionCard,
  StatusChip,
  SwitchField,
} from "../console/components/ui";

import { ChatComposer } from "./ChatComposer";
import { ChatInspectorColumn } from "./ChatInspectorColumn";
import { ChatSessionsSidebar } from "./ChatSessionsSidebar";
import { ChatTranscript } from "./ChatTranscript";

interface ChatContextBudgetSummary {
  readonly estimated_total_tokens: number;
  readonly label: string;
  readonly tone: "default" | "warning" | "danger";
}

interface ChatConsoleWorkspaceViewProps {
  readonly allowSensitiveTools: boolean;
  readonly canAbortRun: boolean;
  readonly canInspectRun: boolean;
  readonly composerProps: ComponentProps<typeof ChatComposer>;
  readonly contextBudget: ChatContextBudgetSummary;
  readonly inspectorProps: ComponentProps<typeof ChatInspectorColumn>;
  readonly onAbortRun: () => void;
  readonly onOpenObjective?: (() => void) | null;
  readonly onOpenRunDetails: () => void;
  readonly onRefresh: () => void;
  readonly onSetAllowSensitiveTools: (next: boolean) => void;
  readonly pendingApprovalCount: number;
  readonly runActionBusy: boolean;
  readonly selectedObjectiveFocus?: string | null;
  readonly selectedObjectiveLabel?: string | null;
  readonly selectedSessionBranchState: string;
  readonly selectedSessionLineage: string;
  readonly selectedSessionTitle: string;
  readonly sessionsBusy: boolean;
  readonly sessionsSidebarProps: ComponentProps<typeof ChatSessionsSidebar>;
  readonly streaming: boolean;
  readonly toolPayloadCount: number;
  readonly transcriptBusy: boolean;
  readonly transcriptProps: ComponentProps<typeof ChatTranscript>;
}

export function ChatConsoleWorkspaceView({
  allowSensitiveTools,
  canAbortRun,
  canInspectRun,
  composerProps,
  contextBudget,
  inspectorProps,
  onAbortRun,
  onOpenObjective,
  onOpenRunDetails,
  onRefresh,
  onSetAllowSensitiveTools,
  pendingApprovalCount,
  runActionBusy,
  selectedObjectiveFocus,
  selectedObjectiveLabel,
  selectedSessionBranchState,
  selectedSessionLineage,
  selectedSessionTitle,
  sessionsBusy,
  sessionsSidebarProps,
  streaming,
  toolPayloadCount,
  transcriptBusy,
  transcriptProps,
}: ChatConsoleWorkspaceViewProps) {
  return (
    <>
      <PageHeader
        eyebrow="Chat"
        title={selectedSessionTitle}
        description="Sessions, retries, branches, transcript operations, and payload inspection stay on one operator surface without dumping raw tool JSON into the main conversation."
        status={
          <>
            <StatusChip tone={streaming ? "warning" : "success"}>
              {streaming ? "Streaming" : "Idle"}
            </StatusChip>
            <StatusChip tone={pendingApprovalCount > 0 ? "warning" : "default"}>
              {pendingApprovalCount} pending approval{pendingApprovalCount === 1 ? "" : "s"}
            </StatusChip>
            <StatusChip tone={toolPayloadCount > 0 ? "accent" : "default"}>
              {toolPayloadCount} payload{toolPayloadCount === 1 ? "" : "s"} in sidebar
            </StatusChip>
            {selectedObjectiveLabel ? (
              <StatusChip tone="accent">{selectedObjectiveLabel}</StatusChip>
            ) : null}
            <Chip size="sm" variant="secondary">
              {selectedSessionBranchState}
            </Chip>
          </>
        }
        actions={
          <div className="workspace-inline-actions">
            <SwitchField
              checked={allowSensitiveTools}
              description="Applies to the next streamed run only."
              label="Allow sensitive tools"
              onChange={onSetAllowSensitiveTools}
            />
            <ActionButton
              isDisabled={sessionsBusy || transcriptBusy}
              type="button"
              variant="secondary"
              onPress={onRefresh}
            >
              {sessionsBusy || transcriptBusy ? "Refreshing..." : "Refresh"}
            </ActionButton>
            <ActionButton isDisabled={!canInspectRun} type="button" onPress={onOpenRunDetails}>
              Run details
            </ActionButton>
            {selectedObjectiveLabel ? (
              <ActionButton
                isDisabled={onOpenObjective === null || onOpenObjective === undefined}
                type="button"
                variant="secondary"
                onPress={() => onOpenObjective?.()}
              >
                Open objective
              </ActionButton>
            ) : null}
            <ActionButton
              isDisabled={runActionBusy || !canAbortRun}
              type="button"
              variant="ghost"
              onPress={onAbortRun}
            >
              {runActionBusy ? "Interrupting..." : "Interrupt run"}
            </ActionButton>
          </div>
        }
      />

      <section className="chat-workspace__layout">
        <SectionCard
          className="chat-panel"
          description="Create, rename, branch-aware inspect, reset, and switch sessions without leaving the active conversation."
          title="Sessions"
          actions={
            <StatusChip tone={sessionsSidebarProps.selectedSession ? "success" : "warning"}>
              {sessionsSidebarProps.selectedSession ? "Active session" : "No session"}
            </StatusChip>
          }
        >
          <ChatSessionsSidebar {...sessionsSidebarProps} />
        </SectionCard>

        <SectionCard
          className="chat-panel chat-panel--conversation"
          description="Transcript stays readable, tool payloads move to the side panel, and slash commands stay close to the composer."
          title="Conversation"
          actions={
            <div className="workspace-inline-actions">
              <StatusChip tone={streaming ? "warning" : "success"}>
                {streaming ? "Streaming" : "Idle"}
              </StatusChip>
              <StatusChip
                tone={
                  contextBudget.tone === "danger"
                    ? "danger"
                    : contextBudget.tone === "warning"
                      ? "warning"
                      : "default"
                }
              >
                {contextBudget.label}
              </StatusChip>
              {selectedObjectiveFocus ? (
                <StatusChip tone="accent">{selectedObjectiveFocus}</StatusChip>
              ) : null}
              <Chip variant="secondary">{selectedSessionLineage}</Chip>
            </div>
          }
        >
          <div className="chat-panel__body">
            <ChatTranscript {...transcriptProps} />
            <ChatComposer {...composerProps} />
          </div>
        </SectionCard>

        <ChatInspectorColumn {...inspectorProps} />
      </section>
    </>
  );
}
