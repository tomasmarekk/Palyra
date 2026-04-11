import { Tabs } from "@heroui/react";
import { useMemo, useState } from "react";

import { DiscordOnboardingPanel } from "../../features/channels/connectors/discord/components/DiscordOnboardingPanel";
import { DiscordConnectorActionsPanel } from "../../features/channels/connectors/discord/components/DiscordConnectorActionsPanel";
import {
  ActionButton,
  ActionCluster,
  AppForm,
  CheckboxField,
  EmptyState,
  EntityTable,
  InlineNotice,
  KeyValueList,
  SelectField,
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
  PrettyJsonBlock,
  channelConnectorAvailability,
  readBool,
  readNumber,
  readObject,
  readString,
  type JsonObject,
} from "../shared";
import type { ConsoleAppState } from "../useConsoleAppState";

type ChannelsTab = "connectors" | "messages" | "router" | "discord";

type ConnectorRow = {
  connector: JsonObject;
  connectorId: string;
  connectorKind: string;
  enabled: boolean;
  readiness: string;
  availability: string;
  isSelected: boolean;
};

type DeadLetterRow = {
  deadLetterId: number;
};

type PairingRow = {
  id: string;
  channel: string;
  principal: string;
  status: string;
};

type MessageRow = {
  id: string;
  conversationId: string;
  threadId: string;
  sender: string;
  body: string;
  createdAt: string;
  attachments: number;
  reactions: string;
  link: string | null;
  raw: JsonObject;
};

export function ChannelsSection({ app }: { app: ConsoleAppState }) {
  const [activeTab, setActiveTab] = useState<ChannelsTab>("connectors");
  const discord = app.discordChannel;
  const selectedStatusPayload: JsonObject = app.channelsSelectedStatus ?? {};
  const selectedConnector = readObject(selectedStatusPayload, "connector") ?? selectedStatusPayload;
  const selectedOperations = readObject(selectedStatusPayload, "operations");
  const selectedQueue =
    selectedOperations !== null ? readObject(selectedOperations, "queue") : null;
  const selectedSaturation =
    selectedOperations !== null ? readObject(selectedOperations, "saturation") : null;
  const selectedDiscordOps =
    selectedOperations !== null ? readObject(selectedOperations, "discord") : null;
  const selectedHealthRefresh = readObject(selectedStatusPayload, "health_refresh");
  const selectedConnectorKind = readString(selectedConnector, "kind");
  const enabledConnectors = app.channelsConnectors.filter((connector) =>
    readBool(connector, "enabled"),
  );

  const connectorRows = useMemo<ConnectorRow[]>(
    () =>
      app.channelsConnectors.map((connector) => {
        const connectorId = readString(connector, "connector_id") ?? "unknown";
        return {
          connector,
          connectorId,
          connectorKind: readString(connector, "kind") ?? "unknown kind",
          enabled: readBool(connector, "enabled"),
          readiness: readString(connector, "readiness") ?? "readiness n/a",
          availability: channelConnectorAvailability(connector),
          isSelected: connectorId === app.channelsSelectedConnectorId,
        };
      }),
    [app.channelsConnectors, app.channelsSelectedConnectorId],
  );

  const deadLetterRows = useMemo<DeadLetterRow[]>(
    () =>
      app.channelsDeadLetters.flatMap((deadLetter) => {
        const deadLetterId =
          typeof deadLetter.dead_letter_id === "number"
            ? deadLetter.dead_letter_id
            : Number(deadLetter.dead_letter_id ?? Number.NaN);
        return Number.isFinite(deadLetterId) ? [{ deadLetterId }] : [];
      }),
    [app.channelsDeadLetters],
  );

  const pairingRows = useMemo<PairingRow[]>(
    () =>
      app.channelRouterPairings.map((pairing, index) => {
        const record = asJsonObject(pairing);
        return {
          id:
            readString(record, "pairing_id") ??
            readString(record, "principal") ??
            `pairing-${index}`,
          channel: readString(record, "channel") ?? readString(record, "channel_id") ?? "n/a",
          principal: readString(record, "principal") ?? readString(record, "user_id") ?? "n/a",
          status: readString(record, "status") ?? "unknown",
        };
      }),
    [app.channelRouterPairings],
  );
  const messageCapabilities = useMemo<JsonObject[]>(() => {
    const connectorCapabilities = readObject(selectedConnector, "capabilities");
    const messageCapabilitiesObject =
      connectorCapabilities !== null ? readObject(connectorCapabilities, "message") : null;
    const actionDetails = messageCapabilitiesObject?.action_details;
    return Array.isArray(actionDetails)
      ? actionDetails.filter((entry): entry is JsonObject => asMaybeJsonObject(entry) !== null)
      : [];
  }, [selectedConnector]);
  const messageReadResult = readObject(app.channelMessageReadResult ?? {}, "result");
  const messageSearchResult = readObject(app.channelMessageSearchResult ?? {}, "result");
  const messageMutationResult = app.channelMessageMutationResult ?? null;
  const messageMutationApproval = readObject(messageMutationResult ?? {}, "approval");
  const messageMutationPreview = readObject(messageMutationResult ?? {}, "preview");
  const messageMutationPolicy = readObject(messageMutationResult ?? {}, "policy");
  const messageMutationAppliedResult = readObject(messageMutationResult ?? {}, "result");
  const latestMessageCollection =
    messageSearchResult !== null ? messageSearchResult : messageReadResult;
  const latestMessageRows = useMemo<MessageRow[]>(() => {
    const entries = Array.isArray(latestMessageCollection?.messages)
      ? latestMessageCollection.messages
      : Array.isArray(latestMessageCollection?.matches)
        ? latestMessageCollection.matches
        : [];
    return entries
      .map((entry, index) => toMessageRow(entry, index))
      .filter((row): row is MessageRow => row !== null);
  }, [latestMessageCollection]);

  function prefillMutationFromRow(row: MessageRow): void {
    const locator = readObject(row.raw, "locator");
    app.setChannelMessageConversationId(readString(locator ?? {}, "conversation_id") ?? "");
    app.setChannelMessageThreadId(readString(locator ?? {}, "thread_id") ?? "");
    app.setChannelMessageMutationMessageId(readString(locator ?? {}, "message_id") ?? "");
    app.setChannelMessageMutationBody(readString(row.raw, "body") ?? "");
  }

  return (
    <main className="workspace-page">
      <WorkspacePageHeader
        eyebrow="Control"
        title="Channels"
        description="Operate connector health, routing behavior, and Discord onboarding from one domain surface with focused modes instead of a flat pile of unrelated cards."
        status={
          <>
            <WorkspaceStatusChip tone={enabledConnectors.length > 0 ? "success" : "default"}>
              {enabledConnectors.length} enabled
            </WorkspaceStatusChip>
            <WorkspaceStatusChip
              tone={app.channelRouterWarnings.length > 0 ? "warning" : "success"}
            >
              {app.channelRouterWarnings.length} router warnings
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={discord.discordWizardBusy ? "warning" : "default"}>
              {discord.discordWizardBusy ? "Discord setup busy" : "Discord setup ready"}
            </WorkspaceStatusChip>
          </>
        }
        actions={
          <ActionButton
            variant="secondary"
            onPress={() => void app.refreshChannels()}
            isDisabled={app.channelsBusy}
          >
            {app.channelsBusy ? "Refreshing..." : "Refresh channels"}
          </ActionButton>
        }
      />

      <section className="workspace-metric-grid">
        <WorkspaceMetricCard
          label="Connectors"
          value={app.channelsConnectors.length}
          detail={
            app.channelsConnectors[0] === undefined
              ? "No connectors configured."
              : (readString(app.channelsConnectors[0], "connector_id") ?? "Connector available")
          }
          tone={enabledConnectors.length > 0 ? "success" : "default"}
        />
        <WorkspaceMetricCard
          label="Selected connector"
          value={app.channelsSelectedConnectorId || "None"}
          detail={readString(selectedConnector, "kind") ?? "Select a connector to inspect status."}
        />
        <WorkspaceMetricCard
          label="Router pairings"
          value={app.channelRouterPairings.length}
          detail={
            app.channelRouterMintResult === null
              ? "No fresh pairing code minted."
              : "Latest mint result available."
          }
          tone={app.channelRouterWarnings.length > 0 ? "warning" : "default"}
        />
        <WorkspaceMetricCard
          label="Discord onboarding"
          value={
            discord.discordWizardApply === null
              ? "Awaiting action"
              : (readString(discord.discordWizardApply, "result") ?? "Result ready")
          }
          detail={
            discord.discordWizardPreflight === null
              ? "Run preflight to inspect requirements."
              : "Preflight results are available."
          }
          tone={discord.discordWizardApply === null ? "default" : "success"}
        />
      </section>

      <Tabs
        className="w-full"
        selectedKey={activeTab}
        variant="secondary"
        onSelectionChange={(key) => setActiveTab(String(key) as ChannelsTab)}
      >
        <Tabs.ListContainer>
          <Tabs.List aria-label="Channels workspace modes" className="w-fit">
            <Tabs.Tab id="connectors">
              Connectors
              <Tabs.Indicator />
            </Tabs.Tab>
            <Tabs.Tab id="messages">
              Messages
              <Tabs.Indicator />
            </Tabs.Tab>
            <Tabs.Tab id="router">
              Router
              <Tabs.Indicator />
            </Tabs.Tab>
            <Tabs.Tab id="discord">
              Discord setup
              <Tabs.Indicator />
            </Tabs.Tab>
          </Tabs.List>
        </Tabs.ListContainer>

        <Tabs.Panel className="pt-4" id="connectors">
          <section className="workspace-two-column">
            <WorkspaceSectionCard
              title="Connector inventory"
              description="Enable, disable, and select the connector you want to inspect before using test or recovery actions."
            >
              <EntityTable
                ariaLabel="Connector inventory"
                columns={[
                  {
                    key: "connector",
                    label: "Connector",
                    isRowHeader: true,
                    render: (row) => (
                      <div className="workspace-stack">
                        <strong>{row.connectorId}</strong>
                        <span className="chat-muted">
                          {row.connectorKind} · {row.availability}
                        </span>
                      </div>
                    ),
                  },
                  {
                    key: "state",
                    label: "State",
                    render: (row) => (
                      <div className="workspace-inline">
                        <WorkspaceStatusChip tone={row.enabled ? "success" : "default"}>
                          {row.enabled ? "enabled" : "disabled"}
                        </WorkspaceStatusChip>
                        <WorkspaceStatusChip tone="default">{row.readiness}</WorkspaceStatusChip>
                        {row.isSelected ? (
                          <WorkspaceStatusChip tone="accent">selected</WorkspaceStatusChip>
                        ) : null}
                      </div>
                    ),
                  },
                  {
                    key: "actions",
                    label: "Actions",
                    align: "end",
                    render: (row) => (
                      <ActionCluster>
                        <ActionButton
                          aria-label={`Select ${row.connectorId}`}
                          variant="secondary"
                          size="sm"
                          onPress={() => void app.selectChannelConnector(row.connectorId)}
                        >
                          Select
                        </ActionButton>
                        <ActionButton
                          aria-label={`${row.enabled ? "Disable" : "Enable"} ${row.connectorId}`}
                          size="sm"
                          onPress={() => void app.toggleConnector(row.connector, !row.enabled)}
                          isDisabled={app.channelsBusy}
                        >
                          {row.enabled ? "Disable" : "Enable"}
                        </ActionButton>
                      </ActionCluster>
                    ),
                  },
                ]}
                rows={connectorRows}
                getRowId={(row) => row.connectorId}
                emptyTitle="No connectors configured"
                emptyDescription="Configure at least one connector to inspect status and recovery actions."
              />
            </WorkspaceSectionCard>

            <WorkspaceSectionCard
              title="Selected connector"
              description="Connector status, recovery controls, logs, and direct test actions stay in one focused column."
            >
              <div className="workspace-stack">
                {app.channelsSelectedStatus === null ? (
                  <EmptyState
                    compact
                    title="No connector selected"
                    description="Select a connector from inventory to inspect runtime state."
                  />
                ) : (
                  <PrettyJsonBlock
                    value={app.channelsSelectedStatus}
                    revealSensitiveValues={app.revealSensitiveValues}
                  />
                )}

                <div className="workspace-form-grid">
                  <TextInputField
                    label="Selected connector"
                    value={app.channelsSelectedConnectorId}
                    onChange={() => undefined}
                    readOnly
                  />
                  <TextInputField
                    label="Log limit"
                    value={app.channelsLogsLimit}
                    onChange={app.setChannelsLogsLimit}
                  />
                </div>

                <ActionCluster>
                  <ActionButton
                    variant="secondary"
                    onPress={() => void app.pauseChannelQueue()}
                    isDisabled={app.channelsBusy}
                  >
                    Pause queue
                  </ActionButton>
                  <ActionButton
                    variant="secondary"
                    onPress={() => void app.resumeChannelQueue()}
                    isDisabled={app.channelsBusy}
                  >
                    Resume queue
                  </ActionButton>
                  <ActionButton
                    variant="secondary"
                    onPress={() => void app.drainChannelQueue()}
                    isDisabled={app.channelsBusy}
                  >
                    Drain queue
                  </ActionButton>
                  <ActionButton
                    variant="secondary"
                    onPress={() => void discord.refreshHealth()}
                    isDisabled={app.channelsBusy}
                  >
                    Refresh health
                  </ActionButton>
                </ActionCluster>

                {(selectedQueue !== null || selectedHealthRefresh !== null) && (
                  <InlineNotice title="Recovery telemetry" tone="default">
                    <KeyValueList
                      items={[
                        {
                          label: "Queue paused",
                          value: readBool(selectedQueue ?? {}, "paused") ? "yes" : "no",
                        },
                        {
                          label: "Dead letters",
                          value: displayScalar(selectedQueue?.dead_letters, "0"),
                        },
                        {
                          label: "Saturation",
                          value: readString(selectedSaturation ?? {}, "state") ?? "n/a",
                        },
                        {
                          label: "Last auth failure",
                          value:
                            readString(selectedOperations ?? {}, "last_auth_failure") ?? "none",
                        },
                        {
                          label: "Discord permission gap",
                          value:
                            readString(selectedDiscordOps ?? {}, "last_permission_failure") ??
                            "none",
                        },
                      ]}
                    />
                    {selectedHealthRefresh !== null ? (
                      <PrettyJsonBlock
                        value={selectedHealthRefresh}
                        revealSensitiveValues={app.revealSensitiveValues}
                      />
                    ) : null}
                  </InlineNotice>
                )}

                <AppForm onSubmit={(event) => void app.sendChannelTest(event)}>
                  <div className="workspace-form-grid">
                    <TextInputField
                      label="Text"
                      value={app.channelsTestText}
                      onChange={app.setChannelsTestText}
                    />
                    <TextInputField
                      label="Conversation ID"
                      value={app.channelsTestConversationId}
                      onChange={app.setChannelsTestConversationId}
                    />
                    <TextInputField
                      label="Sender ID"
                      value={app.channelsTestSenderId}
                      onChange={app.setChannelsTestSenderId}
                    />
                    <TextInputField
                      label="Sender display"
                      value={app.channelsTestSenderDisplay}
                      onChange={app.setChannelsTestSenderDisplay}
                    />
                  </div>
                  <div className="workspace-inline">
                    <CheckboxField
                      label="Simulate crash once"
                      checked={app.channelsTestCrashOnce}
                      onChange={app.setChannelsTestCrashOnce}
                    />
                    <CheckboxField
                      label="Direct message"
                      checked={app.channelsTestDirectMessage}
                      onChange={app.setChannelsTestDirectMessage}
                    />
                    <CheckboxField
                      label="Broadcast"
                      checked={app.channelsTestBroadcast}
                      onChange={app.setChannelsTestBroadcast}
                    />
                  </div>
                  <ActionButton type="submit" isDisabled={app.channelsBusy}>
                    {app.channelsBusy ? "Sending..." : "Send connector test"}
                  </ActionButton>
                </AppForm>

                {(app.channelsEvents.length > 0 || app.channelsDeadLetters.length > 0) && (
                  <WorkspaceSectionCard
                    title="Connector logs and dead letters"
                    description="Recent events and replay controls remain visible without leaving the connector workspace."
                    className="workspace-section-card--nested"
                  >
                    <div className="workspace-stack">
                      {deadLetterRows.length > 0 ? (
                        <EntityTable
                          ariaLabel="Connector dead letters"
                          columns={[
                            {
                              key: "id",
                              label: "Dead letter",
                              isRowHeader: true,
                              render: (row) => <strong>{row.deadLetterId}</strong>,
                            },
                            {
                              key: "actions",
                              label: "Actions",
                              align: "end",
                              render: (row) => (
                                <ActionCluster>
                                  <ActionButton
                                    variant="secondary"
                                    size="sm"
                                    onPress={() =>
                                      void app.replayChannelDeadLetter(row.deadLetterId)
                                    }
                                    isDisabled={app.channelsBusy}
                                  >
                                    Replay
                                  </ActionButton>
                                  <ActionButton
                                    variant="secondary"
                                    size="sm"
                                    onPress={() =>
                                      void app.discardChannelDeadLetter(row.deadLetterId)
                                    }
                                    isDisabled={app.channelsBusy}
                                  >
                                    Discard
                                  </ActionButton>
                                </ActionCluster>
                              ),
                            },
                          ]}
                          rows={deadLetterRows}
                          getRowId={(row) => String(row.deadLetterId)}
                          emptyTitle="No dead letters"
                          emptyDescription="Dead-letter actions will appear here when retries fail."
                        />
                      ) : null}
                      <PrettyJsonBlock
                        value={{
                          events: app.channelsEvents,
                          dead_letters: app.channelsDeadLetters,
                        }}
                        revealSensitiveValues={app.revealSensitiveValues}
                      />
                    </div>
                  </WorkspaceSectionCard>
                )}
              </div>
            </WorkspaceSectionCard>
          </section>
        </Tabs.Panel>

        <Tabs.Panel className="pt-4" id="messages">
          <section className="workspace-two-column">
            <WorkspaceSectionCard
              title="Message read and search"
              description="Inspect Discord history with bounded filters, then pivot directly into edit, delete, or reaction workflows."
            >
              <div className="workspace-stack">
                <div className="workspace-form-grid">
                  <TextInputField
                    label="Conversation ID"
                    value={app.channelMessageConversationId}
                    onChange={app.setChannelMessageConversationId}
                    placeholder="discord:channel:123 or DM conversation"
                  />
                  <TextInputField
                    label="Thread ID"
                    value={app.channelMessageThreadId}
                    onChange={app.setChannelMessageThreadId}
                    placeholder="Optional thread"
                  />
                </div>

                <WorkspaceSectionCard
                  title="Read history"
                  description="Fetch a single message, surrounding context, or bounded history without dumping the whole channel."
                  className="workspace-section-card--nested"
                >
                  <AppForm onSubmit={(event) => void app.readChannelMessages(event)}>
                    <div className="workspace-form-grid">
                      <TextInputField
                        label="Message ID"
                        value={app.channelMessageReadMessageId}
                        onChange={app.setChannelMessageReadMessageId}
                        placeholder="Exact message lookup"
                      />
                      <TextInputField
                        label="Before message"
                        value={app.channelMessageReadBeforeMessageId}
                        onChange={app.setChannelMessageReadBeforeMessageId}
                      />
                      <TextInputField
                        label="After message"
                        value={app.channelMessageReadAfterMessageId}
                        onChange={app.setChannelMessageReadAfterMessageId}
                      />
                      <TextInputField
                        label="Around message"
                        value={app.channelMessageReadAroundMessageId}
                        onChange={app.setChannelMessageReadAroundMessageId}
                      />
                      <TextInputField
                        label="Limit"
                        value={app.channelMessageReadLimit}
                        onChange={app.setChannelMessageReadLimit}
                        type="number"
                      />
                    </div>
                    <ActionButton type="submit" isDisabled={app.channelsBusy}>
                      {app.channelsBusy ? "Loading..." : "Read messages"}
                    </ActionButton>
                  </AppForm>
                </WorkspaceSectionCard>

                <WorkspaceSectionCard
                  title="Search"
                  description="Filter by text, author, attachments, and pagination cursor to keep the result set reviewable."
                  className="workspace-section-card--nested"
                >
                  <AppForm onSubmit={(event) => void app.searchChannelMessages(event)}>
                    <div className="workspace-form-grid">
                      <TextInputField
                        label="Query"
                        value={app.channelMessageSearchQuery}
                        onChange={app.setChannelMessageSearchQuery}
                      />
                      <TextInputField
                        label="Author ID"
                        value={app.channelMessageSearchAuthorId}
                        onChange={app.setChannelMessageSearchAuthorId}
                      />
                      <SelectField
                        label="Attachments"
                        value={app.channelMessageSearchHasAttachments}
                        onChange={app.setChannelMessageSearchHasAttachments}
                        options={[
                          { key: "any", label: "Any" },
                          { key: "with", label: "With attachments" },
                          { key: "without", label: "Without attachments" },
                        ]}
                      />
                      <TextInputField
                        label="Before message"
                        value={app.channelMessageSearchBeforeMessageId}
                        onChange={app.setChannelMessageSearchBeforeMessageId}
                      />
                      <TextInputField
                        label="Limit"
                        value={app.channelMessageSearchLimit}
                        onChange={app.setChannelMessageSearchLimit}
                        type="number"
                      />
                    </div>
                    <ActionButton type="submit" isDisabled={app.channelsBusy}>
                      {app.channelsBusy ? "Searching..." : "Search messages"}
                    </ActionButton>
                  </AppForm>
                </WorkspaceSectionCard>

                {messageCapabilities.length > 0 ? (
                  <InlineNotice title="Connector message capabilities" tone="default">
                    <EntityTable
                      ariaLabel="Connector message capabilities"
                      columns={[
                        {
                          key: "action",
                          label: "Action",
                          isRowHeader: true,
                          render: (row) => (
                            <strong>{readString(row, "action") ?? "unknown"}</strong>
                          ),
                        },
                        {
                          key: "support",
                          label: "Support",
                          render: (row) => (
                            <div className="workspace-inline">
                              <WorkspaceStatusChip
                                tone={readBool(row, "supported") ? "success" : "warning"}
                              >
                                {readBool(row, "supported") ? "supported" : "blocked"}
                              </WorkspaceStatusChip>
                              <WorkspaceStatusChip tone="default">
                                {readString(row, "approval_mode") ?? "n/a"}
                              </WorkspaceStatusChip>
                              <WorkspaceStatusChip tone="default">
                                {readString(row, "risk_level") ?? "n/a"}
                              </WorkspaceStatusChip>
                            </div>
                          ),
                        },
                        {
                          key: "permissions",
                          label: "Permissions",
                          render: (row) =>
                            arrayToText(row.required_permissions, "No explicit permissions"),
                        },
                      ]}
                      rows={messageCapabilities}
                      getRowId={(row, index) => readString(row, "action") ?? `action-${index}`}
                    />
                  </InlineNotice>
                ) : null}
              </div>
            </WorkspaceSectionCard>

            <WorkspaceSectionCard
              title="Results and mutations"
              description="Results stay visible with links and metadata, while mutation controls make it explicit whether the action is only a preview or was actually applied."
            >
              <div className="workspace-stack">
                <WorkspaceSectionCard
                  title="Latest results"
                  description="Use result rows to prefill the mutation form instead of copying Discord IDs by hand."
                  className="workspace-section-card--nested"
                >
                  {latestMessageRows.length === 0 ? (
                    <EmptyState
                      compact
                      title="No message results yet"
                      description="Run read or search to inspect history and prefill the mutation form."
                    />
                  ) : (
                    <EntityTable
                      ariaLabel="Discord message results"
                      columns={[
                        {
                          key: "message",
                          label: "Message",
                          isRowHeader: true,
                          render: (row) => (
                            <div className="workspace-stack">
                              <strong>{row.sender}</strong>
                              <span className="chat-muted">{row.body}</span>
                            </div>
                          ),
                        },
                        {
                          key: "meta",
                          label: "Metadata",
                          render: (row) => (
                            <div className="workspace-stack">
                              <span>{row.createdAt}</span>
                              <span className="chat-muted">
                                {row.attachments} attachments · {row.reactions}
                              </span>
                              <span className="chat-muted">
                                {row.threadId === "none"
                                  ? row.conversationId
                                  : `${row.conversationId} · ${row.threadId}`}
                              </span>
                              {row.link !== null ? (
                                <a href={row.link} target="_blank" rel="noreferrer">
                                  Open Discord message
                                </a>
                              ) : null}
                            </div>
                          ),
                        },
                        {
                          key: "actions",
                          label: "Actions",
                          align: "end",
                          render: (row) => (
                            <ActionButton
                              variant="secondary"
                              size="sm"
                              onPress={() => prefillMutationFromRow(row)}
                            >
                              Prefill mutation
                            </ActionButton>
                          ),
                        },
                      ]}
                      rows={latestMessageRows}
                      getRowId={(row) => row.id}
                    />
                  )}
                  {latestMessageCollection !== null ? (
                    <PrettyJsonBlock
                      value={latestMessageCollection}
                      revealSensitiveValues={app.revealSensitiveValues}
                    />
                  ) : null}
                </WorkspaceSectionCard>

                <WorkspaceSectionCard
                  title="Mutate message"
                  description="Edit, delete, and reaction changes share the same locator and approval context, so you can inspect and retry an approval-required action without rebuilding the request."
                  className="workspace-section-card--nested"
                >
                  <div className="workspace-form-grid">
                    <TextInputField
                      label="Message ID"
                      value={app.channelMessageMutationMessageId}
                      onChange={app.setChannelMessageMutationMessageId}
                    />
                    <TextInputField
                      label="Approval ID"
                      value={app.channelMessageMutationApprovalId}
                      onChange={app.setChannelMessageMutationApprovalId}
                      placeholder="Use returned approval ID to apply"
                    />
                    <TextInputField
                      label="Reaction emoji"
                      value={app.channelMessageMutationEmoji}
                      onChange={app.setChannelMessageMutationEmoji}
                    />
                    <TextInputField
                      label="Delete reason"
                      value={app.channelMessageMutationDeleteReason}
                      onChange={app.setChannelMessageMutationDeleteReason}
                    />
                  </div>
                  <TextAreaField
                    label="Edit body"
                    value={app.channelMessageMutationBody}
                    onChange={app.setChannelMessageMutationBody}
                    rows={5}
                  />
                  <ActionCluster>
                    <ActionButton
                      onPress={() => void app.editChannelMessage()}
                      isDisabled={app.channelsBusy}
                    >
                      Edit message
                    </ActionButton>
                    <ActionButton
                      variant="secondary"
                      onPress={() => void app.deleteChannelMessage()}
                      isDisabled={app.channelsBusy}
                    >
                      Delete message
                    </ActionButton>
                    <ActionButton
                      variant="secondary"
                      onPress={() => void app.addChannelMessageReaction()}
                      isDisabled={app.channelsBusy}
                    >
                      Add reaction
                    </ActionButton>
                    <ActionButton
                      variant="secondary"
                      onPress={() => void app.removeChannelMessageReaction()}
                      isDisabled={app.channelsBusy}
                    >
                      Remove reaction
                    </ActionButton>
                  </ActionCluster>
                </WorkspaceSectionCard>

                {messageMutationResult !== null ? (
                  <WorkspaceSectionCard
                    title="Latest mutation outcome"
                    description="Preview and applied results are intentionally separated so the operator can see whether the platform changed."
                    className="workspace-section-card--nested"
                  >
                    {readBool(messageMutationResult, "approval_required") ? (
                      <InlineNotice title="Approval required" tone="warning">
                        <p>
                          No platform mutation has been applied yet. This response is a preview plus
                          approval artifact for a follow-up confirmation.
                        </p>
                        <KeyValueList
                          items={[
                            {
                              label: "Approval ID",
                              value:
                                readString(messageMutationApproval ?? {}, "approval_id") ?? "n/a",
                            },
                            {
                              label: "Policy action",
                              value: readString(messageMutationPolicy ?? {}, "action") ?? "n/a",
                            },
                            {
                              label: "Policy reason",
                              value: readString(messageMutationPolicy ?? {}, "reason") ?? "n/a",
                            },
                          ]}
                        />
                      </InlineNotice>
                    ) : (
                      <InlineNotice title="Mutation applied" tone="success">
                        <p>
                          The connector returned an applied mutation result. Review the result below
                          before continuing with follow-up operations.
                        </p>
                        <KeyValueList
                          items={[
                            {
                              label: "Status",
                              value:
                                readString(messageMutationAppliedResult ?? {}, "status") ?? "n/a",
                            },
                            {
                              label: "Reason",
                              value:
                                readString(messageMutationAppliedResult ?? {}, "reason") ?? "none",
                            },
                          ]}
                        />
                      </InlineNotice>
                    )}
                    {messageMutationPreview !== null ? (
                      <PrettyJsonBlock
                        value={messageMutationPreview}
                        revealSensitiveValues={app.revealSensitiveValues}
                      />
                    ) : null}
                    <PrettyJsonBlock
                      value={messageMutationResult}
                      revealSensitiveValues={app.revealSensitiveValues}
                    />
                  </WorkspaceSectionCard>
                ) : null}
              </div>
            </WorkspaceSectionCard>
          </section>
        </Tabs.Panel>

        <Tabs.Panel className="pt-4" id="router">
          <section className="workspace-two-column">
            <WorkspaceSectionCard
              title="Router rules and warnings"
              description="Preview route acceptance and keep warnings visible before broadening ingress policy."
            >
              <div className="workspace-stack">
                <div className="workspace-inline">
                  <WorkspaceStatusChip
                    tone={app.channelRouterWarnings.length > 0 ? "warning" : "success"}
                  >
                    {app.channelRouterWarnings.length} warnings
                  </WorkspaceStatusChip>
                  <WorkspaceStatusChip tone="default">
                    Config {app.channelRouterConfigHash || "n/a"}
                  </WorkspaceStatusChip>
                </div>
                {app.channelRouterWarnings.length === 0 ? (
                  <EmptyState
                    compact
                    title="No router warnings"
                    description="No router warnings published for the current configuration."
                  />
                ) : (
                  <InlineNotice title="Current warnings" tone="warning">
                    <ul className="console-compact-list">
                      {app.channelRouterWarnings.map((warning) => (
                        <li key={warning}>{warning}</li>
                      ))}
                    </ul>
                  </InlineNotice>
                )}
                {app.channelRouterRules === null ? (
                  <EmptyState
                    compact
                    title="No router rules"
                    description="Refresh router state to inspect the active routing rules."
                  />
                ) : (
                  <PrettyJsonBlock
                    value={app.channelRouterRules}
                    revealSensitiveValues={app.revealSensitiveValues}
                  />
                )}
              </div>
            </WorkspaceSectionCard>

            <WorkspaceSectionCard
              title="Route preview"
              description="Use a controlled preview form to verify allowlist, DM, and broadcast behavior before a live message arrives."
            >
              <AppForm onSubmit={(event) => void app.previewChannelRouter(event)}>
                <div className="workspace-form-grid">
                  <TextInputField
                    label="Channel"
                    value={app.channelRouterPreviewChannel}
                    onChange={app.setChannelRouterPreviewChannel}
                  />
                  <TextInputField
                    label="Text"
                    value={app.channelRouterPreviewText}
                    onChange={app.setChannelRouterPreviewText}
                  />
                  <TextInputField
                    label="Conversation ID"
                    value={app.channelRouterPreviewConversationId}
                    onChange={app.setChannelRouterPreviewConversationId}
                  />
                  <TextInputField
                    label="Sender identity"
                    value={app.channelRouterPreviewSenderIdentity}
                    onChange={app.setChannelRouterPreviewSenderIdentity}
                  />
                  <TextInputField
                    label="Sender display"
                    value={app.channelRouterPreviewSenderDisplay}
                    onChange={app.setChannelRouterPreviewSenderDisplay}
                  />
                  <TextInputField
                    label="Max payload bytes"
                    value={app.channelRouterPreviewMaxPayloadBytes}
                    onChange={app.setChannelRouterPreviewMaxPayloadBytes}
                  />
                </div>
                <div className="workspace-inline">
                  <CheckboxField
                    label="Sender verified"
                    checked={app.channelRouterPreviewSenderVerified}
                    onChange={app.setChannelRouterPreviewSenderVerified}
                  />
                  <CheckboxField
                    label="Direct message"
                    checked={app.channelRouterPreviewIsDirectMessage}
                    onChange={app.setChannelRouterPreviewIsDirectMessage}
                  />
                  <CheckboxField
                    label="Requested broadcast"
                    checked={app.channelRouterPreviewRequestedBroadcast}
                    onChange={app.setChannelRouterPreviewRequestedBroadcast}
                  />
                </div>
                <ActionButton type="submit" isDisabled={app.channelsBusy}>
                  {app.channelsBusy ? "Previewing..." : "Preview route"}
                </ActionButton>
              </AppForm>
              {app.channelRouterPreviewResult !== null ? (
                <PrettyJsonBlock
                  value={app.channelRouterPreviewResult}
                  revealSensitiveValues={app.revealSensitiveValues}
                />
              ) : null}
            </WorkspaceSectionCard>

            <WorkspaceSectionCard
              title="Pairings"
              description="Mint pairing codes and inspect current pairings without leaving the routing mode."
            >
              <div className="workspace-stack">
                <AppForm onSubmit={(event) => void app.mintChannelRouterPairingCode(event)}>
                  <div className="workspace-form-grid">
                    <TextInputField
                      label="Filter channel"
                      value={app.channelRouterPairingsFilterChannel}
                      onChange={app.setChannelRouterPairingsFilterChannel}
                    />
                    <TextInputField
                      label="Mint channel"
                      value={app.channelRouterMintChannel}
                      onChange={app.setChannelRouterMintChannel}
                    />
                    <TextInputField
                      label="Issued by"
                      value={app.channelRouterMintIssuedBy}
                      onChange={app.setChannelRouterMintIssuedBy}
                    />
                    <TextInputField
                      label="TTL ms"
                      value={app.channelRouterMintTtlMs}
                      onChange={app.setChannelRouterMintTtlMs}
                    />
                  </div>
                  <ActionCluster>
                    <ActionButton
                      type="button"
                      variant="secondary"
                      onPress={() => void app.refreshChannelRouterPairings()}
                      isDisabled={app.channelsBusy}
                    >
                      Refresh pairings
                    </ActionButton>
                    <ActionButton type="submit" isDisabled={app.channelsBusy}>
                      {app.channelsBusy ? "Minting..." : "Mint pairing code"}
                    </ActionButton>
                  </ActionCluster>
                </AppForm>

                {app.channelRouterMintResult !== null ? (
                  <PrettyJsonBlock
                    value={app.channelRouterMintResult}
                    revealSensitiveValues={app.revealSensitiveValues}
                  />
                ) : null}

                <EntityTable
                  ariaLabel="Channel router pairings"
                  columns={[
                    {
                      key: "channel",
                      label: "Channel",
                      isRowHeader: true,
                      render: (row) => <strong>{row.channel}</strong>,
                    },
                    {
                      key: "principal",
                      label: "Principal",
                      render: (row) => row.principal,
                    },
                    {
                      key: "status",
                      label: "Status",
                      render: (row) => (
                        <WorkspaceStatusChip tone={row.status === "active" ? "success" : "default"}>
                          {row.status}
                        </WorkspaceStatusChip>
                      ),
                    },
                  ]}
                  rows={pairingRows}
                  getRowId={(row) => row.id}
                  emptyTitle="No pairings loaded"
                  emptyDescription="Mint a pairing code or refresh pairings to inspect current associations."
                />
              </div>
            </WorkspaceSectionCard>
          </section>
        </Tabs.Panel>

        <Tabs.Panel className="pt-4" id="discord">
          <section className="workspace-two-column">
            <WorkspaceSectionCard
              title="Discord onboarding"
              description="Probe, apply, and verify onboarding from the dashboard instead of the old desktop flow."
            >
              <DiscordOnboardingPanel
                discord={discord}
                revealSensitiveValues={app.revealSensitiveValues}
              />
            </WorkspaceSectionCard>

            <WorkspaceSectionCard
              title="Discord connector actions"
              description="Verification send, targeted health checks, and connector-specific actions stay alongside the onboarding flow."
            >
              <div className="workspace-stack">
                <TextInputField
                  label="Verify channel"
                  value={discord.discordWizardVerifyChannelId}
                  onChange={discord.setDiscordWizardVerifyChannelId}
                />
                <ActionCluster>
                  <ActionButton
                    variant="secondary"
                    onPress={() => void discord.refreshHealth()}
                    isDisabled={app.channelsBusy}
                  >
                    Refresh health
                  </ActionButton>
                  <ActionButton
                    variant="secondary"
                    onPress={() => void app.refreshChannels()}
                    isDisabled={app.channelsBusy}
                  >
                    Refresh connector state
                  </ActionButton>
                </ActionCluster>
                <DiscordConnectorActionsPanel
                  discord={discord}
                  selectedConnectorKind={selectedConnectorKind}
                />
              </div>
            </WorkspaceSectionCard>
          </section>
        </Tabs.Panel>
      </Tabs>
    </main>
  );
}

function displayScalar(value: unknown, fallback = "n/a"): string {
  if (typeof value === "string") {
    return value.trim().length > 0 ? value : fallback;
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  return fallback;
}

function asJsonObject(value: unknown): JsonObject {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as JsonObject)
    : {};
}

function asMaybeJsonObject(value: unknown): JsonObject | null {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as JsonObject)
    : null;
}

function arrayToText(value: unknown, fallback: string): string {
  if (!Array.isArray(value)) {
    return fallback;
  }
  const entries = value.filter(
    (entry): entry is string => typeof entry === "string" && entry.length > 0,
  );
  return entries.length > 0 ? entries.join(", ") : fallback;
}

function toMessageRow(value: unknown, index: number): MessageRow | null {
  const record = asMaybeJsonObject(value);
  const locator = record !== null ? readObject(record, "locator") : null;
  const messageId = readString(locator ?? {}, "message_id");
  const conversationId = readString(locator ?? {}, "conversation_id");
  if (record === null || messageId === null || conversationId === null) {
    return null;
  }
  const sender =
    readString(record, "sender_display") ?? readString(record, "sender_id") ?? "unknown sender";
  const body = compactCopy(readString(record, "body") ?? "(empty message)");
  const reactions = Array.isArray(record.reactions)
    ? record.reactions
        .map((entry) => {
          const reaction = asMaybeJsonObject(entry);
          if (reaction === null) {
            return null;
          }
          const emoji = readString(reaction, "emoji");
          if (emoji === null) {
            return null;
          }
          return `${emoji} ${readNumber(reaction, "count") ?? 0}`;
        })
        .filter((entry): entry is string => entry !== null)
        .join(", ")
    : "";
  return {
    id: `${conversationId}:${messageId}:${index}`,
    conversationId,
    threadId: readString(locator ?? {}, "thread_id") ?? "none",
    sender,
    body,
    createdAt: displayScalar(record.created_at_unix_ms, "n/a"),
    attachments: Array.isArray(record.attachments) ? record.attachments.length : 0,
    reactions: reactions.length > 0 ? reactions : "no reactions",
    link: readString(record, "link"),
    raw: record,
  };
}

function compactCopy(value: string): string {
  const collapsed = value.replace(/\s+/g, " ").trim();
  if (collapsed.length <= 160) {
    return collapsed;
  }
  return `${collapsed.slice(0, 157)}...`;
}
