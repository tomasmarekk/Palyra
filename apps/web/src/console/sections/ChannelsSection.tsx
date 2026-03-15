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
  TextInputField
} from "../components/ui";
import {
  WorkspaceMetricCard,
  WorkspacePageHeader,
  WorkspaceSectionCard,
  WorkspaceStatusChip
} from "../components/workspace/WorkspaceChrome";
import {
  PrettyJsonBlock,
  channelConnectorAvailability,
  readBool,
  readObject,
  readString,
  type JsonObject
} from "../shared";
import type { ConsoleAppState } from "../useConsoleAppState";

type ChannelsTab = "connectors" | "router" | "discord";

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

export function ChannelsSection({ app }: { app: ConsoleAppState }) {
  const [activeTab, setActiveTab] = useState<ChannelsTab>("connectors");
  const selectedStatusPayload: JsonObject = app.channelsSelectedStatus ?? {};
  const selectedConnector =
    readObject(selectedStatusPayload, "connector") ?? selectedStatusPayload;
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
    readBool(connector, "enabled")
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
          isSelected: connectorId === app.channelsSelectedConnectorId
        };
      }),
    [app.channelsConnectors, app.channelsSelectedConnectorId]
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
    [app.channelsDeadLetters]
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
          status: readString(record, "status") ?? "unknown"
        };
      }),
    [app.channelRouterPairings]
  );

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
            <WorkspaceStatusChip tone={app.discordWizardBusy ? "warning" : "default"}>
              {app.discordWizardBusy ? "Discord setup busy" : "Discord setup ready"}
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
              : readString(app.channelsConnectors[0], "connector_id") ?? "Connector available"
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
            app.discordWizardApply === null
              ? "Awaiting action"
              : readString(app.discordWizardApply, "result") ?? "Result ready"
          }
          detail={
            app.discordWizardPreflight === null
              ? "Run preflight to inspect requirements."
              : "Preflight results are available."
          }
          tone={app.discordWizardApply === null ? "default" : "success"}
        />
      </section>

      <ActionCluster className="workspace-tab-row" aria-label="Channels workspace modes">
        <ActionButton
          variant={activeTab === "connectors" ? "primary" : "ghost"}
          onPress={() => setActiveTab("connectors")}
        >
          Connectors
        </ActionButton>
        <ActionButton
          variant={activeTab === "router" ? "primary" : "ghost"}
          onPress={() => setActiveTab("router")}
        >
          Router
        </ActionButton>
        <ActionButton
          variant={activeTab === "discord" ? "primary" : "ghost"}
          onPress={() => setActiveTab("discord")}
        >
          Discord setup
        </ActionButton>
      </ActionCluster>

      {activeTab === "connectors" && (
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
                  )
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
                  )
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
                  )
                }
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
                  onPress={() => void app.refreshChannelHealth()}
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
                        value: readBool(selectedQueue ?? {}, "paused") ? "yes" : "no"
                      },
                      {
                        label: "Dead letters",
                        value: displayScalar(selectedQueue?.dead_letters, "0")
                      },
                      {
                        label: "Saturation",
                        value: readString(selectedSaturation ?? {}, "state") ?? "n/a"
                      },
                      {
                        label: "Last auth failure",
                        value: readString(selectedOperations ?? {}, "last_auth_failure") ?? "none"
                      },
                      {
                        label: "Discord permission gap",
                        value:
                          readString(selectedDiscordOps ?? {}, "last_permission_failure") ??
                          "none"
                      }
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
                            render: (row) => <strong>{row.deadLetterId}</strong>
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
                                  onPress={() => void app.replayChannelDeadLetter(row.deadLetterId)}
                                  isDisabled={app.channelsBusy}
                                >
                                  Replay
                                </ActionButton>
                                <ActionButton
                                  variant="secondary"
                                  size="sm"
                                  onPress={() => void app.discardChannelDeadLetter(row.deadLetterId)}
                                  isDisabled={app.channelsBusy}
                                >
                                  Discard
                                </ActionButton>
                              </ActionCluster>
                            )
                          }
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
                        dead_letters: app.channelsDeadLetters
                      }}
                      revealSensitiveValues={app.revealSensitiveValues}
                    />
                  </div>
                </WorkspaceSectionCard>
              )}
            </div>
          </WorkspaceSectionCard>
        </section>
      )}

      {activeTab === "router" && (
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
                    render: (row) => <strong>{row.channel}</strong>
                  },
                  {
                    key: "principal",
                    label: "Principal",
                    render: (row) => row.principal
                  },
                  {
                    key: "status",
                    label: "Status",
                    render: (row) => (
                      <WorkspaceStatusChip tone={row.status === "active" ? "success" : "default"}>
                        {row.status}
                      </WorkspaceStatusChip>
                    )
                  }
                ]}
                rows={pairingRows}
                getRowId={(row) => row.id}
                emptyTitle="No pairings loaded"
                emptyDescription="Mint a pairing code or refresh pairings to inspect current associations."
              />
            </div>
          </WorkspaceSectionCard>
        </section>
      )}

      {activeTab === "discord" && (
        <section className="workspace-two-column">
          <WorkspaceSectionCard
            title="Discord onboarding"
            description="Probe, apply, and verify onboarding from the dashboard instead of the old desktop flow."
          >
            <DiscordOnboardingPanel app={app} />
          </WorkspaceSectionCard>

          <WorkspaceSectionCard
            title="Discord connector actions"
            description="Verification send, targeted health checks, and connector-specific actions stay alongside the onboarding flow."
          >
            <div className="workspace-stack">
              <TextInputField
                label="Verify channel"
                value={app.discordWizardVerifyChannelId}
                onChange={app.setDiscordWizardVerifyChannelId}
              />
              <ActionCluster>
                <ActionButton
                  variant="secondary"
                  onPress={() => void app.refreshChannelHealth()}
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
                app={app}
                selectedConnectorKind={selectedConnectorKind}
              />
            </div>
          </WorkspaceSectionCard>
        </section>
      )}
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
