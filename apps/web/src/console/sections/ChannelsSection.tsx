import { Button } from "@heroui/react";
import { useState } from "react";

import { DiscordOnboardingPanel } from "../../features/channels/connectors/discord/components/DiscordOnboardingPanel";
import { DiscordConnectorActionsPanel } from "../../features/channels/connectors/discord/components/DiscordConnectorActionsPanel";
import { WorkspaceMetricCard, WorkspacePageHeader, WorkspaceSectionCard, WorkspaceStatusChip } from "../components/workspace/WorkspaceChrome";
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
  const enabledConnectors = app.channelsConnectors.filter((connector) => readBool(connector, "enabled"));

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
            <WorkspaceStatusChip tone={app.channelRouterWarnings.length > 0 ? "warning" : "success"}>
              {app.channelRouterWarnings.length} router warnings
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={app.discordWizardBusy ? "warning" : "default"}>
              {app.discordWizardBusy ? "Discord setup busy" : "Discord setup ready"}
            </WorkspaceStatusChip>
          </>
        }
        actions={
          <Button
            variant="secondary"
            onPress={() => void app.refreshChannels()}
            isDisabled={app.channelsBusy}
          >
            {app.channelsBusy ? "Refreshing..." : "Refresh channels"}
          </Button>
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
          detail={app.channelRouterMintResult === null ? "No fresh pairing code minted." : "Latest mint result available."}
          tone={app.channelRouterWarnings.length > 0 ? "warning" : "default"}
        />
        <WorkspaceMetricCard
          label="Discord onboarding"
          value={app.discordWizardApply === null ? "Awaiting action" : readString(app.discordWizardApply, "result") ?? "Result ready"}
          detail={app.discordWizardPreflight === null ? "Run preflight to inspect requirements." : "Preflight results are available."}
          tone={app.discordWizardApply === null ? "default" : "success"}
        />
      </section>

      <section className="workspace-tab-row" aria-label="Channels workspace modes">
        <button
          type="button"
          className={`workspace-tab${activeTab === "connectors" ? " is-active" : ""}`}
          onClick={() => setActiveTab("connectors")}
        >
          Connectors
        </button>
        <button
          type="button"
          className={`workspace-tab${activeTab === "router" ? " is-active" : ""}`}
          onClick={() => setActiveTab("router")}
        >
          Router
        </button>
        <button
          type="button"
          className={`workspace-tab${activeTab === "discord" ? " is-active" : ""}`}
          onClick={() => setActiveTab("discord")}
        >
          Discord setup
        </button>
      </section>

      {activeTab === "connectors" && (
        <section className="workspace-two-column">
          <WorkspaceSectionCard
            title="Connector inventory"
            description="Enable, disable, and select the connector you want to inspect before using test or recovery actions."
          >
            {app.channelsConnectors.length === 0 ? (
              <p className="chat-muted">No channel connectors configured.</p>
            ) : (
              <div className="workspace-list">
                {app.channelsConnectors.map((connector) => {
                  const connectorId = readString(connector, "connector_id") ?? "unknown";
                  const enabled = readBool(connector, "enabled");
                  const isSelected = connectorId === app.channelsSelectedConnectorId;
                  return (
                    <article key={connectorId} className={`workspace-list-item workspace-list-item--job${isSelected ? " is-active" : ""}`}>
                      <button
                        type="button"
                        className="workspace-list-button workspace-list-button--flat"
                        onClick={() => void app.selectChannelConnector(connectorId)}
                      >
                        <div>
                          <strong>{connectorId}</strong>
                          <p className="chat-muted">
                            {readString(connector, "kind") ?? "unknown kind"} · {channelConnectorAvailability(connector)}
                          </p>
                        </div>
                        <div className="workspace-inline">
                          <WorkspaceStatusChip tone={enabled ? "success" : "default"}>
                            {enabled ? "enabled" : "disabled"}
                          </WorkspaceStatusChip>
                          <WorkspaceStatusChip tone="default">
                            {readString(connector, "readiness") ?? "readiness n/a"}
                          </WorkspaceStatusChip>
                        </div>
                      </button>
                      <div className="console-inline-actions">
                        <Button
                          variant="secondary"
                          size="sm"
                          onPress={() => void app.selectChannelConnector(connectorId)}
                        >
                          Select
                        </Button>
                        <Button
                          size="sm"
                          onPress={() => void app.toggleConnector(connector, !enabled)}
                          isDisabled={app.channelsBusy}
                        >
                          {enabled ? "Disable" : "Enable"}
                        </Button>
                      </div>
                    </article>
                  );
                })}
              </div>
            )}
          </WorkspaceSectionCard>

          <WorkspaceSectionCard
            title="Selected connector"
            description="Connector status, recovery controls, logs, and direct test actions stay in one focused column."
          >
            <div className="workspace-stack">
              {app.channelsSelectedStatus === null ? (
                <p className="chat-muted">Select a connector to inspect its current runtime state.</p>
              ) : (
                <PrettyJsonBlock
                  value={app.channelsSelectedStatus}
                  revealSensitiveValues={app.revealSensitiveValues}
                />
              )}

              <div className="workspace-form-grid">
                <label>
                  Selected connector
                  <input value={app.channelsSelectedConnectorId} readOnly />
                </label>
                <label>
                  Log limit
                  <input
                    value={app.channelsLogsLimit}
                    onChange={(event) => app.setChannelsLogsLimit(event.target.value)}
                  />
                </label>
              </div>

              <div className="console-inline-actions">
                <Button
                  variant="secondary"
                  onPress={() => void app.pauseChannelQueue()}
                  isDisabled={app.channelsBusy}
                >
                  Pause queue
                </Button>
                <Button
                  variant="secondary"
                  onPress={() => void app.resumeChannelQueue()}
                  isDisabled={app.channelsBusy}
                >
                  Resume queue
                </Button>
                <Button
                  variant="secondary"
                  onPress={() => void app.drainChannelQueue()}
                  isDisabled={app.channelsBusy}
                >
                  Drain queue
                </Button>
                <Button
                  variant="secondary"
                  onPress={() => void app.refreshChannelHealth()}
                  isDisabled={app.channelsBusy}
                >
                  Refresh health
                </Button>
              </div>

              {(selectedQueue !== null || selectedHealthRefresh !== null) && (
                <div className="workspace-callout">
                  <p className="console-label">Recovery telemetry</p>
                  <dl className="workspace-key-value-grid">
                    <div>
                      <dt>Queue paused</dt>
                      <dd>{readBool(selectedQueue ?? {}, "paused") ? "yes" : "no"}</dd>
                    </div>
                    <div>
                      <dt>Dead letters</dt>
                      <dd>{displayScalar(selectedQueue?.dead_letters, "0")}</dd>
                    </div>
                    <div>
                      <dt>Saturation</dt>
                      <dd>{readString(selectedSaturation ?? {}, "state") ?? "n/a"}</dd>
                    </div>
                    <div>
                      <dt>Last auth failure</dt>
                      <dd>{readString(selectedOperations ?? {}, "last_auth_failure") ?? "none"}</dd>
                    </div>
                    <div>
                      <dt>Discord permission gap</dt>
                      <dd>{readString(selectedDiscordOps ?? {}, "last_permission_failure") ?? "none"}</dd>
                    </div>
                  </dl>
                  {selectedHealthRefresh !== null && (
                    <PrettyJsonBlock
                      value={selectedHealthRefresh}
                      revealSensitiveValues={app.revealSensitiveValues}
                    />
                  )}
                </div>
              )}

              <form className="workspace-form" onSubmit={(event) => void app.sendChannelTest(event)}>
                <div className="workspace-form-grid">
                  <label>
                    Text
                    <input value={app.channelsTestText} onChange={(event) => app.setChannelsTestText(event.target.value)} />
                  </label>
                  <label>
                    Conversation ID
                    <input
                      value={app.channelsTestConversationId}
                      onChange={(event) => app.setChannelsTestConversationId(event.target.value)}
                    />
                  </label>
                  <label>
                    Sender ID
                    <input value={app.channelsTestSenderId} onChange={(event) => app.setChannelsTestSenderId(event.target.value)} />
                  </label>
                  <label>
                    Sender display
                    <input
                      value={app.channelsTestSenderDisplay}
                      onChange={(event) => app.setChannelsTestSenderDisplay(event.target.value)}
                    />
                  </label>
                </div>
                <div className="workspace-inline">
                  <label className="console-checkbox-inline">
                    <input
                      type="checkbox"
                      checked={app.channelsTestCrashOnce}
                      onChange={(event) => app.setChannelsTestCrashOnce(event.target.checked)}
                    />
                    Simulate crash once
                  </label>
                  <label className="console-checkbox-inline">
                    <input
                      type="checkbox"
                      checked={app.channelsTestDirectMessage}
                      onChange={(event) => app.setChannelsTestDirectMessage(event.target.checked)}
                    />
                    Direct message
                  </label>
                  <label className="console-checkbox-inline">
                    <input
                      type="checkbox"
                      checked={app.channelsTestBroadcast}
                      onChange={(event) => app.setChannelsTestBroadcast(event.target.checked)}
                    />
                    Broadcast
                  </label>
                </div>
                <Button type="submit" isDisabled={app.channelsBusy}>
                  {app.channelsBusy ? "Sending..." : "Send connector test"}
                </Button>
              </form>

              {(app.channelsEvents.length > 0 || app.channelsDeadLetters.length > 0) && (
                <WorkspaceSectionCard
                  title="Connector logs and dead letters"
                  description="Recent events and replay controls remain visible without leaving the connector workspace."
                  className="workspace-section-card--nested"
                >
                  {app.channelsDeadLetters.length > 0 && (
                    <div className="workspace-list">
                      {app.channelsDeadLetters.map((deadLetter) => {
                        const deadLetterId =
                          typeof deadLetter.dead_letter_id === "number"
                            ? deadLetter.dead_letter_id
                            : Number(deadLetter.dead_letter_id ?? Number.NaN);
                        if (!Number.isFinite(deadLetterId)) {
                          return null;
                        }
                        return (
                          <article key={deadLetterId} className="workspace-list-item">
                            <strong>Dead letter {deadLetterId}</strong>
                            <div className="console-inline-actions">
                              <Button
                                variant="secondary"
                                size="sm"
                                onPress={() => void app.replayChannelDeadLetter(deadLetterId)}
                                isDisabled={app.channelsBusy}
                              >
                                Replay
                              </Button>
                              <Button
                                variant="secondary"
                                size="sm"
                                onPress={() => void app.discardChannelDeadLetter(deadLetterId)}
                                isDisabled={app.channelsBusy}
                              >
                                Discard
                              </Button>
                            </div>
                          </article>
                        );
                      })}
                    </div>
                  )}
                  <PrettyJsonBlock
                    value={{ events: app.channelsEvents, dead_letters: app.channelsDeadLetters }}
                    revealSensitiveValues={app.revealSensitiveValues}
                  />
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
                <WorkspaceStatusChip tone={app.channelRouterWarnings.length > 0 ? "warning" : "success"}>
                  {app.channelRouterWarnings.length} warnings
                </WorkspaceStatusChip>
                <WorkspaceStatusChip tone="default">
                  Config {app.channelRouterConfigHash || "n/a"}
                </WorkspaceStatusChip>
              </div>
              {app.channelRouterWarnings.length === 0 ? (
                <p className="chat-muted">No router warnings published.</p>
              ) : (
                <ul className="console-compact-list">
                  {app.channelRouterWarnings.map((warning) => (
                    <li key={warning}>{warning}</li>
                  ))}
                </ul>
              )}
              {app.channelRouterRules === null ? (
                <p className="chat-muted">No router rules loaded.</p>
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
            <form className="workspace-form" onSubmit={(event) => void app.previewChannelRouter(event)}>
              <div className="workspace-form-grid">
                <label>
                  Channel
                  <input value={app.channelRouterPreviewChannel} onChange={(event) => app.setChannelRouterPreviewChannel(event.target.value)} />
                </label>
                <label>
                  Text
                  <input value={app.channelRouterPreviewText} onChange={(event) => app.setChannelRouterPreviewText(event.target.value)} />
                </label>
                <label>
                  Conversation ID
                  <input
                    value={app.channelRouterPreviewConversationId}
                    onChange={(event) => app.setChannelRouterPreviewConversationId(event.target.value)}
                  />
                </label>
                <label>
                  Sender identity
                  <input
                    value={app.channelRouterPreviewSenderIdentity}
                    onChange={(event) => app.setChannelRouterPreviewSenderIdentity(event.target.value)}
                  />
                </label>
                <label>
                  Sender display
                  <input
                    value={app.channelRouterPreviewSenderDisplay}
                    onChange={(event) => app.setChannelRouterPreviewSenderDisplay(event.target.value)}
                  />
                </label>
                <label>
                  Max payload bytes
                  <input
                    value={app.channelRouterPreviewMaxPayloadBytes}
                    onChange={(event) => app.setChannelRouterPreviewMaxPayloadBytes(event.target.value)}
                  />
                </label>
              </div>
              <div className="workspace-inline">
                <label className="console-checkbox-inline">
                  <input
                    type="checkbox"
                    checked={app.channelRouterPreviewSenderVerified}
                    onChange={(event) => app.setChannelRouterPreviewSenderVerified(event.target.checked)}
                  />
                  Sender verified
                </label>
                <label className="console-checkbox-inline">
                  <input
                    type="checkbox"
                    checked={app.channelRouterPreviewIsDirectMessage}
                    onChange={(event) => app.setChannelRouterPreviewIsDirectMessage(event.target.checked)}
                  />
                  Direct message
                </label>
                <label className="console-checkbox-inline">
                  <input
                    type="checkbox"
                    checked={app.channelRouterPreviewRequestedBroadcast}
                    onChange={(event) => app.setChannelRouterPreviewRequestedBroadcast(event.target.checked)}
                  />
                  Requested broadcast
                </label>
              </div>
              <Button type="submit" isDisabled={app.channelsBusy}>
                {app.channelsBusy ? "Previewing..." : "Preview route"}
              </Button>
            </form>
            {app.channelRouterPreviewResult !== null && (
              <PrettyJsonBlock
                value={app.channelRouterPreviewResult}
                revealSensitiveValues={app.revealSensitiveValues}
              />
            )}
          </WorkspaceSectionCard>

          <WorkspaceSectionCard
            title="Pairings"
            description="Mint pairing codes and inspect current pairings without leaving the routing mode."
          >
            <form className="workspace-form" onSubmit={(event) => void app.mintChannelRouterPairingCode(event)}>
              <div className="workspace-form-grid">
                <label>
                  Filter channel
                  <input
                    value={app.channelRouterPairingsFilterChannel}
                    onChange={(event) => app.setChannelRouterPairingsFilterChannel(event.target.value)}
                  />
                </label>
                <label>
                  Mint channel
                  <input
                    value={app.channelRouterMintChannel}
                    onChange={(event) => app.setChannelRouterMintChannel(event.target.value)}
                  />
                </label>
                <label>
                  Issued by
                  <input
                    value={app.channelRouterMintIssuedBy}
                    onChange={(event) => app.setChannelRouterMintIssuedBy(event.target.value)}
                  />
                </label>
                <label>
                  TTL ms
                  <input
                    value={app.channelRouterMintTtlMs}
                    onChange={(event) => app.setChannelRouterMintTtlMs(event.target.value)}
                  />
                </label>
              </div>
              <div className="console-inline-actions">
                <Button
                  variant="secondary"
                  onPress={() => void app.refreshChannelRouterPairings()}
                  isDisabled={app.channelsBusy}
                >
                  Refresh pairings
                </Button>
                <Button type="submit" isDisabled={app.channelsBusy}>
                  {app.channelsBusy ? "Minting..." : "Mint pairing code"}
                </Button>
              </div>
            </form>

            {app.channelRouterMintResult !== null && (
              <PrettyJsonBlock
                value={app.channelRouterMintResult}
                revealSensitiveValues={app.revealSensitiveValues}
              />
            )}
            {app.channelRouterPairings.length === 0 ? (
              <p className="chat-muted">No pairings loaded.</p>
            ) : (
              <PrettyJsonBlock
                value={app.channelRouterPairings}
                revealSensitiveValues={app.revealSensitiveValues}
              />
            )}
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
              <div className="workspace-form-grid">
                <label>
                  Verify channel
                  <input
                    value={app.discordWizardVerifyChannelId}
                    onChange={(event) => app.setDiscordWizardVerifyChannelId(event.target.value)}
                  />
                </label>
              </div>
              <div className="console-inline-actions">
                <Button
                  variant="secondary"
                  onPress={() => void app.refreshChannelHealth()}
                  isDisabled={app.channelsBusy}
                >
                  Refresh health
                </Button>
                <Button
                  variant="secondary"
                  onPress={() => void app.refreshChannels()}
                  isDisabled={app.channelsBusy}
                >
                  Refresh connector state
                </Button>
              </div>
              <DiscordConnectorActionsPanel app={app} selectedConnectorKind={selectedConnectorKind} />
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
