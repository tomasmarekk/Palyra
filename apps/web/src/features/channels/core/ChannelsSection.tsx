import { Button } from "@heroui/react";
import { useState } from "react";

import { WorkspaceMetricCard, WorkspacePageHeader, WorkspaceSectionCard, WorkspaceStatusChip } from "../../../console/components/workspace/WorkspaceChrome";
import {
  channelConnectorAvailability,
  readBool,
  readObject,
  readString,
  toPrettyJson,
  type JsonObject,
} from "../../../console/shared";
import type { ConsoleAppState } from "../../../console/useConsoleAppState";
import { DiscordConnectorActionsPanel } from "../connectors/discord/components/DiscordConnectorActionsPanel";
import { DiscordOnboardingPanel } from "../connectors/discord/components/DiscordOnboardingPanel";

type ChannelsTab = "connectors" | "router" | "discord";

function displayScalar(value: unknown, fallback = "n/a"): string {
  if (typeof value === "string") {
    return value.trim().length > 0 ? value : fallback;
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  return fallback;
}

export function ChannelsSection({ app }: { app: ConsoleAppState }) {
  const [activeTab, setActiveTab] = useState<ChannelsTab>("connectors");
  const selectedStatusPayload: JsonObject = app.channelsSelectedStatus ?? {};
  const selectedConnector = readObject(selectedStatusPayload, "connector") ?? selectedStatusPayload;
  const selectedOperations = readObject(selectedStatusPayload, "operations");
  const selectedQueue = selectedOperations !== null ? readObject(selectedOperations, "queue") : null;
  const selectedSaturation = selectedOperations !== null ? readObject(selectedOperations, "saturation") : null;
  const selectedDiscordOps = selectedOperations !== null ? readObject(selectedOperations, "discord") : null;
  const selectedHealthRefresh = readObject(selectedStatusPayload, "health_refresh");
  const selectedConnectorKind = readString(selectedConnector, "kind");
  const selectedConnectorId = readString(selectedConnector, "connector_id") ?? app.channelsSelectedConnectorId;

  return (
    <main className="workspace-page">
      <WorkspacePageHeader
        eyebrow="Control"
        title="Channels"
        headingLabel="Channels and Router"
        description="Operate connectors, router policy, and Discord setup as one domain with focused modes instead of a pile of unrelated cards."
        status={
          <>
            <WorkspaceStatusChip tone={app.channelsConnectors.length > 0 ? "success" : "default"}>
              {app.channelsConnectors.length} connectors
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={app.channelRouterWarnings.length > 0 ? "warning" : "default"}>
              {app.channelRouterWarnings.length} router warnings
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={selectedConnectorId.length > 0 ? "success" : "default"}>
              {selectedConnectorId.length > 0 ? `Selected ${selectedConnectorId}` : "No connector selected"}
            </WorkspaceStatusChip>
          </>
        }
        actions={
          <Button onPress={() => void app.refreshChannels()} isDisabled={app.channelsBusy} variant="secondary">
            {app.channelsBusy ? "Refreshing..." : "Refresh channels"}
          </Button>
        }
      />

      <section className="workspace-metric-grid">
        <WorkspaceMetricCard
          label="Connectors"
          value={app.channelsConnectors.length}
          detail={app.channelsConnectors[0] === undefined ? "No connectors configured." : readString(app.channelsConnectors[0], "kind") ?? "Connector kind unavailable"}
          tone={app.channelsConnectors.length > 0 ? "success" : "default"}
        />
        <WorkspaceMetricCard
          label="Selected connector"
          value={selectedConnectorId.length > 0 ? selectedConnectorId : "None"}
          detail={selectedConnectorKind ?? "Pick a connector to inspect status and recovery."}
        />
        <WorkspaceMetricCard
          label="Router warnings"
          value={app.channelRouterWarnings.length}
          detail={app.channelRouterWarnings[0] ?? "No router warnings published."}
          tone={app.channelRouterWarnings.length > 0 ? "warning" : "success"}
        />
        <WorkspaceMetricCard
          label="Dead letters"
          value={app.channelsDeadLetters.length}
          detail={app.channelsEvents.length > 0 ? `${app.channelsEvents.length} recent events loaded` : "No logs loaded"}
          tone={app.channelsDeadLetters.length > 0 ? "warning" : "default"}
        />
      </section>

      <div className="workspace-tab-row" role="tablist" aria-label="Channels modes">
        {([
          ["connectors", "Connectors"],
          ["router", "Router"],
          ["discord", "Discord setup"],
        ] as const).map(([tabId, label]) => (
          <button
            key={tabId}
            type="button"
            role="tab"
            aria-selected={activeTab === tabId}
            className={`workspace-tab-button${activeTab === tabId ? " is-active" : ""}`}
            onClick={() => setActiveTab(tabId)}
          >
            {label}
          </button>
        ))}
      </div>

      {activeTab === "connectors" && (
        <section className="workspace-stack">
          <WorkspaceSectionCard
            title="Connector inventory"
            description="Enable, disable, and inspect connectors before dropping into recovery or test actions."
          >
            <div className="workspace-list">
              {app.channelsConnectors.length === 0 ? (
                <p className="chat-muted">No channel connectors configured.</p>
              ) : (
                app.channelsConnectors.map((connector) => {
                  const connectorId = readString(connector, "connector_id") ?? "(missing)";
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
                            {readString(connector, "kind") ?? "unknown kind"} ·{" "}
                            {channelConnectorAvailability(connector)}
                          </p>
                        </div>
                        <WorkspaceStatusChip tone={enabled ? "success" : "default"}>
                          {enabled ? "enabled" : "disabled"}
                        </WorkspaceStatusChip>
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
                })
              )}
            </div>
          </WorkspaceSectionCard>

          <section className="workspace-two-column">
            <WorkspaceSectionCard
              title="Selected connector status"
              description="Inspect readiness, liveness, queue posture, and health refresh output in one place."
            >
              {app.channelsSelectedStatus === null ? (
                <p className="chat-muted">Select a connector to inspect status and routing.</p>
              ) : (
                <div className="workspace-stack">
                  <dl className="workspace-key-value-grid">
                    <div>
                      <dt>Kind</dt>
                      <dd>{selectedConnectorKind ?? "n/a"}</dd>
                    </div>
                    <div>
                      <dt>Queue paused</dt>
                      <dd>{readBool(selectedQueue ?? {}, "paused") ? "Yes" : "No"}</dd>
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
                      <dt>Auth failure</dt>
                      <dd>{readString(selectedOperations ?? {}, "last_auth_failure") ?? "none"}</dd>
                    </div>
                    <div>
                      <dt>Permission gap</dt>
                      <dd>{readString(selectedDiscordOps ?? {}, "last_permission_failure") ?? "none"}</dd>
                    </div>
                  </dl>

                  {selectedHealthRefresh !== null && (
                    <pre>{toPrettyJson(selectedHealthRefresh, app.revealSensitiveValues)}</pre>
                  )}
                </div>
              )}
            </WorkspaceSectionCard>

            <WorkspaceSectionCard
              title="Recovery and test controls"
              description="Queue control, health refresh, test-send, and dead-letter replay stay adjacent to live connector telemetry."
            >
              <div className="workspace-stack">
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
                    Force drain
                  </Button>
                </div>

                <div className="workspace-form-grid">
                  <label>
                    Verify channel
                    <input
                      value={app.discordWizardVerifyChannelId}
                      onChange={(event) => app.setDiscordWizardVerifyChannelId(event.target.value)}
                    />
                  </label>
                </div>

                <Button onPress={() => void app.refreshChannelHealth()} isDisabled={app.channelsBusy}>
                  {app.channelsBusy ? "Refreshing..." : "Run health refresh"}
                </Button>

                <form className="workspace-form" onSubmit={(event) => void app.sendChannelTest(event)}>
                  <div className="workspace-form-grid">
                    <label>
                      Text
                      <input value={app.channelsTestText} onChange={(event) => app.setChannelsTestText(event.target.value)} />
                    </label>
                    <label>
                      Conversation ID
                      <input value={app.channelsTestConversationId} onChange={(event) => app.setChannelsTestConversationId(event.target.value)} />
                    </label>
                    <label>
                      Sender ID
                      <input value={app.channelsTestSenderId} onChange={(event) => app.setChannelsTestSenderId(event.target.value)} />
                    </label>
                    <label>
                      Sender display
                      <input value={app.channelsTestSenderDisplay} onChange={(event) => app.setChannelsTestSenderDisplay(event.target.value)} />
                    </label>
                  </div>
                  <div className="workspace-inline">
                    <label className="console-checkbox-inline">
                      <input type="checkbox" checked={app.channelsTestCrashOnce} onChange={(event) => app.setChannelsTestCrashOnce(event.target.checked)} />
                      Simulate crash once
                    </label>
                    <label className="console-checkbox-inline">
                      <input type="checkbox" checked={app.channelsTestDirectMessage} onChange={(event) => app.setChannelsTestDirectMessage(event.target.checked)} />
                      Direct message
                    </label>
                    <label className="console-checkbox-inline">
                      <input type="checkbox" checked={app.channelsTestBroadcast} onChange={(event) => app.setChannelsTestBroadcast(event.target.checked)} />
                      Broadcast
                    </label>
                  </div>
                  <Button type="submit" isDisabled={app.channelsBusy}>
                    {app.channelsBusy ? "Sending..." : "Send connector test"}
                  </Button>
                </form>

                <DiscordConnectorActionsPanel app={app} selectedConnectorKind={selectedConnectorKind} />
              </div>
            </WorkspaceSectionCard>
          </section>

          <WorkspaceSectionCard
            title="Logs and dead letters"
            description="Keep operational evidence visible before replay or discard actions."
          >
            <div className="workspace-form-grid">
              <label>
                Selected connector
                <input value={app.channelsSelectedConnectorId} readOnly />
              </label>
              <label>
                Log limit
                <input value={app.channelsLogsLimit} onChange={(event) => app.setChannelsLogsLimit(event.target.value)} />
              </label>
            </div>

            {app.channelsDeadLetters.length > 0 && (
              <div className="workspace-inline">
                {app.channelsDeadLetters.map((deadLetter) => {
                  const deadLetterId =
                    typeof deadLetter.dead_letter_id === "number"
                      ? deadLetter.dead_letter_id
                      : Number(deadLetter.dead_letter_id ?? Number.NaN);
                  if (!Number.isFinite(deadLetterId)) {
                    return null;
                  }
                  return (
                    <div key={deadLetterId} className="console-inline-actions">
                      <span className="chat-muted">Dead letter {deadLetterId}</span>
                      <Button size="sm" variant="secondary" onPress={() => void app.replayChannelDeadLetter(deadLetterId)}>
                        Replay
                      </Button>
                      <Button size="sm" variant="secondary" onPress={() => void app.discardChannelDeadLetter(deadLetterId)}>
                        Discard
                      </Button>
                    </div>
                  );
                })}
              </div>
            )}

            {app.channelsEvents.length === 0 && app.channelsDeadLetters.length === 0 ? (
              <p className="chat-muted">No connector logs loaded.</p>
            ) : (
              <pre>
                {toPrettyJson(
                  {
                    events: app.channelsEvents,
                    dead_letters: app.channelsDeadLetters,
                  },
                  app.revealSensitiveValues
                )}
              </pre>
            )}
          </WorkspaceSectionCard>
        </section>
      )}

      {activeTab === "router" && (
        <section className="workspace-two-column">
          <WorkspaceSectionCard
            title="Router rules and warnings"
            description="Preview current router posture before enabling broader message ingress."
          >
            <div className="workspace-stack">
              <p><strong>Config hash:</strong> {app.channelRouterConfigHash || "n/a"}</p>
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
                <pre>{toPrettyJson(app.channelRouterRules, app.revealSensitiveValues)}</pre>
              )}
            </div>
          </WorkspaceSectionCard>

          <WorkspaceSectionCard
            title="Route preview"
            description="Test route acceptance with the current sender, channel, and broadcast posture."
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
                  <input value={app.channelRouterPreviewConversationId} onChange={(event) => app.setChannelRouterPreviewConversationId(event.target.value)} />
                </label>
                <label>
                  Sender identity
                  <input value={app.channelRouterPreviewSenderIdentity} onChange={(event) => app.setChannelRouterPreviewSenderIdentity(event.target.value)} />
                </label>
                <label>
                  Sender display
                  <input value={app.channelRouterPreviewSenderDisplay} onChange={(event) => app.setChannelRouterPreviewSenderDisplay(event.target.value)} />
                </label>
                <label>
                  Max payload bytes
                  <input value={app.channelRouterPreviewMaxPayloadBytes} onChange={(event) => app.setChannelRouterPreviewMaxPayloadBytes(event.target.value)} />
                </label>
              </div>
              <div className="workspace-inline">
                <label className="console-checkbox-inline">
                  <input type="checkbox" checked={app.channelRouterPreviewSenderVerified} onChange={(event) => app.setChannelRouterPreviewSenderVerified(event.target.checked)} />
                  Sender verified
                </label>
                <label className="console-checkbox-inline">
                  <input type="checkbox" checked={app.channelRouterPreviewIsDirectMessage} onChange={(event) => app.setChannelRouterPreviewIsDirectMessage(event.target.checked)} />
                  Direct message
                </label>
                <label className="console-checkbox-inline">
                  <input type="checkbox" checked={app.channelRouterPreviewRequestedBroadcast} onChange={(event) => app.setChannelRouterPreviewRequestedBroadcast(event.target.checked)} />
                  Requested broadcast
                </label>
              </div>
              <Button type="submit" isDisabled={app.channelsBusy}>
                {app.channelsBusy ? "Previewing..." : "Preview route"}
              </Button>
            </form>
            {app.channelRouterPreviewResult !== null && (
              <pre>{toPrettyJson(app.channelRouterPreviewResult, app.revealSensitiveValues)}</pre>
            )}
          </WorkspaceSectionCard>

          <WorkspaceSectionCard
            title="Pairings and pairing codes"
            description="Mint pairing codes and inspect active pairings without leaving the router workspace."
            className="workspace-section-card--wide"
          >
            <form className="workspace-form" onSubmit={(event) => void app.mintChannelRouterPairingCode(event)}>
              <div className="workspace-form-grid">
                <label>
                  Filter channel
                  <input value={app.channelRouterPairingsFilterChannel} onChange={(event) => app.setChannelRouterPairingsFilterChannel(event.target.value)} />
                </label>
                <label>
                  Mint channel
                  <input value={app.channelRouterMintChannel} onChange={(event) => app.setChannelRouterMintChannel(event.target.value)} />
                </label>
                <label>
                  Issued by
                  <input value={app.channelRouterMintIssuedBy} onChange={(event) => app.setChannelRouterMintIssuedBy(event.target.value)} />
                </label>
                <label>
                  TTL ms
                  <input value={app.channelRouterMintTtlMs} onChange={(event) => app.setChannelRouterMintTtlMs(event.target.value)} />
                </label>
              </div>
              <div className="console-inline-actions">
                <Button variant="secondary" onPress={() => void app.refreshChannelRouterPairings()} isDisabled={app.channelsBusy}>
                  Refresh pairings
                </Button>
                <Button type="submit" isDisabled={app.channelsBusy}>
                  {app.channelsBusy ? "Minting..." : "Mint pairing code"}
                </Button>
              </div>
            </form>

            {app.channelRouterMintResult !== null && (
              <pre>{toPrettyJson(app.channelRouterMintResult, app.revealSensitiveValues)}</pre>
            )}
            {app.channelRouterPairings.length === 0 ? (
              <p className="chat-muted">No pairings loaded.</p>
            ) : (
              <pre>{toPrettyJson(app.channelRouterPairings, app.revealSensitiveValues)}</pre>
            )}
          </WorkspaceSectionCard>
        </section>
      )}

      {activeTab === "discord" && (
        <section className="workspace-stack">
          <WorkspaceSectionCard
            title="Discord onboarding"
            description="Probe, apply, and verify the live Discord connector contract from the dashboard."
          >
            <DiscordOnboardingPanel app={app} />
          </WorkspaceSectionCard>

          <WorkspaceSectionCard
            title="Discord verification"
            description="Keep direct verification flows with the onboarding path so Discord setup remains one focused mode."
          >
            <DiscordConnectorActionsPanel app={app} selectedConnectorKind={selectedConnectorKind} />
          </WorkspaceSectionCard>
        </section>
      )}
    </main>
  );
}
