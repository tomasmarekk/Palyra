import { ConsoleSectionHeader } from "../components/ConsoleSectionHeader";
import {
  DiscordOnboardingHighlights,
  channelConnectorAvailability,
  readBool,
  readString,
  toPrettyJson,
} from "../shared";
import type { ConsoleAppState } from "../useConsoleAppState";

type ChannelsSectionProps = {
  app: Pick<
    ConsoleAppState,
    | "channelsBusy"
    | "channelsConnectors"
    | "channelsSelectedConnectorId"
    | "channelsSelectedStatus"
    | "channelsEvents"
    | "channelsDeadLetters"
    | "channelsLogsLimit"
    | "setChannelsLogsLimit"
    | "channelsTestText"
    | "setChannelsTestText"
    | "channelsTestConversationId"
    | "setChannelsTestConversationId"
    | "channelsTestSenderId"
    | "setChannelsTestSenderId"
    | "channelsTestSenderDisplay"
    | "setChannelsTestSenderDisplay"
    | "channelsTestCrashOnce"
    | "setChannelsTestCrashOnce"
    | "channelsTestDirectMessage"
    | "setChannelsTestDirectMessage"
    | "channelsTestBroadcast"
    | "setChannelsTestBroadcast"
    | "channelsDiscordTarget"
    | "setChannelsDiscordTarget"
    | "channelsDiscordText"
    | "setChannelsDiscordText"
    | "channelsDiscordAutoReaction"
    | "setChannelsDiscordAutoReaction"
    | "channelsDiscordThreadId"
    | "setChannelsDiscordThreadId"
    | "channelsDiscordConfirm"
    | "setChannelsDiscordConfirm"
    | "channelRouterRules"
    | "channelRouterConfigHash"
    | "channelRouterWarnings"
    | "channelRouterPreviewChannel"
    | "setChannelRouterPreviewChannel"
    | "channelRouterPreviewText"
    | "setChannelRouterPreviewText"
    | "channelRouterPreviewConversationId"
    | "setChannelRouterPreviewConversationId"
    | "channelRouterPreviewSenderIdentity"
    | "setChannelRouterPreviewSenderIdentity"
    | "channelRouterPreviewSenderDisplay"
    | "setChannelRouterPreviewSenderDisplay"
    | "channelRouterPreviewSenderVerified"
    | "setChannelRouterPreviewSenderVerified"
    | "channelRouterPreviewIsDirectMessage"
    | "setChannelRouterPreviewIsDirectMessage"
    | "channelRouterPreviewRequestedBroadcast"
    | "setChannelRouterPreviewRequestedBroadcast"
    | "channelRouterPreviewMaxPayloadBytes"
    | "setChannelRouterPreviewMaxPayloadBytes"
    | "channelRouterPreviewResult"
    | "channelRouterPairingsFilterChannel"
    | "setChannelRouterPairingsFilterChannel"
    | "channelRouterPairings"
    | "channelRouterMintChannel"
    | "setChannelRouterMintChannel"
    | "channelRouterMintIssuedBy"
    | "setChannelRouterMintIssuedBy"
    | "channelRouterMintTtlMs"
    | "setChannelRouterMintTtlMs"
    | "channelRouterMintResult"
    | "discordWizardBusy"
    | "discordWizardAccountId"
    | "setDiscordWizardAccountId"
    | "discordWizardMode"
    | "setDiscordWizardMode"
    | "discordWizardToken"
    | "setDiscordWizardToken"
    | "discordWizardScope"
    | "setDiscordWizardScope"
    | "discordWizardAllowFrom"
    | "setDiscordWizardAllowFrom"
    | "discordWizardDenyFrom"
    | "setDiscordWizardDenyFrom"
    | "discordWizardRequireMention"
    | "setDiscordWizardRequireMention"
    | "discordWizardBroadcast"
    | "setDiscordWizardBroadcast"
    | "discordWizardConcurrency"
    | "setDiscordWizardConcurrency"
    | "discordWizardVerifyChannelId"
    | "setDiscordWizardVerifyChannelId"
    | "discordWizardPreflight"
    | "discordWizardApply"
    | "discordWizardVerifyTarget"
    | "setDiscordWizardVerifyTarget"
    | "discordWizardVerifyText"
    | "setDiscordWizardVerifyText"
    | "discordWizardVerifyConfirm"
    | "setDiscordWizardVerifyConfirm"
    | "refreshChannels"
    | "selectChannelConnector"
    | "toggleConnector"
    | "previewChannelRouter"
    | "refreshChannelRouterPairings"
    | "mintChannelRouterPairingCode"
    | "sendChannelTest"
    | "sendDiscordTest"
    | "runDiscordPreflight"
    | "applyDiscordOnboarding"
    | "runDiscordVerification"
    | "revealSensitiveValues"
  >;
};

export function ChannelsSection({ app }: ChannelsSectionProps) {
  return (
    <main className="console-card">
      <ConsoleSectionHeader
        title="Channels and Router"
        description="Operate Discord onboarding, connector health, router previews, pairing codes, and delivery diagnostics from the canonical dashboard surface."
        actions={(
          <button type="button" onClick={() => void app.refreshChannels()} disabled={app.channelsBusy}>
            {app.channelsBusy ? "Refreshing..." : "Refresh channels"}
          </button>
        )}
      />

      <section className="console-subpanel">
        <div className="console-subpanel__header">
          <div>
            <h3>Discord onboarding wizard</h3>
            <p className="chat-muted">
              Probe, apply, and verify the live Discord connector contract without falling back to manual config edits.
            </p>
          </div>
        </div>
        <div className="console-grid-4">
          <label>
            Account ID
            <input value={app.discordWizardAccountId} onChange={(event) => app.setDiscordWizardAccountId(event.target.value)} />
          </label>
          <label>
            Mode
            <select value={app.discordWizardMode} onChange={(event) => app.setDiscordWizardMode(event.target.value === "remote_vps" ? "remote_vps" : "local")}>
              <option value="local">local</option>
              <option value="remote_vps">remote_vps</option>
            </select>
          </label>
          <label>
            Bot token
            <input value={app.discordWizardToken} onChange={(event) => app.setDiscordWizardToken(event.target.value)} placeholder="Never persisted in config plaintext" />
          </label>
          <label>
            Verify channel ID
            <input value={app.discordWizardVerifyChannelId} onChange={(event) => app.setDiscordWizardVerifyChannelId(event.target.value)} />
          </label>
        </div>
        <div className="console-grid-4">
          <label>
            Inbound scope
            <select value={app.discordWizardScope} onChange={(event) => app.setDiscordWizardScope(event.target.value as "dm_only" | "allowlisted_guild_channels" | "open_guild_channels")}>
              <option value="dm_only">dm_only</option>
              <option value="allowlisted_guild_channels">allowlisted_guild_channels</option>
              <option value="open_guild_channels">open_guild_channels</option>
            </select>
          </label>
          <label>
            Allow from
            <input value={app.discordWizardAllowFrom} onChange={(event) => app.setDiscordWizardAllowFrom(event.target.value)} />
          </label>
          <label>
            Deny from
            <input value={app.discordWizardDenyFrom} onChange={(event) => app.setDiscordWizardDenyFrom(event.target.value)} />
          </label>
          <label>
            Concurrency
            <input value={app.discordWizardConcurrency} onChange={(event) => app.setDiscordWizardConcurrency(event.target.value)} />
          </label>
        </div>
        <div className="console-inline-actions">
          <label className="console-checkbox-inline">
            <input type="checkbox" checked={app.discordWizardRequireMention} onChange={(event) => app.setDiscordWizardRequireMention(event.target.checked)} />
            Require mention
          </label>
          <label>
            Broadcast strategy
            <select value={app.discordWizardBroadcast} onChange={(event) => app.setDiscordWizardBroadcast(event.target.value as "deny" | "mention_only" | "allow")}>
              <option value="deny">deny</option>
              <option value="mention_only">mention_only</option>
              <option value="allow">allow</option>
            </select>
          </label>
          <button type="button" onClick={() => void app.runDiscordPreflight()} disabled={app.discordWizardBusy}>
            {app.discordWizardBusy ? "Running..." : "Run preflight"}
          </button>
          <button type="button" onClick={() => void app.applyDiscordOnboarding()} disabled={app.discordWizardBusy}>
            {app.discordWizardBusy ? "Applying..." : "Apply onboarding"}
          </button>
        </div>
        {app.discordWizardPreflight !== null && <DiscordOnboardingHighlights title="Preflight highlights" payload={app.discordWizardPreflight} />}
        {app.discordWizardPreflight !== null && <pre>{toPrettyJson(app.discordWizardPreflight, app.revealSensitiveValues)}</pre>}
        {app.discordWizardApply !== null && <pre>{toPrettyJson(app.discordWizardApply, app.revealSensitiveValues)}</pre>}
      </section>

      <div className="console-table-wrap">
        <table className="console-table">
          <thead>
            <tr>
              <th>Connector ID</th>
              <th>Kind</th>
              <th>Availability</th>
              <th>Enabled</th>
              <th>Readiness</th>
              <th>Liveness</th>
              <th>Action</th>
            </tr>
          </thead>
          <tbody>
            {app.channelsConnectors.length === 0 && <tr><td colSpan={7}>No channel connectors configured.</td></tr>}
            {app.channelsConnectors.map((connector) => {
              const connectorId = readString(connector, "connector_id") ?? "(missing)";
              const enabled = readBool(connector, "enabled");
              return (
                <tr key={connectorId}>
                  <td>{connectorId}</td>
                  <td>{readString(connector, "kind") ?? "-"}</td>
                  <td>{channelConnectorAvailability(connector)}</td>
                  <td>{enabled ? "yes" : "no"}</td>
                  <td>{readString(connector, "readiness") ?? "-"}</td>
                  <td>{readString(connector, "liveness") ?? "-"}</td>
                  <td className="console-action-cell">
                    <button type="button" className="secondary" onClick={() => void app.selectChannelConnector(connectorId)}>Select</button>
                    <button type="button" onClick={() => void app.toggleConnector(connector, !enabled)}>
                      {enabled ? "Disable" : "Enable"}
                    </button>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      <section className="console-grid-2">
        <article className="console-subpanel">
          <h3>Selected connector status</h3>
          {app.channelsSelectedStatus === null ? <p>Select a connector to inspect status and routing.</p> : <pre>{toPrettyJson(app.channelsSelectedStatus, app.revealSensitiveValues)}</pre>}
        </article>
        <article className="console-subpanel">
          <div className="console-subpanel__header">
            <div>
              <h3>Connector logs and dead letters</h3>
              <p className="chat-muted">
                Delivery diagnostics stay on the dashboard so operators can inspect failures without switching surfaces.
              </p>
            </div>
          </div>
          <div className="console-grid-2">
            <label>
              Selected connector
              <input value={app.channelsSelectedConnectorId} readOnly />
            </label>
            <label>
              Log limit
              <input value={app.channelsLogsLimit} onChange={(event) => app.setChannelsLogsLimit(event.target.value)} />
            </label>
          </div>
          {app.channelsEvents.length === 0 && app.channelsDeadLetters.length === 0 ? (
            <p>No connector logs loaded.</p>
          ) : (
            <pre>{toPrettyJson({ events: app.channelsEvents, dead_letters: app.channelsDeadLetters }, app.revealSensitiveValues)}</pre>
          )}
        </article>
      </section>

      <section className="console-grid-2">
        <article className="console-subpanel">
          <div className="console-subpanel__header">
            <div>
              <h3>Router rules and warnings</h3>
              <p className="chat-muted">
                Preview route acceptance, inspect current config hash, and keep warning output visible before enabling broader message ingress.
              </p>
            </div>
          </div>
          <p><strong>Config hash:</strong> {app.channelRouterConfigHash || "n/a"}</p>
          {app.channelRouterWarnings.length === 0 ? (
            <p>No router warnings published.</p>
          ) : (
            <ul className="console-compact-list">
              {app.channelRouterWarnings.map((warning) => (
                <li key={warning}>{warning}</li>
              ))}
            </ul>
          )}
          {app.channelRouterRules === null ? <p>No router rules loaded.</p> : <pre>{toPrettyJson(app.channelRouterRules, app.revealSensitiveValues)}</pre>}
        </article>
        <article className="console-subpanel">
          <h3>Route preview</h3>
          <form className="console-form" onSubmit={(event) => void app.previewChannelRouter(event)}>
            <div className="console-grid-3">
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
            </div>
            <div className="console-grid-4">
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
              <div className="console-inline-actions">
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
            </div>
            <button type="submit" disabled={app.channelsBusy}>{app.channelsBusy ? "Previewing..." : "Preview route"}</button>
          </form>
          {app.channelRouterPreviewResult === null ? <p>No route preview computed.</p> : <pre>{toPrettyJson(app.channelRouterPreviewResult, app.revealSensitiveValues)}</pre>}
        </article>
      </section>

      <section className="console-grid-2">
        <article className="console-subpanel">
          <h3>Router pairing codes</h3>
          <form className="console-form" onSubmit={(event) => void app.mintChannelRouterPairingCode(event)}>
            <div className="console-grid-3">
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
            </div>
            <div className="console-grid-2">
              <label>
                TTL ms
                <input value={app.channelRouterMintTtlMs} onChange={(event) => app.setChannelRouterMintTtlMs(event.target.value)} />
              </label>
              <div className="console-inline-actions">
                <button type="button" className="secondary" onClick={() => void app.refreshChannelRouterPairings()} disabled={app.channelsBusy}>
                  Refresh pairings
                </button>
                <button type="submit" disabled={app.channelsBusy}>
                  {app.channelsBusy ? "Minting..." : "Mint pairing code"}
                </button>
              </div>
            </div>
          </form>
          {app.channelRouterMintResult !== null && <pre>{toPrettyJson(app.channelRouterMintResult, app.revealSensitiveValues)}</pre>}
          {app.channelRouterPairings.length === 0 ? <p>No pairings loaded.</p> : <pre>{toPrettyJson(app.channelRouterPairings, app.revealSensitiveValues)}</pre>}
        </article>

        <article className="console-subpanel">
          <h3>Connector test send</h3>
          <form className="console-form" onSubmit={(event) => void app.sendChannelTest(event)}>
            <div className="console-grid-4">
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
            <div className="console-inline-actions">
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
              <button type="submit" disabled={app.channelsBusy}>{app.channelsBusy ? "Sending..." : "Send connector test"}</button>
            </div>
          </form>

          {readString(app.channelsSelectedStatus ?? {}, "kind") === "discord" && (
            <>
              <h4>Discord direct verification</h4>
              <form className="console-form" onSubmit={(event) => void app.sendDiscordTest(event)}>
                <div className="console-grid-4">
                  <label>
                    Target
                    <input value={app.channelsDiscordTarget} onChange={(event) => app.setChannelsDiscordTarget(event.target.value)} />
                  </label>
                  <label>
                    Text
                    <input value={app.channelsDiscordText} onChange={(event) => app.setChannelsDiscordText(event.target.value)} />
                  </label>
                  <label>
                    Auto reaction
                    <input value={app.channelsDiscordAutoReaction} onChange={(event) => app.setChannelsDiscordAutoReaction(event.target.value)} />
                  </label>
                  <label>
                    Thread ID
                    <input value={app.channelsDiscordThreadId} onChange={(event) => app.setChannelsDiscordThreadId(event.target.value)} />
                  </label>
                </div>
                <label className="console-checkbox-inline">
                  <input type="checkbox" checked={app.channelsDiscordConfirm} onChange={(event) => app.setChannelsDiscordConfirm(event.target.checked)} />
                  Confirm Discord outbound test send
                </label>
                <button type="submit" disabled={app.channelsBusy}>{app.channelsBusy ? "Sending..." : "Send Discord test"}</button>
              </form>
            </>
          )}

          <h4>Discord verify target</h4>
          <div className="console-grid-3">
            <label>
              Target
              <input value={app.discordWizardVerifyTarget} onChange={(event) => app.setDiscordWizardVerifyTarget(event.target.value)} />
            </label>
            <label>
              Text
              <input value={app.discordWizardVerifyText} onChange={(event) => app.setDiscordWizardVerifyText(event.target.value)} />
            </label>
            <label className="console-checkbox-inline">
              <input type="checkbox" checked={app.discordWizardVerifyConfirm} onChange={(event) => app.setDiscordWizardVerifyConfirm(event.target.checked)} />
              Confirm verification send
            </label>
          </div>
          <button type="button" onClick={() => void app.runDiscordVerification()} disabled={app.channelsBusy}>
            {app.channelsBusy ? "Verifying..." : "Verify Discord target"}
          </button>
        </article>
      </section>
    </main>
  );
}
