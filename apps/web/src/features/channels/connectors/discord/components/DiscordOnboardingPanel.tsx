import {
  ActionButton,
  ActionCluster,
  CheckboxField,
  SelectField,
  TextInputField
} from "../../../../../console/components/ui";
import { DiscordOnboardingHighlights, toPrettyJson } from "../../../../../console/shared";
import type { ConsoleAppState } from "../../../../../console/useConsoleAppState";

type DiscordOnboardingPanelProps = {
  app: ConsoleAppState;
};

export function DiscordOnboardingPanel({
  app,
}: DiscordOnboardingPanelProps) {
  const modeOptions = [
    { key: "local", label: "local" },
    { key: "remote_vps", label: "remote_vps" }
  ] as const;

  const scopeOptions = [
    { key: "dm_only", label: "dm_only" },
    { key: "allowlisted_guild_channels", label: "allowlisted_guild_channels" },
    { key: "open_guild_channels", label: "open_guild_channels" }
  ] as const;

  const broadcastOptions = [
    { key: "deny", label: "deny" },
    { key: "mention_only", label: "mention_only" },
    { key: "allow", label: "allow" }
  ] as const;

  return (
    <section className="console-subpanel">
      <div className="console-subpanel__header">
        <div>
          <h3>Discord onboarding wizard</h3>
          <p className="chat-muted">
            Probe, apply, and verify the live Discord connector contract without
            falling back to manual config edits.
          </p>
        </div>
      </div>
      <div className="console-grid-4">
        <TextInputField
          label="Account ID"
          value={app.discordWizardAccountId}
          onChange={app.setDiscordWizardAccountId}
        />
        <SelectField
          label="Mode"
          value={app.discordWizardMode}
          onChange={(value) =>
            app.setDiscordWizardMode(value === "remote_vps" ? "remote_vps" : "local")
          }
          options={modeOptions}
        />
        <TextInputField
          label="Bot token"
          value={app.discordWizardToken}
          onChange={app.setDiscordWizardToken}
          placeholder="Never persisted in config plaintext"
        />
        <TextInputField
          label="Verify channel ID"
          value={app.discordWizardVerifyChannelId}
          onChange={app.setDiscordWizardVerifyChannelId}
        />
      </div>
      <div className="console-grid-4">
        <SelectField
          label="Inbound scope"
          value={app.discordWizardScope}
          onChange={(value) =>
            app.setDiscordWizardScope(
              value as "dm_only" | "allowlisted_guild_channels" | "open_guild_channels"
            )
          }
          options={scopeOptions}
        />
        <TextInputField
          label="Allow from"
          value={app.discordWizardAllowFrom}
          onChange={app.setDiscordWizardAllowFrom}
        />
        <TextInputField
          label="Deny from"
          value={app.discordWizardDenyFrom}
          onChange={app.setDiscordWizardDenyFrom}
        />
        <TextInputField
          label="Concurrency"
          value={app.discordWizardConcurrency}
          onChange={app.setDiscordWizardConcurrency}
        />
      </div>
      <div className="workspace-stack">
        <div className="console-grid-4">
          <CheckboxField
            label="Require mention"
            checked={app.discordWizardRequireMention}
            onChange={app.setDiscordWizardRequireMention}
            disabled={app.discordWizardBusy}
          />
          <SelectField
            label="Broadcast strategy"
            value={app.discordWizardBroadcast}
            onChange={(value) =>
              app.setDiscordWizardBroadcast(value as "deny" | "mention_only" | "allow")
            }
            options={broadcastOptions}
            disabled={app.discordWizardBusy}
          />
        </div>
        <ActionCluster>
          <ActionButton
            type="button"
            variant="secondary"
            onPress={() => void app.runDiscordPreflight()}
            isDisabled={app.discordWizardBusy}
          >
            {app.discordWizardBusy ? "Running..." : "Run preflight"}
          </ActionButton>
          <ActionButton
            type="button"
            onPress={() => void app.applyDiscordOnboarding()}
            isDisabled={app.discordWizardBusy}
          >
            {app.discordWizardBusy ? "Applying..." : "Apply onboarding"}
          </ActionButton>
        </ActionCluster>
      </div>
      {app.discordWizardPreflight !== null && (
        <DiscordOnboardingHighlights
          title="Preflight highlights"
          payload={app.discordWizardPreflight}
        />
      )}
      {app.discordWizardPreflight !== null && (
        <pre>
          {toPrettyJson(app.discordWizardPreflight, app.revealSensitiveValues)}
        </pre>
      )}
      {app.discordWizardApply !== null && (
        <pre>{toPrettyJson(app.discordWizardApply, app.revealSensitiveValues)}</pre>
      )}
    </section>
  );
}
