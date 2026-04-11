import {
  ActionButton,
  ActionCluster,
  CheckboxField,
  SelectField,
  TextInputField,
} from "../../../../../console/components/ui";
import { DiscordOnboardingHighlights, toPrettyJson } from "../../../../../console/shared";
import type { DiscordChannelController } from "../controller";

type DiscordOnboardingPanelProps = {
  discord: DiscordChannelController;
  revealSensitiveValues: boolean;
};

export function DiscordOnboardingPanel({
  discord,
  revealSensitiveValues,
}: DiscordOnboardingPanelProps) {
  const modeOptions = [
    { key: "local", label: "local" },
    { key: "remote_vps", label: "remote_vps" },
  ] as const;

  const scopeOptions = [
    { key: "dm_only", label: "dm_only" },
    { key: "allowlisted_guild_channels", label: "allowlisted_guild_channels" },
    { key: "open_guild_channels", label: "open_guild_channels" },
  ] as const;

  const broadcastOptions = [
    { key: "deny", label: "deny" },
    { key: "mention_only", label: "mention_only" },
    { key: "allow", label: "allow" },
  ] as const;

  return (
    <section className="console-subpanel">
      <div className="console-subpanel__header">
        <div>
          <h3>Discord onboarding wizard</h3>
          <p className="chat-muted">
            Probe, apply, and verify the live Discord connector contract without falling back to
            manual config edits.
          </p>
        </div>
      </div>
      <div className="console-grid-4">
        <TextInputField
          label="Account ID"
          value={discord.discordWizardAccountId}
          onChange={discord.setDiscordWizardAccountId}
        />
        <SelectField
          label="Mode"
          value={discord.discordWizardMode}
          onChange={(value) =>
            discord.setDiscordWizardMode(value === "remote_vps" ? "remote_vps" : "local")
          }
          options={modeOptions}
        />
        <TextInputField
          label="Bot token"
          value={discord.discordWizardToken}
          onChange={discord.setDiscordWizardToken}
          placeholder="Never persisted in config plaintext"
        />
        <TextInputField
          label="Verify channel ID"
          value={discord.discordWizardVerifyChannelId}
          onChange={discord.setDiscordWizardVerifyChannelId}
        />
      </div>
      <div className="console-grid-4">
        <SelectField
          label="Inbound scope"
          value={discord.discordWizardScope}
          onChange={(value) =>
            discord.setDiscordWizardScope(
              value as "dm_only" | "allowlisted_guild_channels" | "open_guild_channels",
            )
          }
          options={scopeOptions}
        />
        <TextInputField
          label="Allow from"
          value={discord.discordWizardAllowFrom}
          onChange={discord.setDiscordWizardAllowFrom}
        />
        <TextInputField
          label="Deny from"
          value={discord.discordWizardDenyFrom}
          onChange={discord.setDiscordWizardDenyFrom}
        />
        <TextInputField
          label="Concurrency"
          value={discord.discordWizardConcurrency}
          onChange={discord.setDiscordWizardConcurrency}
        />
      </div>
      <div className="workspace-stack">
        <div className="console-grid-4">
          <CheckboxField
            label="Require mention"
            checked={discord.discordWizardRequireMention}
            onChange={discord.setDiscordWizardRequireMention}
            disabled={discord.discordWizardBusy}
          />
          <SelectField
            label="Broadcast strategy"
            value={discord.discordWizardBroadcast}
            onChange={(value) =>
              discord.setDiscordWizardBroadcast(value as "deny" | "mention_only" | "allow")
            }
            options={broadcastOptions}
            disabled={discord.discordWizardBusy}
          />
        </div>
        <ActionCluster>
          <ActionButton
            type="button"
            variant="secondary"
            onPress={() => void discord.runPreflight()}
            isDisabled={discord.discordWizardBusy}
          >
            {discord.discordWizardBusy ? "Running..." : "Run preflight"}
          </ActionButton>
          <ActionButton
            type="button"
            onPress={() => void discord.applyOnboarding()}
            isDisabled={discord.discordWizardBusy}
          >
            {discord.discordWizardBusy ? "Applying..." : "Apply onboarding"}
          </ActionButton>
        </ActionCluster>
      </div>
      {discord.discordWizardPreflight !== null && (
        <DiscordOnboardingHighlights
          title="Preflight highlights"
          payload={discord.discordWizardPreflight}
        />
      )}
      {discord.discordWizardPreflight !== null && (
        <pre>{toPrettyJson(discord.discordWizardPreflight, revealSensitiveValues)}</pre>
      )}
      {discord.discordWizardApply !== null && (
        <pre>{toPrettyJson(discord.discordWizardApply, revealSensitiveValues)}</pre>
      )}
    </section>
  );
}
