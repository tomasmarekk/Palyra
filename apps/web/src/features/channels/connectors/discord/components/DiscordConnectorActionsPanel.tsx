import {
  ActionButton,
  AppForm,
  CheckboxField,
  TextInputField,
} from "../../../../../console/components/ui";
import type { DiscordChannelController } from "../controller";

type DiscordConnectorActionsPanelProps = {
  discord: DiscordChannelController;
  selectedConnectorKind: string | null;
};

export function DiscordConnectorActionsPanel({
  discord,
  selectedConnectorKind,
}: DiscordConnectorActionsPanelProps) {
  return (
    <>
      {selectedConnectorKind === "discord" && (
        <>
          <h4>Discord direct verification</h4>
          <AppForm className="console-form" onSubmit={(event) => void discord.sendTest(event)}>
            <div className="console-grid-4">
              <TextInputField
                label="Target"
                value={discord.channelsDiscordTarget}
                onChange={discord.setChannelsDiscordTarget}
              />
              <TextInputField
                label="Text"
                value={discord.channelsDiscordText}
                onChange={discord.setChannelsDiscordText}
              />
              <TextInputField
                label="Auto reaction"
                value={discord.channelsDiscordAutoReaction}
                onChange={discord.setChannelsDiscordAutoReaction}
              />
              <TextInputField
                label="Thread ID"
                value={discord.channelsDiscordThreadId}
                onChange={discord.setChannelsDiscordThreadId}
              />
            </div>
            <CheckboxField
              label="Confirm Discord outbound test send"
              checked={discord.channelsDiscordConfirm}
              onChange={discord.setChannelsDiscordConfirm}
              disabled={discord.isBusy}
            />
            <ActionButton type="submit" isDisabled={discord.isBusy}>
              {discord.isBusy ? "Sending..." : "Send Discord test"}
            </ActionButton>
          </AppForm>
        </>
      )}

      <h4>Discord verify target</h4>
      <div className="console-grid-3">
        <TextInputField
          label="Target"
          value={discord.discordWizardVerifyTarget}
          onChange={discord.setDiscordWizardVerifyTarget}
        />
        <TextInputField
          label="Text"
          value={discord.discordWizardVerifyText}
          onChange={discord.setDiscordWizardVerifyText}
        />
        <CheckboxField
          label="Confirm verification send"
          checked={discord.discordWizardVerifyConfirm}
          onChange={discord.setDiscordWizardVerifyConfirm}
          disabled={discord.isBusy}
        />
      </div>
      <ActionButton
        type="button"
        onPress={() => void discord.runVerification()}
        isDisabled={discord.isBusy}
      >
        {discord.isBusy ? "Verifying..." : "Verify Discord target"}
      </ActionButton>
    </>
  );
}
