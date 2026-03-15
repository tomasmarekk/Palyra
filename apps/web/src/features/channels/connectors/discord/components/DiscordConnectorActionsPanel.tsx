import {
  ActionButton,
  AppForm,
  CheckboxField,
  TextInputField
} from "../../../../../console/components/ui";
import type { ConsoleAppState } from "../../../../../console/useConsoleAppState";

type DiscordConnectorActionsPanelProps = {
  app: ConsoleAppState;
  selectedConnectorKind: string | null;
};

export function DiscordConnectorActionsPanel({
  app,
  selectedConnectorKind,
}: DiscordConnectorActionsPanelProps) {
  return (
    <>
      {selectedConnectorKind === "discord" && (
        <>
          <h4>Discord direct verification</h4>
          <AppForm
            className="console-form"
            onSubmit={(event) => void app.sendDiscordTest(event)}
          >
            <div className="console-grid-4">
              <TextInputField
                label="Target"
                value={app.channelsDiscordTarget}
                onChange={app.setChannelsDiscordTarget}
              />
              <TextInputField
                label="Text"
                value={app.channelsDiscordText}
                onChange={app.setChannelsDiscordText}
              />
              <TextInputField
                label="Auto reaction"
                value={app.channelsDiscordAutoReaction}
                onChange={app.setChannelsDiscordAutoReaction}
              />
              <TextInputField
                label="Thread ID"
                value={app.channelsDiscordThreadId}
                onChange={app.setChannelsDiscordThreadId}
              />
            </div>
            <CheckboxField
              label="Confirm Discord outbound test send"
              checked={app.channelsDiscordConfirm}
              onChange={app.setChannelsDiscordConfirm}
              disabled={app.channelsBusy}
            />
            <ActionButton type="submit" isDisabled={app.channelsBusy}>
              {app.channelsBusy ? "Sending..." : "Send Discord test"}
            </ActionButton>
          </AppForm>
        </>
      )}

      <h4>Discord verify target</h4>
      <div className="console-grid-3">
        <TextInputField
          label="Target"
          value={app.discordWizardVerifyTarget}
          onChange={app.setDiscordWizardVerifyTarget}
        />
        <TextInputField
          label="Text"
          value={app.discordWizardVerifyText}
          onChange={app.setDiscordWizardVerifyText}
        />
        <CheckboxField
          label="Confirm verification send"
          checked={app.discordWizardVerifyConfirm}
          onChange={app.setDiscordWizardVerifyConfirm}
          disabled={app.channelsBusy}
        />
      </div>
      <ActionButton
        type="button"
        onPress={() => void app.runDiscordVerification()}
        isDisabled={app.channelsBusy}
      >
        {app.channelsBusy ? "Verifying..." : "Verify Discord target"}
      </ActionButton>
    </>
  );
}
