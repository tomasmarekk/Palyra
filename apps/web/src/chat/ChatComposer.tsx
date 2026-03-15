import {
  ActionButton,
  ActionCluster,
  AppForm,
  TextAreaField
} from "../console/components/ui";

type ChatComposerProps = {
  composerText: string;
  setComposerText: (value: string) => void;
  streaming: boolean;
  activeSessionId: string;
  submitMessage: () => void;
  cancelStreaming: () => void;
  clearTranscript: () => void;
};

export function ChatComposer({
  composerText,
  setComposerText,
  streaming,
  activeSessionId,
  submitMessage,
  cancelStreaming,
  clearTranscript
}: ChatComposerProps) {
  return (
    <AppForm
      className="chat-composer"
      onSubmit={(event) => {
        event.preventDefault();
        submitMessage();
      }}
    >
      <div className="workspace-panel__intro">
        <p className="workspace-kicker">Composer</p>
        <h4>Next operator instruction</h4>
      </div>
      <TextAreaField
        label="Message"
        placeholder="Describe what you want the assistant to do"
        rows={4}
        value={composerText}
        onChange={setComposerText}
      />
      <ActionCluster>
        <ActionButton
          isDisabled={streaming || activeSessionId.trim().length === 0}
          type="submit"
          variant="primary"
        >
          {streaming ? "Streaming..." : "Send"}
        </ActionButton>
        <ActionButton
          isDisabled={!streaming}
          type="button"
          variant="danger"
          onPress={cancelStreaming}
        >
          Cancel stream
        </ActionButton>
        <ActionButton type="button" variant="secondary" onPress={clearTranscript}>
          Clear local transcript
        </ActionButton>
      </ActionCluster>
    </AppForm>
  );
}
