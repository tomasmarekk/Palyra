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
    <form
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
      <label>
        Message
        <textarea
          value={composerText}
          onChange={(event) => setComposerText(event.target.value)}
          rows={4}
          placeholder="Describe what you want the assistant to do"
        />
      </label>
      <div className="workspace-inline-actions">
        <button type="submit" disabled={streaming || activeSessionId.trim().length === 0}>
          {streaming ? "Streaming..." : "Send"}
        </button>
        <button type="button" className="button--warn" onClick={cancelStreaming} disabled={!streaming}>
          Cancel stream
        </button>
        <button type="button" className="secondary" onClick={clearTranscript}>
          Clear local transcript
        </button>
      </div>
    </form>
  );
}
