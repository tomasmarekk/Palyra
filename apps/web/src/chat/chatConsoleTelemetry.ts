import type { UxTelemetryEvent } from "../console/contracts";

type EmitChatUxEvent = (
  event: Omit<UxTelemetryEvent, "surface" | "locale" | "mode">,
) => Promise<void>;

export async function emitSessionResumed(
  emitUxEvent: EmitChatUxEvent,
  sessionId: string,
): Promise<void> {
  await emitUxEvent({
    name: "ux.session.resumed",
    section: "chat",
    sessionId,
    summary: "Resumed chat session.",
  });
}

export async function emitPromptSubmitted(
  emitUxEvent: EmitChatUxEvent,
  sessionId: string,
): Promise<void> {
  await emitUxEvent({
    name: "ux.chat.prompt_submitted",
    section: "chat",
    sessionId,
    summary: "Submitted a chat prompt.",
  });
}

export async function emitRunInspected(emitUxEvent: EmitChatUxEvent, runId: string): Promise<void> {
  await emitUxEvent({
    name: "ux.run.inspected",
    section: "chat",
    runId,
    summary: "Opened run inspector.",
  });
}
