import type { ConsoleApiClient, SessionCatalogRecord, SessionProjectContextEnvelope } from "../consoleApi";

import { toErrorMessage } from "./chatShared";

type ProjectContextActionArgs = {
  api: ConsoleApiClient;
  sessionId: string;
  selectedSession: SessionCatalogRecord | null;
  composerText: string;
  loadProjectContextPreview: (
    text: string,
    options?: { reportError?: boolean },
  ) => Promise<unknown>;
  upsertSession: (session: SessionCatalogRecord, options?: { select?: boolean }) => void;
  setError: (next: string | null) => void;
  setNotice: (next: string | null) => void;
  setPhase4BusyKey: (next: string | null) => void;
};

async function runProjectContextMutation(
  args: ProjectContextActionArgs & {
    actionKey: string;
    successMessage: string;
    operation: () => Promise<SessionProjectContextEnvelope>;
  },
): Promise<void> {
  if (args.sessionId.trim().length === 0) {
    args.setError("Select a session first.");
    return;
  }
  args.setError(null);
  args.setNotice(null);
  args.setPhase4BusyKey(`project-context:${args.actionKey}`);
  try {
    const response = await args.operation();
    args.upsertSession(response.session, { select: true });
    await args.loadProjectContextPreview(args.composerText, { reportError: false });
    if (response.action === "scaffold") {
      args.setNotice(
        response.scaffold?.overwritten
          ? "PALYRA.md was refreshed in the workspace root."
          : "PALYRA.md was created in the workspace root.",
      );
      return;
    }
    args.setNotice(args.successMessage);
  } catch (error) {
    args.setError(toErrorMessage(error));
  } finally {
    args.setPhase4BusyKey(null);
  }
}

export async function refreshProjectContextAction(args: ProjectContextActionArgs): Promise<void> {
  await runProjectContextMutation({
    ...args,
    actionKey: "refresh",
    successMessage: "Project context refreshed.",
    operation: () => args.api.refreshSessionProjectContext(args.sessionId),
  });
}

export async function disableProjectContextEntryAction(
  args: ProjectContextActionArgs & { entryId: string },
): Promise<void> {
  await runProjectContextMutation({
    ...args,
    actionKey: `disable:${args.entryId}`,
    successMessage: "Project context file disabled for this session.",
    operation: () => args.api.disableSessionProjectContextEntry(args.sessionId, args.entryId),
  });
}

export async function enableProjectContextEntryAction(
  args: ProjectContextActionArgs & { entryId: string },
): Promise<void> {
  await runProjectContextMutation({
    ...args,
    actionKey: `enable:${args.entryId}`,
    successMessage: "Project context file re-enabled for this session.",
    operation: () => args.api.enableSessionProjectContextEntry(args.sessionId, args.entryId),
  });
}

export async function approveProjectContextEntryAction(
  args: ProjectContextActionArgs & { entryId: string },
): Promise<void> {
  await runProjectContextMutation({
    ...args,
    actionKey: `approve:${args.entryId}`,
    successMessage: "Project context file approved for this session.",
    operation: () => args.api.approveSessionProjectContextEntry(args.sessionId, args.entryId),
  });
}

export async function scaffoldProjectContextAction(args: ProjectContextActionArgs): Promise<void> {
  await runProjectContextMutation({
    ...args,
    actionKey: "scaffold",
    successMessage: "PALYRA.md was created in the workspace root.",
    operation: () =>
      args.api.scaffoldSessionProjectContext(args.sessionId, {
        project_name: args.selectedSession?.session_label ?? args.selectedSession?.title ?? undefined,
      }),
  });
}
