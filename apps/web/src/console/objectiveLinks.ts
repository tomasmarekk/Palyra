import { getSectionPath } from "./navigation";
import {
  readObject,
  readString,
  readStringList,
  type JsonObject,
} from "./shared";

type SessionLike = {
  session_id?: string;
  session_key?: string;
  session_label?: string;
};

export function resolveObjectiveId(objective: JsonObject | null): string | null {
  if (objective === null) {
    return null;
  }
  return readString(objective, "objective_id");
}

export function objectiveWorkspaceDocumentPath(objective: JsonObject | null): string | null {
  if (objective === null) {
    return null;
  }
  return readString(readObject(objective, "workspace") ?? {}, "workspace_document_path");
}

export function objectiveRelatedSessionIds(objective: JsonObject | null): string[] {
  if (objective === null) {
    return [];
  }
  const workspace = readObject(objective, "workspace") ?? {};
  const relatedSessionIds = workspace.related_session_ids;
  return Array.isArray(relatedSessionIds) ? readStringList(workspace, "related_session_ids") : [];
}

export function objectivePrimarySessionId(
  objective: JsonObject | null,
  fallbackSessionId?: string | null,
): string | null {
  const relatedSessionIds = objectiveRelatedSessionIds(objective);
  if (relatedSessionIds.length > 0) {
    return relatedSessionIds[0] ?? null;
  }
  if (typeof fallbackSessionId === "string" && fallbackSessionId.trim().length > 0) {
    return fallbackSessionId.trim();
  }
  return null;
}

export function objectiveMatchesSession(
  objective: JsonObject,
  session: SessionLike | null,
): boolean {
  if (session === null) {
    return false;
  }
  const workspace = readObject(objective, "workspace") ?? {};
  const objectiveSessionKey = readString(workspace, "session_key");
  const objectiveSessionLabel = readString(workspace, "session_label");
  const relatedSessionIds = objectiveRelatedSessionIds(objective);

  if (
    typeof session.session_id === "string" &&
    session.session_id.trim().length > 0 &&
    relatedSessionIds.includes(session.session_id.trim())
  ) {
    return true;
  }
  if (
    typeof session.session_key === "string" &&
    session.session_key.trim().length > 0 &&
    objectiveSessionKey !== null &&
    objectiveSessionKey === session.session_key.trim()
  ) {
    return true;
  }
  if (
    typeof session.session_label === "string" &&
    session.session_label.trim().length > 0 &&
    objectiveSessionLabel !== null &&
    objectiveSessionLabel === session.session_label.trim()
  ) {
    return true;
  }
  return false;
}

export function findObjectiveForSession(
  objectives: JsonObject[],
  session: SessionLike | null,
  preferredObjectiveId?: string | null,
): JsonObject | null {
  const preferred = preferredObjectiveId?.trim();
  if (preferred) {
    const directMatch =
      objectives.find((objective) => resolveObjectiveId(objective) === preferred) ?? null;
    if (directMatch !== null) {
      return directMatch;
    }
  }

  const linked = objectives
    .filter((objective) => objectiveMatchesSession(objective, session))
    .sort((left, right) => {
      const leftState = readString(left, "state") ?? "";
      const rightState = readString(right, "state") ?? "";
      const leftScore = objectiveStateScore(leftState);
      const rightScore = objectiveStateScore(rightState);
      if (leftScore !== rightScore) {
        return rightScore - leftScore;
      }
      const leftUpdated = Number(readString(left, "updated_at_unix_ms") ?? "0");
      const rightUpdated = Number(readString(right, "updated_at_unix_ms") ?? "0");
      return rightUpdated - leftUpdated;
    });
  return linked[0] ?? null;
}

export function buildObjectiveOverviewHref(objectiveId: string): string {
  const params = new URLSearchParams();
  params.set("objectiveId", objectiveId);
  return `${getSectionPath("overview")}?${params.toString()}`;
}

export function buildObjectiveChatHref(options: {
  objective: JsonObject;
  fallbackSessionId?: string | null;
  runId?: string | null;
}): string {
  const params = new URLSearchParams();
  const objectiveId = resolveObjectiveId(options.objective);
  if (objectiveId !== null) {
    params.set("objectiveId", objectiveId);
  }
  const sessionId = objectivePrimarySessionId(options.objective, options.fallbackSessionId);
  if (sessionId !== null) {
    params.set("sessionId", sessionId);
  }
  const runId = options.runId?.trim();
  if (runId) {
    params.set("runId", runId);
  }
  return `${getSectionPath("chat")}?${params.toString()}`;
}

function objectiveStateScore(state: string): number {
  switch (state) {
    case "active":
      return 4;
    case "paused":
      return 3;
    case "draft":
      return 2;
    case "cancelled":
      return 1;
    case "archived":
      return 0;
    default:
      return 0;
  }
}
