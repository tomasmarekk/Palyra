import { useCallback, useMemo, useState } from "react";

import type { ConsoleApiClient } from "../consoleApi";
import { findObjectiveForSession } from "../console/objectiveLinks";
import { isJsonObject, readString, type JsonObject } from "../console/shared";

type SessionObjectiveBinding = {
  session_id: string;
  session_key?: string;
  session_label?: string;
};

type UseChatObjectivesParams = {
  api: ConsoleApiClient;
  preferredObjectiveId: string | null;
  selectedSession: SessionObjectiveBinding | null;
};

export function useChatObjectives({
  api,
  preferredObjectiveId,
  selectedSession,
}: UseChatObjectivesParams) {
  const [objectives, setObjectives] = useState<JsonObject[]>([]);

  const refreshObjectives = useCallback(async () => {
    const response = await api.listObjectives(new URLSearchParams({ limit: "64" }));
    setObjectives(
      Array.isArray(response.objectives) ? response.objectives.filter(isJsonObject) : [],
    );
  }, [api]);

  const selectedObjective = useMemo(
    () => findObjectiveForSession(objectives, selectedSession, preferredObjectiveId),
    [objectives, preferredObjectiveId, selectedSession],
  );

  const selectedObjectiveLabel = useMemo(() => {
    if (selectedObjective === null) {
      return null;
    }
    const name = readString(selectedObjective, "name") ?? "Objective";
    const kind = readString(selectedObjective, "kind") ?? "objective";
    return `${kind.replaceAll("_", " ")} · ${name}`;
  }, [selectedObjective]);

  const selectedObjectiveFocus = useMemo(() => {
    if (selectedObjective === null) {
      return null;
    }
    return readString(selectedObjective, "current_focus");
  }, [selectedObjective]);

  return {
    objectives,
    refreshObjectives,
    selectedObjective,
    selectedObjectiveFocus,
    selectedObjectiveLabel,
  };
}
