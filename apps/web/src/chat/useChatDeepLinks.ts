import { useEffect, useRef } from "react";

interface UseChatDeepLinksOptions {
  readonly activeSessionId: string;
  readonly preferredSessionId: string | null;
  readonly preferredRunId: string | null;
  readonly preferredCompactionId: string | null;
  readonly preferredCheckpointId: string | null;
  readonly openRunDetails: (runId: string) => void;
  readonly inspectCompaction: (artifactId: string) => Promise<void>;
  readonly inspectCheckpoint: (checkpointId: string) => Promise<void>;
}

export function useChatDeepLinks({
  activeSessionId,
  preferredSessionId,
  preferredRunId,
  preferredCompactionId,
  preferredCheckpointId,
  openRunDetails,
  inspectCompaction,
  inspectCheckpoint,
}: UseChatDeepLinksOptions): void {
  const deepLinkedRunRef = useRef<string | null>(null);
  const deepLinkedCompactionRef = useRef<string | null>(null);
  const deepLinkedCheckpointRef = useRef<string | null>(null);

  useEffect(() => {
    if (preferredRunId === null || preferredRunId.trim().length === 0) {
      deepLinkedRunRef.current = null;
      return;
    }
    if (activeSessionId.trim().length === 0) {
      return;
    }
    if (
      preferredSessionId !== null &&
      preferredSessionId.trim().length > 0 &&
      activeSessionId !== preferredSessionId
    ) {
      return;
    }
    if (deepLinkedRunRef.current === preferredRunId) {
      return;
    }
    deepLinkedRunRef.current = preferredRunId;
    openRunDetails(preferredRunId);
  }, [activeSessionId, openRunDetails, preferredRunId, preferredSessionId]);

  useEffect(() => {
    if (preferredCompactionId === null || preferredCompactionId.trim().length === 0) {
      deepLinkedCompactionRef.current = null;
      return;
    }
    if (activeSessionId.trim().length === 0) {
      return;
    }
    if (
      preferredSessionId !== null &&
      preferredSessionId.trim().length > 0 &&
      activeSessionId !== preferredSessionId
    ) {
      return;
    }
    if (deepLinkedCompactionRef.current === preferredCompactionId) {
      return;
    }
    deepLinkedCompactionRef.current = preferredCompactionId;
    void inspectCompaction(preferredCompactionId);
  }, [activeSessionId, inspectCompaction, preferredCompactionId, preferredSessionId]);

  useEffect(() => {
    if (preferredCheckpointId === null || preferredCheckpointId.trim().length === 0) {
      deepLinkedCheckpointRef.current = null;
      return;
    }
    if (activeSessionId.trim().length === 0) {
      return;
    }
    if (
      preferredSessionId !== null &&
      preferredSessionId.trim().length > 0 &&
      activeSessionId !== preferredSessionId
    ) {
      return;
    }
    if (deepLinkedCheckpointRef.current === preferredCheckpointId) {
      return;
    }
    deepLinkedCheckpointRef.current = preferredCheckpointId;
    void inspectCheckpoint(preferredCheckpointId);
  }, [activeSessionId, inspectCheckpoint, preferredCheckpointId, preferredSessionId]);
}
