import { startTransition, useEffect, useEffectEvent, useRef, useState } from "react";

import {
  DESKTOP_PREVIEW_COMPANION_SNAPSHOT,
  type DesktopCompanionSnapshot,
  getDesktopCompanionSnapshot,
  isDesktopHostAvailable,
} from "../lib/desktopApi";

type DesktopCompanionState = {
  error: string | null;
  loading: boolean;
  previewMode: boolean;
  refresh: () => Promise<void>;
  snapshot: DesktopCompanionSnapshot;
};

const ACTIVE_REFRESH_INTERVAL_MS = 5_000;
const IDLE_REFRESH_INTERVAL_MS = 12_000;

function resolveInterval(snapshot: DesktopCompanionSnapshot): number {
  return snapshot.connection_state === "connected"
    ? ACTIVE_REFRESH_INTERVAL_MS
    : IDLE_REFRESH_INTERVAL_MS;
}

export function useDesktopCompanion(): DesktopCompanionState {
  const [snapshot, setSnapshot] = useState<DesktopCompanionSnapshot>(
    DESKTOP_PREVIEW_COMPANION_SNAPSHOT,
  );
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [previewMode, setPreviewMode] = useState(!isDesktopHostAvailable());
  const timerRef = useRef<number | null>(null);
  const intervalRef = useRef<number>(resolveInterval(DESKTOP_PREVIEW_COMPANION_SNAPSHOT));

  const refresh = useEffectEvent(async (): Promise<void> => {
    if (!isDesktopHostAvailable()) {
      intervalRef.current = resolveInterval(DESKTOP_PREVIEW_COMPANION_SNAPSHOT);
      startTransition(() => {
        setSnapshot(DESKTOP_PREVIEW_COMPANION_SNAPSHOT);
        setPreviewMode(true);
        setError(null);
      });
      setLoading(false);
      return;
    }

    try {
      const next = await getDesktopCompanionSnapshot();
      intervalRef.current = resolveInterval(next);
      startTransition(() => {
        setSnapshot(next);
        setPreviewMode(false);
        setError(null);
      });
    } catch (failure) {
      const message = failure instanceof Error ? failure.message : String(failure);
      intervalRef.current = resolveInterval(DESKTOP_PREVIEW_COMPANION_SNAPSHOT);
      startTransition(() => {
        setSnapshot(DESKTOP_PREVIEW_COMPANION_SNAPSHOT);
        setPreviewMode(true);
        setError(message);
      });
    } finally {
      setLoading(false);
    }
  });

  useEffect(() => {
    let cancelled = false;

    const scheduleNext = (): void => {
      if (cancelled) {
        return;
      }

      timerRef.current = window.setTimeout(() => {
        void runLoop();
      }, intervalRef.current);
    };

    const runLoop = async (): Promise<void> => {
      if (cancelled) {
        return;
      }
      await refresh();
      if (cancelled) {
        return;
      }
      scheduleNext();
    };

    void runLoop();
    return () => {
      cancelled = true;
      if (timerRef.current !== null) {
        window.clearTimeout(timerRef.current);
      }
    };
  }, [refresh]);

  return { snapshot, loading, error, previewMode, refresh };
}
