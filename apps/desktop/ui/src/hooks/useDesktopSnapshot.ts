import { startTransition, useEffect, useEffectEvent, useRef, useState } from "react";

import {
  DESKTOP_PREVIEW_SNAPSHOT,
  type ControlCenterSnapshot,
  getSnapshot,
} from "../lib/desktopApi";

type SnapshotState = {
  error: string | null;
  loading: boolean;
  previewMode: boolean;
  refresh: () => Promise<void>;
  snapshot: ControlCenterSnapshot;
};

const ACTIVE_REFRESH_INTERVAL_MS = 4_000;
const IDLE_REFRESH_INTERVAL_MS = 12_000;

function resolveInterval(snapshot: ControlCenterSnapshot): number {
  return snapshot.overall_status === "down" ? IDLE_REFRESH_INTERVAL_MS : ACTIVE_REFRESH_INTERVAL_MS;
}

export function useDesktopSnapshot(): SnapshotState {
  const [snapshot, setSnapshot] = useState<ControlCenterSnapshot>(DESKTOP_PREVIEW_SNAPSHOT);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [previewMode, setPreviewMode] = useState(false);
  const timerRef = useRef<number | null>(null);
  const intervalRef = useRef<number>(resolveInterval(DESKTOP_PREVIEW_SNAPSHOT));

  const refresh = useEffectEvent(async (): Promise<void> => {
    try {
      const next = await getSnapshot();
      intervalRef.current = resolveInterval(next);
      startTransition(() => {
        setSnapshot(next);
        setPreviewMode(false);
        setError(null);
      });
    } catch (failure) {
      const message = failure instanceof Error ? failure.message : String(failure);
      intervalRef.current = resolveInterval(DESKTOP_PREVIEW_SNAPSHOT);
      startTransition(() => {
        setSnapshot(DESKTOP_PREVIEW_SNAPSHOT);
        setPreviewMode(true);
        setError(message);
      });
    } finally {
      setLoading(false);
    }
  });

  // Effect Events always read current state; restarting this effect would bypass the timer.
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
  }, []);

  return { snapshot, loading, error, previewMode, refresh };
}
