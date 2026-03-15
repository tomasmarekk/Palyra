import { useEffect, useEffectEvent, useMemo, useRef, useState } from "react";

import { AttentionCard } from "./components/AttentionCard";
import { DesktopHeader } from "./components/DesktopHeader";
import { HealthStrip } from "./components/HealthStrip";
import { LifecycleActionBar } from "./components/LifecycleActionBar";
import { ProcessMonitorCard } from "./components/ProcessMonitorCard";
import { QuickFactsCard } from "./components/QuickFactsCard";
import { type ActionName, collectAttentionItems } from "./components/desktopPresentation";
import { InlineNotice } from "./components/ui";
import { useDesktopSnapshot } from "./hooks/useDesktopSnapshot";
import {
  isDesktopHostAvailable,
  openDashboard,
  restartPalyra,
  showMainWindow,
  startPalyra,
  stopPalyra,
  type ActionResult
} from "./lib/desktopApi";

export function App() {
  const { snapshot, loading, error, previewMode, refresh } = useDesktopSnapshot();
  const [actionState, setActionState] = useState<ActionName>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const mainWindowShownRef = useRef(false);

  const attentionItems = useMemo(() => collectAttentionItems(snapshot), [snapshot]);

  const revealMainWindow = useEffectEvent(async () => {
    if (mainWindowShownRef.current || !isDesktopHostAvailable()) {
      return;
    }

    try {
      await showMainWindow();
      mainWindowShownRef.current = true;
    } catch (failure) {
      const message = failure instanceof Error ? failure.message : String(failure);
      setNotice(`Desktop window handoff failed: ${message}`);
    }
  });

  useEffect(() => {
    void revealMainWindow();
  }, [revealMainWindow]);

  async function runAction(action: ActionName, execute: () => Promise<ActionResult>): Promise<void> {
    if (action === null) {
      return;
    }

    setActionState(action);
    try {
      const result = await execute();
      setNotice(result.message);
      await refresh();
    } catch (failure) {
      const message = failure instanceof Error ? failure.message : String(failure);
      setNotice(message);
    } finally {
      setActionState(null);
    }
  }

  return (
    <main className="desktop-root">
      <DesktopHeader loading={loading} snapshot={snapshot} />

      <LifecycleActionBar
        actionState={actionState}
        isGatewayRunning={snapshot.gateway_process.running}
        onAction={(action) =>
          void runAction(
            action,
            action === "start"
              ? startPalyra
              : action === "stop"
                ? stopPalyra
                : action === "restart"
                  ? restartPalyra
                  : openDashboard
          )
        }
        onRefresh={() => void refresh()}
      />

      {(previewMode || notice !== null || error !== null) && (
        <section className="desktop-notice-stack" aria-label="Desktop notices">
          {previewMode ? (
            <InlineNotice title="Preview data active" tone="warning">
              The Tauri host bridge is not available in this context, so the desktop surface is
              rendering its local preview snapshot.
            </InlineNotice>
          ) : null}
          {notice !== null ? (
            <InlineNotice title="Desktop action result">{notice}</InlineNotice>
          ) : null}
          {error !== null ? (
            <InlineNotice title="Snapshot refresh failed" tone="danger">
              {error}
            </InlineNotice>
          ) : null}
        </section>
      )}

      <HealthStrip attentionCount={attentionItems.length} loading={loading} snapshot={snapshot} />

      <section className="desktop-grid desktop-grid--details">
        <QuickFactsCard loading={loading} snapshot={snapshot} />
        <AttentionCard
          attentionItems={attentionItems}
          loading={loading}
          previewMode={previewMode}
        />
      </section>

      <ProcessMonitorCard
        browserdProcess={snapshot.browserd_process}
        gatewayProcess={snapshot.gateway_process}
        loading={loading}
      />
    </main>
  );
}
