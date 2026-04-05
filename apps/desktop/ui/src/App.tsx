import { useEffect, useEffectEvent, useMemo, useRef, useState } from "react";
import { Button, ButtonGroup, Input, ScrollShadow, Spinner, TextArea } from "@heroui/react";

import { AttentionCard } from "./components/AttentionCard";
import { DesktopHeader } from "./components/DesktopHeader";
import { HealthStrip } from "./components/HealthStrip";
import { LifecycleActionBar } from "./components/LifecycleActionBar";
import { ProcessMonitorCard } from "./components/ProcessMonitorCard";
import { QuickFactsCard } from "./components/QuickFactsCard";
import { collectAttentionItems, type ActionName } from "./components/desktopPresentation";
import {
  EmptyState,
  InlineNotice,
  KeyValueList,
  MetricCard,
  PageHeader,
  SectionCard,
  StatusChip,
} from "./components/ui";
import { useDesktopCompanion } from "./hooks/useDesktopCompanion";
import {
  decideDesktopCompanionApproval,
  getDesktopCompanionSessionTranscript,
  isDesktopHostAvailable,
  markDesktopCompanionNotificationsRead,
  openDashboard,
  openDesktopCompanionHandoff,
  removeDesktopCompanionOfflineDraft,
  resolveDesktopCompanionChatSession,
  restartPalyra,
  sendDesktopCompanionChatMessage,
  showMainWindow,
  startPalyra,
  stopPalyra,
  updateDesktopCompanionPreferences,
  updateDesktopCompanionRollout,
  type ActionResult,
  type DesktopCompanionSection,
  type DesktopSessionTranscriptEnvelope,
  type InventoryDeviceRecord,
  type JsonValue,
} from "./lib/desktopApi";

const SECTION_ORDER: DesktopCompanionSection[] = [
  "home",
  "chat",
  "approvals",
  "access",
  "onboarding",
];

export function App() {
  const { snapshot, loading, error, previewMode, refresh } = useDesktopCompanion();
  const [actionState, setActionState] = useState<ActionName>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [activeSection, setActiveSection] = useState<DesktopCompanionSection>("home");
  const [activeSessionId, setActiveSessionId] = useState("");
  const [activeDeviceId, setActiveDeviceId] = useState("");
  const [selectedApprovalId, setSelectedApprovalId] = useState("");
  const [approvalReason, setApprovalReason] = useState("");
  const [sessionLabelDraft, setSessionLabelDraft] = useState("");
  const [composerText, setComposerText] = useState("");
  const [transcriptBusy, setTranscriptBusy] = useState(false);
  const [sendBusy, setSendBusy] = useState(false);
  const [approvalBusy, setApprovalBusy] = useState(false);
  const [transcript, setTranscript] = useState<DesktopSessionTranscriptEnvelope | null>(null);
  const [notificationPermission, setNotificationPermission] = useState<NotificationPermission>(
    typeof window !== "undefined" && "Notification" in window ? Notification.permission : "denied",
  );
  const announcedNotificationIdsRef = useRef<Set<string>>(new Set());
  const mainWindowShownRef = useRef(false);

  const attentionItems = useMemo(
    () => collectAttentionItems(snapshot.control_center),
    [snapshot.control_center],
  );
  const selectedSession =
    snapshot.session_catalog.find((entry) => entry.session_id === activeSessionId) ??
    snapshot.session_catalog[0] ??
    null;
  const selectedDevice =
    snapshot.inventory?.devices.find((entry) => entry.device_id === activeDeviceId) ??
    snapshot.inventory?.devices[0] ??
    null;
  const selectedApproval =
    snapshot.approvals.find((approval) => readString(approval, "approval_id") === selectedApprovalId) ??
    snapshot.approvals[0] ??
    null;
  const selectedApprovalIdResolved = readString(selectedApproval, "approval_id") ?? "";
  const unreadNotifications = snapshot.notifications.filter((entry) => !entry.read);
  const offlineDraftsForSession = snapshot.offline_drafts.filter(
    (draft) => draft.session_id === undefined || draft.session_id === activeSessionId,
  );
  const onboardingProgressLabel = `${snapshot.onboarding.progress_completed}/${snapshot.onboarding.progress_total}`;

  useEffect(() => {
    const fallbackSessionId =
      snapshot.preferences.active_session_id ?? snapshot.session_catalog[0]?.session_id ?? "";
    const fallbackDeviceId =
      snapshot.preferences.active_device_id ?? snapshot.inventory?.devices[0]?.device_id ?? "";
    const fallbackApprovalId = readString(snapshot.approvals[0], "approval_id") ?? "";

    setActiveSection((current) =>
      SECTION_ORDER.includes(current) ? current : snapshot.preferences.active_section,
    );
    setActiveSessionId((current) => {
      if (current.trim().length > 0 && snapshot.session_catalog.some((entry) => entry.session_id === current)) {
        return current;
      }
      return fallbackSessionId;
    });
    setActiveDeviceId((current) => {
      if (
        current.trim().length > 0 &&
        snapshot.inventory?.devices.some((entry) => entry.device_id === current)
      ) {
        return current;
      }
      return fallbackDeviceId;
    });
    setSelectedApprovalId((current) => {
      if (current.trim().length > 0 && snapshot.approvals.some((approval) => readString(approval, "approval_id") === current)) {
        return current;
      }
      return fallbackApprovalId;
    });
  }, [snapshot]);

  useEffect(() => {
    setSessionLabelDraft(selectedSession?.session_label ?? "");
  }, [selectedSession?.session_id]);

  useEffect(() => {
    if (!isDesktopHostAvailable()) {
      return;
    }
    void updateDesktopCompanionPreferences({
      activeSection,
      activeSessionId: activeSessionId || undefined,
      activeDeviceId: activeDeviceId || undefined,
      lastRunId: snapshot.preferences.last_run_id,
    }).catch(() => {});
  }, [activeSection, activeDeviceId, activeSessionId, snapshot.preferences.last_run_id]);

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

  useEffect(() => {
    if (
      !snapshot.rollout.desktop_notifications_enabled ||
      notificationPermission !== "granted" ||
      typeof Notification === "undefined"
    ) {
      return;
    }
    for (const entry of unreadNotifications) {
      if (announcedNotificationIdsRef.current.has(entry.notification_id)) {
        continue;
      }
      announcedNotificationIdsRef.current.add(entry.notification_id);
      new Notification(entry.title, { body: entry.detail });
    }
  }, [notificationPermission, snapshot.rollout.desktop_notifications_enabled, unreadNotifications]);

  useEffect(() => {
    if (activeSessionId.trim().length === 0) {
      setTranscript(null);
      return;
    }
    let cancelled = false;
    const load = async (): Promise<void> => {
      setTranscriptBusy(true);
      try {
        const next = await getDesktopCompanionSessionTranscript(activeSessionId);
        if (!cancelled) {
          setTranscript(next);
        }
      } catch (failure) {
        if (!cancelled) {
          const message = failure instanceof Error ? failure.message : String(failure);
          setNotice(`Transcript refresh failed: ${message}`);
        }
      } finally {
        if (!cancelled) {
          setTranscriptBusy(false);
        }
      }
    };
    void load();
    return () => {
      cancelled = true;
    };
  }, [activeSessionId]);

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

  async function requestNotificationPermission(): Promise<void> {
    if (typeof Notification === "undefined") {
      setNotice("Browser notifications are not available in this desktop host.");
      return;
    }
    const permission = await Notification.requestPermission();
    setNotificationPermission(permission);
    setNotice(
      permission === "granted"
        ? "Desktop notifications enabled."
        : "Desktop notifications were not enabled by the OS/webview.",
    );
  }

  async function markNotificationsRead(): Promise<void> {
    await markDesktopCompanionNotificationsRead();
    await refresh();
  }

  async function createSession(): Promise<void> {
    try {
      const session = await resolveDesktopCompanionChatSession({
        sessionLabel: sessionLabelDraft.trim().length > 0 ? sessionLabelDraft.trim() : "Desktop companion",
      });
      setActiveSessionId(session.session_id);
      setActiveSection("chat");
      setSessionLabelDraft(session.session_label ?? "");
      await refresh();
    } catch (failure) {
      const message = failure instanceof Error ? failure.message : String(failure);
      setNotice(message);
    }
  }

  async function sendMessage(draftId?: string): Promise<void> {
    if (activeSessionId.trim().length === 0) {
      setNotice("Create or select a session before sending a message.");
      return;
    }
    const text =
      draftId === undefined
        ? composerText.trim()
        : snapshot.offline_drafts.find((draft) => draft.draft_id === draftId)?.text.trim() ?? "";
    if (text.length === 0) {
      setNotice("Message cannot be empty.");
      return;
    }
    setSendBusy(true);
    try {
      const result = await sendDesktopCompanionChatMessage({
        sessionId: activeSessionId,
        text,
        sessionLabel: sessionLabelDraft.trim() || undefined,
        queueOnFailure: true,
        draftId,
      });
      setNotice(result.message);
      if (!result.queued_offline && draftId === undefined) {
        setComposerText("");
      }
      await refresh();
      const nextTranscript = await getDesktopCompanionSessionTranscript(activeSessionId);
      setTranscript(nextTranscript);
    } catch (failure) {
      const message = failure instanceof Error ? failure.message : String(failure);
      setNotice(message);
    } finally {
      setSendBusy(false);
    }
  }

  async function removeOfflineDraft(draftId: string): Promise<void> {
    try {
      const result = await removeDesktopCompanionOfflineDraft(draftId);
      setNotice(result.message);
      await refresh();
    } catch (failure) {
      const message = failure instanceof Error ? failure.message : String(failure);
      setNotice(message);
    }
  }

  async function decideApproval(approved: boolean): Promise<void> {
    if (selectedApprovalIdResolved.length === 0) {
      setNotice("Select an approval first.");
      return;
    }
    setApprovalBusy(true);
    try {
      await decideDesktopCompanionApproval({
        approvalId: selectedApprovalIdResolved,
        approved,
        reason: approvalReason.trim() || undefined,
        scope: "once",
      });
      setApprovalReason("");
      await refresh();
    } catch (failure) {
      const message = failure instanceof Error ? failure.message : String(failure);
      setNotice(message);
    } finally {
      setApprovalBusy(false);
    }
  }

  async function openScopedHandoff(
    section: string,
    options: { sessionId?: string; deviceId?: string; runId?: string } = {},
  ): Promise<void> {
    try {
      const result = await openDesktopCompanionHandoff({
        section,
        sessionId: options.sessionId,
        deviceId: options.deviceId,
        runId: options.runId,
      });
      setNotice(result.message);
    } catch (failure) {
      const message = failure instanceof Error ? failure.message : String(failure);
      setNotice(message);
    }
  }

  async function toggleRollout(next: {
    companion_shell_enabled?: boolean;
    desktop_notifications_enabled?: boolean;
    offline_drafts_enabled?: boolean;
    release_channel?: string;
  }): Promise<void> {
    try {
      const result = await updateDesktopCompanionRollout({
        companionShellEnabled: next.companion_shell_enabled,
        desktopNotificationsEnabled: next.desktop_notifications_enabled,
        offlineDraftsEnabled: next.offline_drafts_enabled,
        releaseChannel: next.release_channel,
      });
      setNotice(result.message);
      await refresh();
    } catch (failure) {
      const message = failure instanceof Error ? failure.message : String(failure);
      setNotice(message);
    }
  }

  const selectedApprovalPrompt = readObject(selectedApproval, "prompt");
  const selectedApprovalPolicy = readObject(selectedApproval, "policy_snapshot");

  const onboardingItems = useMemo(
    () => [
      { label: "Current phase", value: snapshot.onboarding.phase },
      { label: "Current step", value: snapshot.onboarding.current_step_title },
      { label: "Progress", value: onboardingProgressLabel },
      {
        label: "Dashboard handoff",
        value: snapshot.onboarding.dashboard_handoff_completed ? "completed" : "pending",
      },
      {
        label: "OpenAI profile",
        value: snapshot.openai_status.default_profile_id ?? "not selected",
      },
      { label: "OpenAI state", value: snapshot.openai_status.state ?? "unknown" },
      { label: "Release channel", value: snapshot.rollout.release_channel },
      {
        label: "Companion shell",
        value: snapshot.rollout.companion_shell_enabled ? "enabled" : "disabled",
      },
    ],
    [onboardingProgressLabel, snapshot],
  );

  return (
    <main className="desktop-root desktop-root--companion">
      <PageHeader
        eyebrow="Desktop Companion"
        title="Palyra companion shell"
        description="One desktop surface for runtime control, active sessions, approvals, trust review, and reconnect-safe drafts."
        status={
          <>
            <StatusChip tone={toneForConnection(snapshot.connection_state)}>
              {snapshot.connection_state}
            </StatusChip>
            <StatusChip tone={snapshot.metrics.pending_approvals > 0 ? "warning" : "default"}>
              {snapshot.metrics.pending_approvals} approvals
            </StatusChip>
            <StatusChip tone={snapshot.metrics.queued_offline_drafts > 0 ? "accent" : "default"}>
              {snapshot.metrics.queued_offline_drafts} queued drafts
            </StatusChip>
            <StatusChip tone={previewMode ? "warning" : "success"}>
              {previewMode ? "Preview data" : snapshot.rollout.release_channel}
            </StatusChip>
          </>
        }
        actions={
          <>
            <ButtonGroup className="desktop-action-group">
              <Button variant="secondary" onPress={() => void refresh()} isDisabled={loading}>
                {loading ? "Refreshing..." : "Refresh"}
              </Button>
              <Button variant="secondary" onPress={() => void openDashboard()}>
                Open dashboard
              </Button>
            </ButtonGroup>
            <ButtonGroup className="desktop-action-group">
              {SECTION_ORDER.map((section) => (
                <Button
                  key={section}
                  variant={section === activeSection ? "primary" : "ghost"}
                  onPress={() => setActiveSection(section)}
                >
                  {labelForSection(section)}
                </Button>
              ))}
            </ButtonGroup>
          </>
        }
      />

      {(previewMode || notice !== null || error !== null || snapshot.warnings.length > 0) && (
        <section className="desktop-notice-stack" aria-label="Desktop notices">
          {previewMode ? (
            <InlineNotice title="Preview data active" tone="warning">
              The Tauri bridge or local runtime data is unavailable, so the companion shell is
              rendering preview data.
            </InlineNotice>
          ) : null}
          {notice !== null ? <InlineNotice title="Desktop action result">{notice}</InlineNotice> : null}
          {error !== null ? (
            <InlineNotice title="Companion refresh failed" tone="danger">
              {error}
            </InlineNotice>
          ) : null}
          {snapshot.warnings.length > 0 ? (
            <InlineNotice title="Companion warnings" tone="warning">
              <ul className="desktop-list">
                {snapshot.warnings.map((warning) => (
                  <li key={warning}>{warning}</li>
                ))}
              </ul>
            </InlineNotice>
          ) : null}
        </section>
      )}

      {activeSection === "home" ? (
        <>
          <DesktopHeader loading={loading} snapshot={snapshot.control_center} />

          <LifecycleActionBar
            actionState={actionState}
            isGatewayRunning={snapshot.control_center.gateway_process.running}
            onAction={(action) =>
              void runAction(
                action,
                action === "start"
                  ? startPalyra
                  : action === "stop"
                    ? stopPalyra
                    : action === "restart"
                      ? restartPalyra
                      : openDashboard,
              )
            }
            onRefresh={() => void refresh()}
          />

          <HealthStrip
            attentionCount={attentionItems.length}
            loading={loading}
            snapshot={snapshot.control_center}
          />

          <section className="desktop-grid desktop-grid--metrics">
            <MetricCard
              label="Active sessions"
              value={snapshot.metrics.active_sessions}
              detail={`${snapshot.metrics.sessions_with_active_runs} with active runs`}
            />
            <MetricCard
              label="Pending approvals"
              value={snapshot.metrics.pending_approvals}
              detail="Desktop inbox shares the same review queue as the web console."
              tone={snapshot.metrics.pending_approvals > 0 ? "warning" : "default"}
            />
            <MetricCard
              label="Trusted devices"
              value={snapshot.metrics.trusted_devices}
              detail={`${snapshot.metrics.stale_devices} stale devices still need review`}
              tone={snapshot.metrics.stale_devices > 0 ? "warning" : "success"}
            />
            <MetricCard
              label="Queued drafts"
              value={snapshot.metrics.queued_offline_drafts}
              detail="Safe offline drafts wait for explicit resend after reconnect."
              tone={snapshot.metrics.queued_offline_drafts > 0 ? "accent" : "default"}
            />
          </section>

          <section className="desktop-grid desktop-grid--details">
            <QuickFactsCard loading={loading} snapshot={snapshot.control_center} />
            <AttentionCard attentionItems={attentionItems} loading={loading} previewMode={previewMode} />
          </section>

          <section className="desktop-grid desktop-grid--details">
            <SectionCard
              title="Notifications"
              description="System and in-app signals for reconnects, run completion, pending approvals, and queued drafts."
              actions={
                <ButtonGroup className="desktop-action-group">
                  <Button
                    variant="secondary"
                    isDisabled={unreadNotifications.length === 0}
                    onPress={() => void markNotificationsRead()}
                  >
                    Mark read
                  </Button>
                  <Button
                    variant="ghost"
                    isDisabled={notificationPermission === "granted"}
                    onPress={() => void requestNotificationPermission()}
                  >
                    {notificationPermission === "default"
                      ? "Enable notifications"
                      : `Notifications: ${notificationPermission}`}
                  </Button>
                </ButtonGroup>
              }
            >
              {snapshot.notifications.length === 0 ? (
                <EmptyState
                  compact
                  title="No notifications yet"
                  description="Run completions, reconnect events, and approval spikes will appear here."
                />
              ) : (
                <ScrollShadow className="desktop-scroll-list" hideScrollBar size={48}>
                  <div className="desktop-stack">
                    {snapshot.notifications
                      .slice()
                      .reverse()
                      .map((entry) => (
                        <article
                          key={entry.notification_id}
                          className={`desktop-timeline-item${entry.read ? "" : " is-unread"}`}
                        >
                          <div className="desktop-inline-row">
                            <strong>{entry.title}</strong>
                            <StatusChip tone={entry.read ? "default" : "accent"}>
                              {entry.kind}
                            </StatusChip>
                          </div>
                          <p className="desktop-muted">{entry.detail}</p>
                          <small className="desktop-muted">
                            {formatUnixMs(entry.created_at_unix_ms)}
                          </small>
                        </article>
                      ))}
                  </div>
                </ScrollShadow>
              )}
            </SectionCard>

            <SectionCard
              title="Companion rollout"
              description="Desktop rollout flags keep the richer companion surface staged without removing the underlying control-center safety rails."
            >
              <KeyValueList
                items={[
                  {
                    label: "Companion shell",
                    value: snapshot.rollout.companion_shell_enabled ? "enabled" : "disabled",
                  },
                  {
                    label: "Desktop notifications",
                    value: snapshot.rollout.desktop_notifications_enabled ? "enabled" : "disabled",
                  },
                  {
                    label: "Offline drafts",
                    value: snapshot.rollout.offline_drafts_enabled ? "enabled" : "disabled",
                  },
                  { label: "Release channel", value: snapshot.rollout.release_channel },
                  { label: "Current onboarding step", value: snapshot.onboarding.current_step_title },
                ]}
              />
              <div className="desktop-inline-row">
                <Button
                  variant="secondary"
                  onPress={() =>
                    void toggleRollout({
                      desktop_notifications_enabled: !snapshot.rollout.desktop_notifications_enabled,
                    })
                  }
                >
                  Toggle notifications
                </Button>
                <Button
                  variant="secondary"
                  onPress={() =>
                    void toggleRollout({
                      offline_drafts_enabled: !snapshot.rollout.offline_drafts_enabled,
                    })
                  }
                >
                  Toggle offline drafts
                </Button>
              </div>
            </SectionCard>
          </section>

          <ProcessMonitorCard
            browserdProcess={snapshot.control_center.browserd_process}
            gatewayProcess={snapshot.control_center.gateway_process}
            loading={loading}
          />
        </>
      ) : null}

      {activeSection === "chat" ? (
        <section className="desktop-grid desktop-grid--details">
          <SectionCard
            title="Sessions"
            description="Recent sessions stay pinned locally so handoff and transcript recall survive desktop restarts."
            actions={
              <ButtonGroup className="desktop-action-group">
                <Button variant="secondary" onPress={() => void createSession()}>
                  New session
                </Button>
                <Button
                  variant="ghost"
                  isDisabled={selectedSession === null}
                  onPress={() =>
                    void openScopedHandoff("chat", {
                      sessionId: selectedSession?.session_id,
                      runId: snapshot.preferences.last_run_id,
                    })
                  }
                >
                  Open in browser
                </Button>
              </ButtonGroup>
            }
            footer={
              <div className="desktop-stack desktop-stack--compact">
                <div className="desktop-stack desktop-stack--compact">
                  <p className="desktop-label">Session label</p>
                  <Input
                    placeholder="Desktop companion"
                    value={sessionLabelDraft}
                    variant="secondary"
                    onChange={(event) => setSessionLabelDraft(event.currentTarget.value)}
                  />
                </div>
                <p className="desktop-muted">
                  Create an explicit session label before handing off to the browser console.
                </p>
              </div>
            }
          >
            {snapshot.session_catalog.length === 0 ? (
              <EmptyState
                compact
                title="No sessions yet"
                description="Create a desktop companion session to keep transcript, approvals, and handoff context together."
                action={
                  <Button variant="secondary" onPress={() => void createSession()}>
                    Create session
                  </Button>
                }
              />
            ) : (
              <div className="desktop-list">
                {snapshot.session_catalog.map((session) => {
                  const isActive = session.session_id === selectedSession?.session_id;
                  return (
                    <button
                      key={session.session_id}
                      className={`desktop-list-button${isActive ? " is-active" : ""}`}
                      type="button"
                      onClick={() => {
                        setActiveSessionId(session.session_id);
                        setSessionLabelDraft(session.session_label ?? "");
                      }}
                    >
                      <div className="desktop-stack desktop-stack--compact">
                        <div className="desktop-inline-row">
                          <strong>{session.title}</strong>
                          <StatusChip tone={session.pending_approvals > 0 ? "warning" : "default"}>
                            {session.pending_approvals} approvals
                          </StatusChip>
                        </div>
                        <p className="desktop-muted">
                          {session.preview ?? "No transcript preview published yet."}
                        </p>
                        <small className="desktop-muted">
                          {session.device_id} · {formatUnixMs(session.updated_at_unix_ms)}
                        </small>
                      </div>
                    </button>
                  );
                })}
              </div>
            )}
          </SectionCard>

          <SectionCard
            title="Conversation"
            description="Desktop keeps the active transcript readable and safely falls back to offline drafts when the control plane is unavailable."
            actions={
              selectedSession === null ? null : (
                <StatusChip tone={selectedSession.last_run_state === "completed" ? "success" : "default"}>
                  {selectedSession.last_run_state ?? "ready"}
                </StatusChip>
              )
            }
            footer={
              <div className="desktop-stack">
                <div className="desktop-stack desktop-stack--compact">
                  <p className="desktop-label">Message</p>
                  <TextArea
                    rows={4}
                    placeholder={
                      selectedSession === null
                        ? "Create or select a session before composing."
                        : "Ask Palyra to continue from the current desktop companion context."
                    }
                    value={composerText}
                    variant="secondary"
                    disabled={selectedSession === null || sendBusy}
                    onChange={(event) => setComposerText(event.currentTarget.value)}
                  />
                </div>
                <div className="desktop-inline-row">
                  <Button
                    variant="secondary"
                    isDisabled={selectedSession === null || sendBusy || composerText.trim().length === 0}
                    onPress={() => void sendMessage()}
                  >
                    {sendBusy ? "Sending..." : "Send message"}
                  </Button>
                  <Button
                    variant="ghost"
                    isDisabled={selectedSession === null}
                    onPress={() => void refresh()}
                  >
                    Refresh transcript
                  </Button>
                </div>
              </div>
            }
          >
            {selectedSession === null ? (
              <EmptyState
                compact
                title="Transcript unavailable"
                description="Select a session from the desktop queue to load its transcript."
              />
            ) : transcriptBusy && transcript === null ? (
              <div className="desktop-loading-stack">
                <Spinner size="sm" />
                <p className="desktop-muted">Loading transcript…</p>
              </div>
            ) : (
              <div className="desktop-stack">
                <div className="desktop-inline-row">
                  <div>
                    <p className="desktop-label">Selected session</p>
                    <strong>{selectedSession.title}</strong>
                  </div>
                  <small className="desktop-muted">
                    Updated {formatUnixMs(selectedSession.updated_at_unix_ms)}
                  </small>
                </div>
                <ScrollShadow className="desktop-scroll-list desktop-transcript" hideScrollBar size={48}>
                  <div className="desktop-stack">
                    {(transcript?.records ?? []).length === 0 ? (
                      <p className="desktop-muted">No transcript records published yet.</p>
                    ) : (
                      (transcript?.records ?? []).map((record) => (
                        <article
                          key={`${record.run_id}:${record.seq}`}
                          className={`desktop-transcript-entry ${toneClassForRecord(record.event_type)}`}
                        >
                          <div className="desktop-inline-row">
                            <strong>{prettyEventName(record.event_type)}</strong>
                            <small className="desktop-muted">
                              {formatUnixMs(record.created_at_unix_ms)}
                            </small>
                          </div>
                          <p>{describeTranscriptRecord(record)}</p>
                        </article>
                      ))
                    )}
                  </div>
                </ScrollShadow>
                {(transcript?.queued_inputs ?? []).length > 0 ? (
                  <div className="desktop-stack desktop-stack--compact">
                    <p className="desktop-label">Queued inputs</p>
                    {(transcript?.queued_inputs ?? []).map((entry) => (
                      <article key={entry.queued_input_id} className="desktop-timeline-item">
                        <div className="desktop-inline-row">
                          <strong>{entry.state}</strong>
                          <small className="desktop-muted">
                            {formatUnixMs(entry.updated_at_unix_ms)}
                          </small>
                        </div>
                        <p className="desktop-muted">{entry.text}</p>
                      </article>
                    ))}
                  </div>
                ) : null}
                {offlineDraftsForSession.length > 0 ? (
                  <div className="desktop-stack desktop-stack--compact">
                    <p className="desktop-label">Offline drafts</p>
                    {offlineDraftsForSession.map((draft) => (
                      <article key={draft.draft_id} className="desktop-timeline-item is-unread">
                        <div className="desktop-inline-row">
                          <strong>Queued during reconnect</strong>
                          <small className="desktop-muted">
                            {formatUnixMs(draft.created_at_unix_ms)}
                          </small>
                        </div>
                        <p>{draft.text}</p>
                        <p className="desktop-muted">{draft.reason}</p>
                        <div className="desktop-inline-row">
                          <Button variant="secondary" isDisabled={sendBusy} onPress={() => void sendMessage(draft.draft_id)}>
                            Resend
                          </Button>
                          <Button variant="ghost" isDisabled={sendBusy} onPress={() => void removeOfflineDraft(draft.draft_id)}>
                            Remove
                          </Button>
                        </div>
                      </article>
                    ))}
                  </div>
                ) : null}
              </div>
            )}
          </SectionCard>
        </section>
      ) : null}

      {activeSection === "approvals" ? (
        <section className="desktop-grid desktop-grid--details">
          <SectionCard
            title="Approval queue"
            description="Pending decisions stay visible on desktop so you can react without losing session context."
            actions={
              <Button variant="secondary" onPress={() => void refresh()} isDisabled={approvalBusy}>
                Refresh approvals
              </Button>
            }
          >
            {snapshot.approvals.length === 0 ? (
              <EmptyState
                compact
                title="No approvals waiting"
                description="Sensitive actions, trust changes, and tool approvals will appear here when they need operator review."
              />
            ) : (
              <div className="desktop-list">
                {snapshot.approvals.map((approval) => {
                  const approvalId = readString(approval, "approval_id") ?? "unknown";
                  const decision = readString(approval, "decision");
                  const isActive = approvalId === selectedApprovalIdResolved;
                  return (
                    <button
                      key={approvalId}
                      className={`desktop-list-button${isActive ? " is-active" : ""}`}
                      type="button"
                      onClick={() => setSelectedApprovalId(approvalId)}
                    >
                      <div className="desktop-stack desktop-stack--compact">
                        <div className="desktop-inline-row">
                          <strong>{readString(approval, "request_summary") ?? approvalId}</strong>
                          <StatusChip tone={decision === null ? "warning" : "default"}>
                            {decision ?? "pending"}
                          </StatusChip>
                        </div>
                        <p className="desktop-muted">
                          {readString(approval, "subject_type") ?? "unknown"} ·{" "}
                          {formatUnixMs(readNumber(approval, "requested_at_unix_ms"))}
                        </p>
                      </div>
                    </button>
                  );
                })}
              </div>
            )}
          </SectionCard>

          <SectionCard
            title="Approval detail"
            description="Keep request metadata, prompt context, and the decision note together on one surface."
            actions={
              selectedApproval === null ? null : (
                <Button
                  variant="ghost"
                  onPress={() =>
                    void openScopedHandoff("approvals", {
                      sessionId: readString(selectedApproval, "session_id") ?? undefined,
                      runId: readString(selectedApproval, "run_id") ?? undefined,
                    })
                  }
                >
                  Open in browser
                </Button>
              )
            }
            footer={
              selectedApproval === null ? null : (
                <div className="desktop-stack">
                  <div className="desktop-stack desktop-stack--compact">
                    <p className="desktop-label">Decision note</p>
                    <Input
                      placeholder="Optional operator reason"
                      value={approvalReason}
                      variant="secondary"
                      onChange={(event) => setApprovalReason(event.currentTarget.value)}
                    />
                  </div>
                  <div className="desktop-inline-row">
                    <Button
                      variant="secondary"
                      isDisabled={approvalBusy || readString(selectedApproval, "decision") !== null}
                      onPress={() => void decideApproval(true)}
                    >
                      {approvalBusy ? "Submitting..." : "Approve"}
                    </Button>
                    <Button
                      variant="ghost"
                      isDisabled={approvalBusy || readString(selectedApproval, "decision") !== null}
                      onPress={() => void decideApproval(false)}
                    >
                      Deny
                    </Button>
                  </div>
                </div>
              )
            }
          >
            {selectedApproval === null ? (
              <EmptyState
                compact
                title="No approval selected"
                description="Choose an approval from the queue to inspect its request and policy context."
              />
            ) : (
              <div className="desktop-stack">
                <KeyValueList
                  items={[
                    { label: "Approval ID", value: selectedApprovalIdResolved },
                    { label: "Subject type", value: readString(selectedApproval, "subject_type") ?? "n/a" },
                    { label: "Subject ID", value: readString(selectedApproval, "subject_id") ?? "n/a" },
                    { label: "Principal", value: readString(selectedApproval, "principal") ?? "n/a" },
                    { label: "Session", value: readString(selectedApproval, "session_id") ?? "n/a" },
                    { label: "Run", value: readString(selectedApproval, "run_id") ?? "n/a" },
                    { label: "Requested", value: formatUnixMs(readNumber(selectedApproval, "requested_at_unix_ms")) },
                    { label: "Decision", value: readString(selectedApproval, "decision") ?? "pending" },
                  ]}
                />
                <InlineNotice title="Request summary" tone="warning">
                  {readString(selectedApproval, "request_summary") ?? "No summary published."}
                </InlineNotice>
                {selectedApprovalPrompt !== null ? (
                  <KeyValueList
                    items={[
                      { label: "Prompt title", value: readString(selectedApprovalPrompt, "title") ?? "Untitled prompt" },
                      { label: "Prompt summary", value: readString(selectedApprovalPrompt, "summary") ?? "No prompt summary published." },
                      { label: "Risk level", value: readString(selectedApprovalPrompt, "risk_level") ?? "unspecified" },
                      { label: "Timeout", value: `${readString(selectedApprovalPrompt, "timeout_seconds") ?? "n/a"}s` },
                    ]}
                  />
                ) : null}
                {selectedApprovalPolicy !== null ? (
                  <pre className="desktop-code-block">
                    {JSON.stringify(selectedApprovalPolicy, null, 2)}
                  </pre>
                ) : null}
              </div>
            )}
          </SectionCard>
        </section>
      ) : null}

      {activeSection === "access" ? (
        <section className="desktop-grid desktop-grid--details">
          <SectionCard
            title="Trusted devices"
            description="Capabilities, trust state, and latest session activity stay readable from the desktop shell."
            actions={
              <Button
                variant="ghost"
                isDisabled={selectedDevice === null}
                onPress={() =>
                  void openScopedHandoff("access", {
                    deviceId: selectedDevice?.device_id,
                    sessionId: selectedDevice?.latest_session_id,
                  })
                }
              >
                Open in browser
              </Button>
            }
          >
            {snapshot.inventory === undefined || snapshot.inventory.devices.length === 0 ? (
              <EmptyState
                compact
                title="No inventory devices"
                description="Device trust and capability summaries will appear here once the local control plane publishes inventory."
              />
            ) : (
              <div className="desktop-list">
                {snapshot.inventory.devices.map((device) => {
                  const isActive = device.device_id === selectedDevice?.device_id;
                  return (
                    <button
                      key={device.device_id}
                      className={`desktop-list-button${isActive ? " is-active" : ""}`}
                      type="button"
                      onClick={() => setActiveDeviceId(device.device_id)}
                    >
                      <div className="desktop-stack desktop-stack--compact">
                        <div className="desktop-inline-row">
                          <strong>{device.device_id}</strong>
                          <StatusChip tone={toneForDevice(device)}>
                            {device.trust_state}
                          </StatusChip>
                        </div>
                        <p className="desktop-muted">
                          {device.client_kind} · {device.platform ?? "unknown platform"}
                        </p>
                        <small className="desktop-muted">
                          {device.capability_summary.available}/{device.capability_summary.total} capabilities ·{" "}
                          {formatUnixMs(device.updated_at_unix_ms)}
                        </small>
                      </div>
                    </button>
                  );
                })}
              </div>
            )}
          </SectionCard>

          <SectionCard
            title="Device detail"
            description="Desktop capability cards make trust review and browser handoff available without opening the full access workspace."
            footer={
              selectedDevice?.latest_session_id === undefined ? null : (
                <div className="desktop-inline-row">
                  <Button
                    variant="secondary"
                    onPress={() => {
                      setActiveSection("chat");
                      setActiveSessionId(selectedDevice.latest_session_id ?? "");
                    }}
                  >
                    Jump to session
                  </Button>
                  <Button
                    variant="ghost"
                    onPress={() =>
                      void openScopedHandoff("chat", {
                        sessionId: selectedDevice.latest_session_id,
                        deviceId: selectedDevice.device_id,
                      })
                    }
                  >
                    Open related session
                  </Button>
                </div>
              )
            }
          >
            {selectedDevice === null ? (
              <EmptyState
                compact
                title="No device selected"
                description="Choose a device from the access list to inspect trust metadata and capabilities."
              />
            ) : (
              <div className="desktop-stack">
                <KeyValueList
                  items={[
                    { label: "Presence", value: selectedDevice.presence_state },
                    { label: "Device status", value: selectedDevice.device_status },
                    { label: "Trust state", value: selectedDevice.trust_state },
                    { label: "Latest session", value: selectedDevice.latest_session_id ?? "n/a" },
                    { label: "Pending pairings", value: String(selectedDevice.pending_pairings) },
                    { label: "Issued by", value: selectedDevice.issued_by },
                    { label: "Fingerprint", value: selectedDevice.identity_fingerprint },
                    { label: "Last event", value: prettyEventName(selectedDevice.last_event_name) },
                    { label: "Last seen", value: formatUnixMs(selectedDevice.last_seen_at_unix_ms) },
                    {
                      label: "Certificate expiry",
                      value: formatUnixMs(selectedDevice.current_certificate_expires_at_unix_ms),
                    },
                  ]}
                />
                {selectedDevice.warnings.length > 0 ? (
                  <InlineNotice title="Trust warnings" tone="warning">
                    <ul className="desktop-list">
                      {selectedDevice.warnings.map((warning) => (
                        <li key={warning}>{warning}</li>
                      ))}
                    </ul>
                  </InlineNotice>
                ) : null}
                <div className="desktop-capability-grid">
                  {selectedDevice.capabilities.map((capability) => (
                    <article key={capability.name} className="desktop-capability-card">
                      <div className="desktop-inline-row">
                        <strong>{capability.name}</strong>
                        <StatusChip tone={capability.available ? "success" : "warning"}>
                          {capability.available ? "available" : "unavailable"}
                        </StatusChip>
                      </div>
                      <p className="desktop-muted">
                        {capability.summary ?? "No capability summary published."}
                      </p>
                    </article>
                  ))}
                </div>
              </div>
            )}
          </SectionCard>
        </section>
      ) : null}

      {activeSection === "onboarding" ? (
        <section className="desktop-grid desktop-grid--details">
          <SectionCard
            title="Onboarding and rollout"
            description="Desktop keeps current onboarding progress, authentication readiness, and release rollout state visible in one place."
            actions={
              <ButtonGroup className="desktop-action-group">
                <Button
                  variant="secondary"
                  onPress={() => void openScopedHandoff("overview")}
                >
                  Browser handoff
                </Button>
                <Button
                  variant="ghost"
                  onPress={() =>
                    void toggleRollout({
                      companion_shell_enabled: !snapshot.rollout.companion_shell_enabled,
                    })
                  }
                >
                  Toggle shell
                </Button>
              </ButtonGroup>
            }
          >
            <KeyValueList items={onboardingItems} />
            <InlineNotice
              title={snapshot.onboarding.current_step_title}
              tone={snapshot.onboarding.dashboard_handoff_completed ? "success" : "warning"}
            >
              {snapshot.onboarding.current_step_detail}
            </InlineNotice>
            {snapshot.onboarding.recovery?.message ? (
              <InlineNotice title="Recovery hint" tone="warning">
                {snapshot.onboarding.recovery.message}
              </InlineNotice>
            ) : null}
          </SectionCard>

          <SectionCard
            title="Readiness"
            description="Completion criteria for the desktop companion release path and operator onboarding handoff."
          >
            <div className="desktop-stack">
              <MetricCard
                label="Onboarding progress"
                value={onboardingProgressLabel}
                detail={snapshot.onboarding.phase}
                tone={snapshot.onboarding.dashboard_handoff_completed ? "success" : "warning"}
              />
              <MetricCard
                label="OpenAI auth"
                value={snapshot.openai_status.ready ? "Ready" : "Attention"}
                detail={snapshot.openai_status.note ?? "No auth note published."}
                tone={snapshot.openai_status.ready ? "success" : "warning"}
              />
              <MetricCard
                label="Last completion"
                value={formatUnixMs(snapshot.onboarding.completion_unix_ms)}
                detail="Persisted locally so desktop can resume after restart."
              />
            </div>
          </SectionCard>
        </section>
      ) : null}
    </main>
  );
}

function labelForSection(section: DesktopCompanionSection): string {
  switch (section) {
    case "home":
      return "Home";
    case "chat":
      return "Chat";
    case "approvals":
      return "Approvals";
    case "access":
      return "Access";
    case "onboarding":
      return "Onboarding";
  }
}

function toneForConnection(connectionState: string): "success" | "warning" | "danger" {
  if (connectionState === "connected") {
    return "success";
  }
  if (connectionState === "reconnecting") {
    return "warning";
  }
  return "danger";
}

function toneForDevice(device: InventoryDeviceRecord): "success" | "warning" | "danger" | "default" {
  if (device.trust_state === "trusted" && device.presence_state === "ok") {
    return "success";
  }
  if (device.trust_state === "revoked" || device.presence_state === "offline") {
    return "danger";
  }
  return "warning";
}

function readString(value: JsonValue | undefined | null, key: string): string | null {
  if (value === null || value === undefined || Array.isArray(value) || typeof value !== "object") {
    return null;
  }
  const candidate = value[key];
  return typeof candidate === "string" && candidate.trim().length > 0 ? candidate : null;
}

function readNumber(value: JsonValue | undefined | null, key: string): number | null {
  if (value === null || value === undefined || Array.isArray(value) || typeof value !== "object") {
    return null;
  }
  const candidate = value[key];
  return typeof candidate === "number" && Number.isFinite(candidate) ? candidate : null;
}

function readObject(
  value: JsonValue | undefined | null,
  key: string,
): { [key: string]: JsonValue } | null {
  if (value === null || value === undefined || Array.isArray(value) || typeof value !== "object") {
    return null;
  }
  const candidate = value[key];
  return candidate !== null && !Array.isArray(candidate) && typeof candidate === "object"
    ? candidate
    : null;
}

function formatUnixMs(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "-";
  }
  return new Date(value).toLocaleString();
}

function prettyEventName(value: string | null | undefined): string {
  if (value === null || value === undefined || value.trim().length === 0) {
    return "Unavailable";
  }
  return value
    .replaceAll(".", " / ")
    .replaceAll("_", " ")
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function toneClassForRecord(eventType: string): string {
  if (eventType.includes("error") || eventType.includes("failed") || eventType.includes("reject")) {
    return "desktop-transcript-entry--danger";
  }
  if (eventType.includes("approval") || eventType.includes("warning")) {
    return "desktop-transcript-entry--warning";
  }
  if (eventType.includes("complete") || eventType.includes("summary") || eventType.includes("assistant")) {
    return "desktop-transcript-entry--success";
  }
  return "desktop-transcript-entry--default";
}

function describeTranscriptRecord(record: DesktopSessionTranscriptEnvelope["records"][number]): string {
  const parsedPayload = parsePayload(record.payload_json);
  const summary =
    readString(parsedPayload, "summary") ??
    readString(parsedPayload, "text") ??
    readString(parsedPayload, "message") ??
    readString(parsedPayload, "detail") ??
    readString(parsedPayload, "status");
  if (summary !== null) {
    return summary;
  }
  return record.payload_json.length > 240
    ? `${record.payload_json.slice(0, 237)}...`
    : record.payload_json;
}

function parsePayload(payloadJson: string): JsonValue | null {
  try {
    return JSON.parse(payloadJson) as JsonValue;
  } catch {
    return null;
  }
}
