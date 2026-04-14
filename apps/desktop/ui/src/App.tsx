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
  emitDesktopCompanionUxEvent,
  enrollDesktopNode,
  getDesktopCompanionSessionTranscript,
  isDesktopHostAvailable,
  markDesktopCompanionNotificationsRead,
  openDashboard,
  openDesktopCompanionHandoff,
  repairDesktopNode,
  resetDesktopNode,
  removeDesktopCompanionOfflineDraft,
  resolveDesktopCompanionChatSession,
  restartPalyra,
  sendDesktopCompanionChatMessage,
  showMainWindow,
  startPalyra,
  stopPalyra,
  switchDesktopCompanionProfile,
  transcribeDesktopCompanionAudio,
  updateDesktopCompanionPreferences,
  updateDesktopCompanionRollout,
  type ActionResult,
  type DesktopCompanionAudioTranscriptionResult,
  type DesktopCompanionSection,
  type DesktopSessionTranscriptEnvelope,
  type InventoryDeviceRecord,
  type JsonValue,
} from "./lib/desktopApi";
import { formatDesktopDateTime, translateDesktopMessage } from "./i18n";
import { readStoredDesktopLocale, type DesktopLocale } from "./preferences";

const SECTION_ORDER: DesktopCompanionSection[] = [
  "home",
  "chat",
  "approvals",
  "access",
  "onboarding",
];
const VOICE_CAPTURE_CONSENT_KEY = "palyra.desktop.voice.capture-consent.v1";
const VOICE_TTS_CONSENT_KEY = "palyra.desktop.voice.tts-consent.v1";

export function App() {
  const { snapshot, loading, error, previewMode, refresh } = useDesktopCompanion();
  const [locale, setLocale] = useState<DesktopLocale>(() => readStoredDesktopLocale());
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
  const [voiceBusy, setVoiceBusy] = useState(false);
  const [voiceRecording, setVoiceRecording] = useState(false);
  const [voiceTranscript, setVoiceTranscript] =
    useState<DesktopCompanionAudioTranscriptionResult | null>(null);
  const [voiceConsentGranted, setVoiceConsentGranted] = useState(false);
  const [ttsConsentGranted, setTtsConsentGranted] = useState(false);
  const [speaking, setSpeaking] = useState(false);
  const [recordingElapsedMs, setRecordingElapsedMs] = useState(0);
  const [approvalBusy, setApprovalBusy] = useState(false);
  const [nodeActionBusy, setNodeActionBusy] = useState(false);
  const [profileSwitchBusy, setProfileSwitchBusy] = useState(false);
  const [transcript, setTranscript] = useState<DesktopSessionTranscriptEnvelope | null>(null);
  const [notificationPermission, setNotificationPermission] = useState<NotificationPermission>(
    typeof window !== "undefined" && "Notification" in window ? Notification.permission : "denied",
  );
  const announcedNotificationIdsRef = useRef<Set<string>>(new Set());
  const mainWindowShownRef = useRef(false);
  const mediaRecorderRef = useRef<MediaRecorder | null>(null);
  const recordingStreamRef = useRef<MediaStream | null>(null);
  const recordingChunksRef = useRef<Blob[]>([]);
  const recordingStartedAtRef = useRef<number | null>(null);
  const speechUtteranceRef = useRef<SpeechSynthesisUtterance | null>(null);

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
    snapshot.approvals.find(
      (approval) => readString(approval, "approval_id") === selectedApprovalId,
    ) ??
    snapshot.approvals[0] ??
    null;
  const selectedApprovalIdResolved = readString(selectedApproval, "approval_id") ?? "";
  const unreadNotifications = snapshot.notifications.filter((entry) => !entry.read);
  const activeProfile =
    snapshot.active_profile?.context ?? snapshot.console_session?.profile ?? null;
  const offlineDraftsForSession = snapshot.offline_drafts.filter(
    (draft) => draft.session_id === undefined || draft.session_id === activeSessionId,
  );
  const onboardingProgressLabel = `${snapshot.onboarding.progress_completed}/${snapshot.onboarding.progress_total}`;
  const voiceCaptureSupported =
    typeof navigator !== "undefined" &&
    typeof navigator.mediaDevices?.getUserMedia === "function" &&
    typeof MediaRecorder !== "undefined";
  const ttsPlaybackSupported =
    typeof window !== "undefined" &&
    typeof window.speechSynthesis !== "undefined" &&
    typeof SpeechSynthesisUtterance !== "undefined";
  const latestAssistantNarration = useMemo(
    () => findLatestAssistantNarration(transcript),
    [transcript],
  );
  const nativeCanvasExperiment = snapshot.control_center.diagnostics.experiments.native_canvas;
  const ambientCompanionEnabled =
    snapshot.rollout.voice_capture_enabled || snapshot.rollout.tts_playback_enabled;
  const t = (
    key: Parameters<typeof translateDesktopMessage>[1],
    variables?: Record<string, string | number>,
  ) => translateDesktopMessage(locale, key, variables);
  const emitUxEvent = useEffectEvent(
    async (payload: {
      name: string;
      summary?: string;
      section?: string;
      outcome?: string;
      step?: string;
      toolName?: string;
      sessionId?: string;
      runId?: string;
      deviceId?: string;
      objectiveId?: string;
      canvasId?: string;
      intent?: string;
      source?: string;
    }) => {
      try {
        await emitDesktopCompanionUxEvent({
          ...payload,
          locale,
          source: payload.source ?? "desktop",
        });
      } catch (failure) {
        console.warn("desktop companion UX telemetry failed", failure);
      }
    },
  );

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
      if (
        current.trim().length > 0 &&
        snapshot.session_catalog.some((entry) => entry.session_id === current)
      ) {
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
      if (
        current.trim().length > 0 &&
        snapshot.approvals.some((approval) => readString(approval, "approval_id") === current)
      ) {
        return current;
      }
      return fallbackApprovalId;
    });
  }, [snapshot]);

  useEffect(() => {
    setSessionLabelDraft(selectedSession?.session_label ?? "");
  }, [selectedSession?.session_id]);

  useEffect(() => {
    setVoiceConsentGranted(readStoredConsent(VOICE_CAPTURE_CONSENT_KEY));
    setTtsConsentGranted(readStoredConsent(VOICE_TTS_CONSENT_KEY));
  }, []);

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
    window.localStorage.setItem("palyra.desktop.locale", locale);
    document.documentElement.lang = locale === "qps-ploc" ? "en-XA" : "en";
  }, [locale]);

  useEffect(() => {
    void emitUxEvent({
      name: "ux.surface.opened",
      section: activeSection,
      summary: `Desktop companion opened ${activeSection}`,
    });
  }, [activeSection, emitUxEvent]);

  useEffect(() => {
    if (activeSessionId.trim().length === 0) {
      setTranscript(null);
      setVoiceTranscript(null);
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

  useEffect(() => {
    if (!voiceRecording) {
      setRecordingElapsedMs(0);
      return;
    }
    const intervalId = window.setInterval(() => {
      const startedAt = recordingStartedAtRef.current;
      if (startedAt === null) {
        setRecordingElapsedMs(0);
        return;
      }
      setRecordingElapsedMs(Math.max(0, Date.now() - startedAt));
    }, 250);
    return () => {
      window.clearInterval(intervalId);
    };
  }, [voiceRecording]);

  useEffect(() => {
    if (!snapshot.rollout.voice_capture_enabled && voiceRecording) {
      void stopVoiceCapture();
    }
  }, [snapshot.rollout.voice_capture_enabled, voiceRecording]);

  useEffect(() => {
    if (!snapshot.rollout.tts_playback_enabled && speaking) {
      stopSpeechPlayback();
    }
  }, [snapshot.rollout.tts_playback_enabled, speaking]);

  useEffect(() => {
    return () => {
      const recorder = mediaRecorderRef.current;
      if (recorder !== null && recorder.state !== "inactive") {
        recorder.stop();
      }
      stopRecordingStream(recordingStreamRef.current);
      if (typeof window !== "undefined" && typeof window.speechSynthesis !== "undefined") {
        window.speechSynthesis.cancel();
      }
    };
  }, []);

  async function runAction(
    action: ActionName,
    execute: () => Promise<ActionResult>,
  ): Promise<void> {
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

  async function runNodeAction(execute: () => Promise<ActionResult>): Promise<void> {
    setNodeActionBusy(true);
    try {
      const result = await execute();
      setNotice(result.message);
      await refresh();
    } catch (failure) {
      const message = failure instanceof Error ? failure.message : String(failure);
      setNotice(message);
    } finally {
      setNodeActionBusy(false);
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
        sessionLabel:
          sessionLabelDraft.trim().length > 0 ? sessionLabelDraft.trim() : "Desktop companion",
      });
      setActiveSessionId(session.session_id);
      setActiveSection("chat");
      setSessionLabelDraft(session.session_label ?? "");
      await emitUxEvent({
        name: "ux.session.resumed",
        section: "chat",
        sessionId: session.session_id,
        summary: "Desktop companion created or resumed a chat session",
      });
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
        : (snapshot.offline_drafts.find((draft) => draft.draft_id === draftId)?.text.trim() ?? "");
    if (text.length === 0) {
      setNotice("Message cannot be empty.");
      return;
    }
    setSendBusy(true);
    try {
      await emitUxEvent({
        name: "ux.chat.prompt_submitted",
        section: "chat",
        sessionId: activeSessionId,
        summary: "Desktop companion submitted a chat prompt",
      });
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

  function stopSpeechPlayback(): void {
    if (!ttsPlaybackSupported) {
      return;
    }
    window.speechSynthesis.cancel();
    speechUtteranceRef.current = null;
    setSpeaking(false);
  }

  async function ensureVoiceConsent(): Promise<boolean> {
    if (voiceConsentGranted) {
      return true;
    }
    const accepted = window.confirm(
      "Enable push-to-talk for this desktop profile? Audio is uploaded only after you stop recording, follows existing media retention rules, and ambient listening remains disabled.",
    );
    if (!accepted) {
      return false;
    }
    storeConsent(VOICE_CAPTURE_CONSENT_KEY);
    setVoiceConsentGranted(true);
    return true;
  }

  async function ensureTtsConsent(): Promise<boolean> {
    if (ttsConsentGranted) {
      return true;
    }
    const accepted = window.confirm(
      "Enable desktop speech playback for this profile? Palyra will only read the selected assistant output when you explicitly ask it to.",
    );
    if (!accepted) {
      return false;
    }
    storeConsent(VOICE_TTS_CONSENT_KEY);
    setTtsConsentGranted(true);
    return true;
  }

  async function startVoiceCapture(): Promise<void> {
    if (selectedSession === null) {
      setNotice("Create or select a session before recording voice input.");
      return;
    }
    if (!snapshot.rollout.voice_capture_enabled) {
      setNotice("Voice capture is disabled by rollout configuration.");
      return;
    }
    if (!voiceCaptureSupported) {
      setNotice(
        "This desktop host does not expose the browser microphone APIs required for voice capture.",
      );
      return;
    }
    if (!(await ensureVoiceConsent())) {
      setNotice("Voice capture remains disabled until you explicitly grant consent.");
      return;
    }
    setVoiceBusy(true);
    setVoiceTranscript(null);
    try {
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
      const preferredMimeType = resolvePreferredVoiceMimeType();
      const recorder =
        preferredMimeType === null
          ? new MediaRecorder(stream)
          : new MediaRecorder(stream, { mimeType: preferredMimeType });
      recordingStreamRef.current = stream;
      mediaRecorderRef.current = recorder;
      recordingChunksRef.current = [];
      recorder.ondataavailable = (event) => {
        if (event.data.size > 0) {
          recordingChunksRef.current.push(event.data);
        }
      };
      recorder.start();
      recordingStartedAtRef.current = Date.now();
      setVoiceRecording(true);
      setNotice("Voice capture started. Recording will upload only after you stop it.");
    } catch (failure) {
      const message = failure instanceof Error ? failure.message : String(failure);
      stopRecordingStream(recordingStreamRef.current);
      mediaRecorderRef.current = null;
      recordingStreamRef.current = null;
      setNotice(`Voice capture could not start: ${message}`);
    } finally {
      setVoiceBusy(false);
    }
  }

  async function stopVoiceCapture(): Promise<void> {
    const recorder = mediaRecorderRef.current;
    if (recorder === null) {
      setVoiceRecording(false);
      return;
    }
    setVoiceBusy(true);
    try {
      const audioBlob = await stopRecorderAndCollectBlob(recorder, recordingChunksRef.current);
      const contentType = audioBlob.type || recorder.mimeType || "audio/webm";
      const extension = extensionForAudioMimeType(contentType);
      const result = await transcribeDesktopCompanionAudio({
        sessionId: activeSessionId,
        filename: `desktop-voice-${Date.now()}.${extension}`,
        contentType,
        bytesBase64: await blobToBase64(audioBlob),
        consentAcknowledged: true,
      });
      setVoiceTranscript(result);
      setNotice("Voice capture uploaded and transcribed. Review the transcript before sending it.");
      await refresh();
    } catch (failure) {
      const message = failure instanceof Error ? failure.message : String(failure);
      setNotice(`Voice transcription failed: ${message}`);
    } finally {
      mediaRecorderRef.current = null;
      recordingChunksRef.current = [];
      recordingStartedAtRef.current = null;
      stopRecordingStream(recordingStreamRef.current);
      recordingStreamRef.current = null;
      setVoiceRecording(false);
      setVoiceBusy(false);
    }
  }

  async function speakLatestAssistant(): Promise<void> {
    if (!snapshot.rollout.tts_playback_enabled) {
      setNotice("TTS playback is disabled by rollout configuration.");
      return;
    }
    if (!ttsPlaybackSupported) {
      setNotice("This desktop host does not expose speech synthesis.");
      return;
    }
    if (latestAssistantNarration === null) {
      setNotice("No recent assistant output is available for speech playback.");
      return;
    }
    if (!(await ensureTtsConsent())) {
      setNotice("Speech playback remains disabled until you explicitly grant consent.");
      return;
    }

    stopSpeechPlayback();
    const utterance = new SpeechSynthesisUtterance(latestAssistantNarration);
    utterance.onend = () => {
      speechUtteranceRef.current = null;
      setSpeaking(false);
    };
    utterance.onerror = () => {
      speechUtteranceRef.current = null;
      setSpeaking(false);
      setNotice("Desktop speech playback failed before completion.");
    };
    speechUtteranceRef.current = utterance;
    setSpeaking(true);
    window.speechSynthesis.speak(utterance);
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
      await emitUxEvent({
        name: "ux.approval.resolved",
        section: "approvals",
        outcome: approved ? "approved" : "denied",
        toolName: readString(selectedApproval, "tool_name") ?? undefined,
        sessionId: readString(selectedApproval, "session_id") ?? undefined,
        runId: readString(selectedApproval, "run_id") ?? undefined,
        summary: approved
          ? "Desktop companion approved a pending action"
          : "Desktop companion denied a pending action",
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
    options: {
      sessionId?: string;
      runId?: string;
      deviceId?: string;
      objectiveId?: string;
      canvasId?: string;
      intent?: string;
      source?: string;
    } = {},
  ): Promise<void> {
    try {
      const result = await openDesktopCompanionHandoff({
        section,
        sessionId: options.sessionId,
        runId: options.runId,
        deviceId: options.deviceId,
        objectiveId: options.objectiveId,
        canvasId: options.canvasId,
        intent: options.intent,
        source: options.source ?? "desktop",
      });
      await emitUxEvent({
        name: "ux.handoff.opened",
        section,
        sessionId: options.sessionId,
        runId: options.runId,
        deviceId: options.deviceId,
        objectiveId: options.objectiveId,
        canvasId: options.canvasId,
        intent: options.intent,
        summary: `Desktop companion opened a ${section} browser handoff`,
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
    voice_capture_enabled?: boolean;
    tts_playback_enabled?: boolean;
    release_channel?: string;
  }): Promise<void> {
    try {
      const result = await updateDesktopCompanionRollout({
        companionShellEnabled: next.companion_shell_enabled,
        desktopNotificationsEnabled: next.desktop_notifications_enabled,
        offlineDraftsEnabled: next.offline_drafts_enabled,
        voiceCaptureEnabled: next.voice_capture_enabled,
        ttsPlaybackEnabled: next.tts_playback_enabled,
        releaseChannel: next.release_channel,
      });
      setNotice(result.message);
      await refresh();
    } catch (failure) {
      const message = failure instanceof Error ? failure.message : String(failure);
      setNotice(message);
    }
  }

  async function switchProfile(
    profileName: string,
    strictMode: boolean,
    label: string,
  ): Promise<void> {
    if (previewMode) {
      setNotice("Profile switching is unavailable while preview data is active.");
      return;
    }
    if (
      strictMode &&
      typeof window !== "undefined" &&
      !window.confirm(
        `Switch to strict profile "${label}"? The local runtime will pause so the desktop can rebind safely.`,
      )
    ) {
      return;
    }
    setProfileSwitchBusy(true);
    try {
      const result = await switchDesktopCompanionProfile({
        profileName,
        allowStrictSwitch: strictMode,
      });
      setNotice(result.message);
      await refresh();
    } catch (failure) {
      const message = failure instanceof Error ? failure.message : String(failure);
      setNotice(message);
    } finally {
      setProfileSwitchBusy(false);
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
        eyebrow={t("desktop.header.eyebrow")}
        title={t("desktop.header.title")}
        description={t("desktop.header.description")}
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
            {activeProfile !== null ? (
              <StatusChip tone={toneForProfile(activeProfile.risk_level)}>
                {activeProfile.label} · {activeProfile.environment}
              </StatusChip>
            ) : null}
            {activeProfile?.strict_mode ? (
              <StatusChip tone="warning">Strict posture</StatusChip>
            ) : null}
          </>
        }
        actions={
          <>
            <ButtonGroup className="desktop-action-group">
              <Button variant="secondary" onPress={() => void refresh()} isDisabled={loading}>
                {loading ? t("desktop.header.refreshing") : t("desktop.header.refresh")}
              </Button>
              <Button variant="secondary" onPress={() => void openDashboard()}>
                {t("desktop.header.openDashboard")}
              </Button>
              <Button
                variant="ghost"
                onPress={() => setLocale((current) => (current === "en" ? "qps-ploc" : "en"))}
              >
                {locale === "en"
                  ? t("desktop.header.locale.switchToPseudo")
                  : t("desktop.header.locale.switchToEnglish")}
              </Button>
            </ButtonGroup>
            <ButtonGroup className="desktop-action-group">
              {SECTION_ORDER.map((section) => (
                <Button
                  key={section}
                  variant={section === activeSection ? "primary" : "ghost"}
                  onPress={() => setActiveSection(section)}
                >
                  {labelForSection(section, locale)}
                </Button>
              ))}
            </ButtonGroup>
          </>
        }
      />

      {(previewMode || notice !== null || error !== null || snapshot.warnings.length > 0) && (
        <section className="desktop-notice-stack" aria-label="Desktop notices">
          {previewMode ? (
            <InlineNotice title={t("desktop.notice.preview.title")} tone="warning">
              {t("desktop.notice.preview.body")}
            </InlineNotice>
          ) : null}
          {notice !== null ? (
            <InlineNotice title={t("desktop.notice.result.title")}>{notice}</InlineNotice>
          ) : null}
          {error !== null ? (
            <InlineNotice title={t("desktop.notice.refreshFailed.title")} tone="danger">
              {error}
            </InlineNotice>
          ) : null}
          {snapshot.warnings.length > 0 ? (
            <InlineNotice title={t("desktop.notice.warnings.title")} tone="warning">
              <ul className="desktop-list">
                {snapshot.warnings.map((warning) => (
                  <li key={warning}>{warning}</li>
                ))}
              </ul>
            </InlineNotice>
          ) : null}
          {activeProfile !== null ? (
            <InlineNotice
              title={t("desktop.notice.profile.title", { label: activeProfile.label })}
              tone={activeProfile.strict_mode ? "warning" : "default"}
            >
              {t("desktop.notice.profile.body", {
                environment: activeProfile.environment,
                riskLevel: activeProfile.risk_level,
                mode: activeProfile.mode,
              })}
            </InlineNotice>
          ) : null}
        </section>
      )}

      {activeSection === "home" ? (
        <>
          <section className="desktop-grid">
            <SectionCard
              title="Profiles"
              description="Desktop startup, drafts, session caches, and companion selections are isolated per profile. Switching pauses local runtime activity before the new profile binds."
            >
              <div className="desktop-profile-grid">
                {snapshot.profiles.map((profile) => (
                  <button
                    key={profile.context.name}
                    className={`desktop-profile-card${profile.active ? " is-active" : ""}`}
                    type="button"
                    disabled={profileSwitchBusy || previewMode || profile.active}
                    onClick={() =>
                      void switchProfile(
                        profile.context.name,
                        profile.context.strict_mode,
                        profile.context.label,
                      )
                    }
                  >
                    <div className="desktop-inline-row">
                      <strong>{profile.context.label}</strong>
                      <StatusChip tone={toneForProfile(profile.context.risk_level)}>
                        {profile.context.environment}
                      </StatusChip>
                    </div>
                    <p className="desktop-muted">
                      {profile.context.mode} · risk {profile.context.risk_level}
                      {profile.implicit ? " · desktop-managed" : ""}
                    </p>
                    <div className="desktop-profile-meta">
                      <StatusChip tone={profile.active ? "success" : "default"}>
                        {profile.active ? "Active" : profile.recent ? "Recent" : "Available"}
                      </StatusChip>
                      {profile.context.strict_mode ? (
                        <StatusChip tone="warning">Strict</StatusChip>
                      ) : null}
                      {profile.last_used_at_unix_ms !== undefined ? (
                        <small className="desktop-muted">
                          {formatUnixMs(profile.last_used_at_unix_ms)}
                        </small>
                      ) : null}
                    </div>
                  </button>
                ))}
              </div>
              {snapshot.recent_profiles.length > 0 ? (
                <p className="desktop-muted">
                  Recent order: {snapshot.recent_profiles.join(" · ")}
                </p>
              ) : null}
            </SectionCard>
          </section>

          <DesktopHeader loading={loading} locale={locale} snapshot={snapshot.control_center} />

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
            <AttentionCard
              attentionItems={attentionItems}
              loading={loading}
              previewMode={previewMode}
            />
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
                  {
                    label: "Voice capture",
                    value: snapshot.rollout.voice_capture_enabled ? "enabled" : "disabled",
                  },
                  {
                    label: "TTS playback",
                    value: snapshot.rollout.tts_playback_enabled ? "enabled" : "disabled",
                  },
                  { label: "Release channel", value: snapshot.rollout.release_channel },
                  {
                    label: "Current onboarding step",
                    value: snapshot.onboarding.current_step_title,
                  },
                ]}
              />
              <div className="desktop-inline-row">
                <Button
                  variant="secondary"
                  onPress={() =>
                    void toggleRollout({
                      desktop_notifications_enabled:
                        !snapshot.rollout.desktop_notifications_enabled,
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
                <Button
                  variant="secondary"
                  onPress={() =>
                    void toggleRollout({
                      voice_capture_enabled: !snapshot.rollout.voice_capture_enabled,
                    })
                  }
                >
                  Toggle voice capture
                </Button>
                <Button
                  variant="secondary"
                  onPress={() =>
                    void toggleRollout({
                      tts_playback_enabled: !snapshot.rollout.tts_playback_enabled,
                    })
                  }
                >
                  Toggle TTS playback
                </Button>
              </div>
            </SectionCard>

            <SectionCard
              title="Experimental governance"
              description="Native canvas and ambient companion work stay governed under the same structured A2UI contract with explicit rollback criteria."
            >
              <KeyValueList
                items={[
                  {
                    label: "Structured contract",
                    value: snapshot.control_center.diagnostics.experiments.structured_contract,
                  },
                  {
                    label: "Canvas rollout",
                    value: formatExperimentStage(nativeCanvasExperiment.rollout_stage),
                  },
                  {
                    label: "Canvas feature flag",
                    value: nativeCanvasExperiment.feature_flag,
                  },
                  {
                    label: "Ambient companion",
                    value: ambientCompanionEnabled ? "operator preview" : "dark launch",
                  },
                  { label: "Ambient mode", value: "push-to-talk only" },
                  {
                    label: "Local consent",
                    value:
                      voiceConsentGranted || ttsConsentGranted
                        ? "granted"
                        : "required before first use",
                  },
                ]}
              />
              <InlineNotice
                title="Fail-closed guardrails"
                tone={
                  snapshot.control_center.diagnostics.experiments.fail_closed
                    ? "success"
                    : "warning"
                }
              >
                Ambient listening remains disabled, native canvas can be turned off with{" "}
                <code>canvas_host.enabled=false</code>, and diagnostics stay in the normal control
                plane instead of a side channel.
              </InlineNotice>
              <InlineNotice
                title="Canvas support posture"
                tone={nativeCanvasExperiment.enabled ? "warning" : "default"}
              >
                {nativeCanvasExperiment.support_summary}
              </InlineNotice>
              <div className="desktop-stack desktop-stack--compact">
                <strong>Security review checklist</strong>
                <ul className="desktop-list">
                  {nativeCanvasExperiment.security_review.map((entry) => (
                    <li key={entry}>{entry}</li>
                  ))}
                </ul>
              </div>
              <div className="desktop-stack desktop-stack--compact">
                <strong>Exit criteria</strong>
                <ul className="desktop-list">
                  {nativeCanvasExperiment.exit_criteria.map((entry) => (
                    <li key={entry}>{entry}</li>
                  ))}
                </ul>
              </div>
            </SectionCard>
          </section>

          <ProcessMonitorCard
            browserdProcess={snapshot.control_center.browserd_process}
            gatewayProcess={snapshot.control_center.gateway_process}
            nodeHostProcess={snapshot.control_center.node_host_process}
            loading={loading}
          />

          <section className="desktop-grid desktop-grid--details">
            <SectionCard
              title="Desktop node host"
              description="This first-party node client keeps native capability handoff bound to the local desktop instead of pretending everything happens in the backend."
              actions={
                <ButtonGroup className="desktop-action-group">
                  <Button
                    variant="secondary"
                    isDisabled={
                      nodeActionBusy || snapshot.control_center.quick_facts.node_host.installed
                    }
                    onPress={() => void runNodeAction(enrollDesktopNode)}
                  >
                    Enroll node
                  </Button>
                  <Button
                    variant="secondary"
                    isDisabled={
                      nodeActionBusy || !snapshot.control_center.quick_facts.node_host.installed
                    }
                    onPress={() => void runNodeAction(repairDesktopNode)}
                  >
                    Repair node
                  </Button>
                  <Button
                    variant="ghost"
                    isDisabled={
                      nodeActionBusy || !snapshot.control_center.quick_facts.node_host.installed
                    }
                    onPress={() => void runNodeAction(resetDesktopNode)}
                  >
                    Reset local node
                  </Button>
                </ButtonGroup>
              }
            >
              <KeyValueList
                items={[
                  {
                    label: "Enrollment",
                    value: snapshot.control_center.quick_facts.node_host.installed
                      ? snapshot.control_center.quick_facts.node_host.paired
                        ? "paired"
                        : "installed"
                      : "not enrolled",
                  },
                  {
                    label: "Running",
                    value: snapshot.control_center.quick_facts.node_host.running ? "yes" : "no",
                  },
                  {
                    label: "Device ID",
                    value: snapshot.control_center.quick_facts.node_host.device_id ?? "n/a",
                  },
                  {
                    label: "Node RPC",
                    value: snapshot.control_center.quick_facts.node_host.grpc_url ?? "n/a",
                  },
                  {
                    label: "Certificate expiry",
                    value: formatUnixMs(
                      snapshot.control_center.quick_facts.node_host.cert_expires_at_unix_ms,
                    ),
                  },
                ]}
              />
              <p className="desktop-muted">
                {snapshot.control_center.quick_facts.node_host.detail}
              </p>
            </SectionCard>
          </section>
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
                      intent: "resume_session",
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
                <StatusChip
                  tone={selectedSession.last_run_state === "completed" ? "success" : "default"}
                >
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
                    isDisabled={
                      selectedSession === null || sendBusy || composerText.trim().length === 0
                    }
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
                <div className="desktop-stack desktop-stack--compact">
                  <div className="desktop-inline-row">
                    <div>
                      <p className="desktop-label">Voice experiment</p>
                      <p className="desktop-muted">
                        Manual push-to-talk only. Ambient listening stays disabled.
                      </p>
                    </div>
                    <StatusChip
                      tone={
                        voiceRecording
                          ? "warning"
                          : snapshot.rollout.voice_capture_enabled
                            ? "default"
                            : "danger"
                      }
                    >
                      {voiceRecording
                        ? `Mic live · ${formatDurationMs(recordingElapsedMs)}`
                        : snapshot.rollout.voice_capture_enabled
                          ? "Ready"
                          : "Disabled"}
                    </StatusChip>
                  </div>
                  <div className="desktop-inline-row">
                    <Button
                      variant="secondary"
                      isDisabled={
                        selectedSession === null ||
                        voiceBusy ||
                        voiceRecording ||
                        !snapshot.rollout.voice_capture_enabled
                      }
                      onPress={() => void startVoiceCapture()}
                    >
                      {voiceBusy && !voiceRecording ? "Starting..." : "Record voice"}
                    </Button>
                    <Button
                      variant="ghost"
                      isDisabled={!voiceRecording || voiceBusy}
                      onPress={() => void stopVoiceCapture()}
                    >
                      {voiceBusy && voiceRecording ? "Stopping..." : "Stop recording"}
                    </Button>
                    <Button
                      variant="ghost"
                      isDisabled={
                        latestAssistantNarration === null ||
                        !snapshot.rollout.tts_playback_enabled ||
                        speaking
                      }
                      onPress={() => void speakLatestAssistant()}
                    >
                      Speak latest assistant
                    </Button>
                    <Button
                      variant="ghost"
                      isDisabled={!speaking}
                      onPress={() => stopSpeechPlayback()}
                    >
                      Stop speaking
                    </Button>
                  </div>
                  {!voiceCaptureSupported ? (
                    <p className="desktop-muted">
                      This desktop host does not expose microphone capture APIs.
                    </p>
                  ) : null}
                  {!ttsPlaybackSupported ? (
                    <p className="desktop-muted">
                      This desktop host does not expose speech synthesis APIs.
                    </p>
                  ) : null}
                  {voiceTranscript !== null ? (
                    <article className="desktop-timeline-item">
                      <div className="desktop-inline-row">
                        <strong>Latest voice transcript</strong>
                        <StatusChip tone="success">
                          {voiceTranscript.transcript_language ?? "unknown language"}
                        </StatusChip>
                      </div>
                      <p>{voiceTranscript.transcript_text}</p>
                      {voiceTranscript.transcript_summary ? (
                        <p className="desktop-muted">{voiceTranscript.transcript_summary}</p>
                      ) : null}
                      <p className="desktop-muted">{voiceTranscript.privacy_note}</p>
                      {voiceTranscript.warnings.length > 0 ? (
                        <ul className="desktop-list">
                          {voiceTranscript.warnings.map((warning) => (
                            <li key={warning}>{warning}</li>
                          ))}
                        </ul>
                      ) : null}
                      <div className="desktop-inline-row">
                        <Button
                          variant="secondary"
                          isDisabled={sendBusy}
                          onPress={() => setComposerText(voiceTranscript.transcript_text)}
                        >
                          Use transcript
                        </Button>
                        <Button variant="ghost" onPress={() => setVoiceTranscript(null)}>
                          Discard transcript
                        </Button>
                      </div>
                    </article>
                  ) : null}
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
                <ScrollShadow
                  className="desktop-scroll-list desktop-transcript"
                  hideScrollBar
                  size={48}
                >
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
                          <Button
                            variant="secondary"
                            isDisabled={sendBusy}
                            onPress={() => void sendMessage(draft.draft_id)}
                          >
                            Resend
                          </Button>
                          <Button
                            variant="ghost"
                            isDisabled={sendBusy}
                            onPress={() => void removeOfflineDraft(draft.draft_id)}
                          >
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
                      intent: "approve",
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
                    {
                      label: "Subject type",
                      value: readString(selectedApproval, "subject_type") ?? "n/a",
                    },
                    {
                      label: "Subject ID",
                      value: readString(selectedApproval, "subject_id") ?? "n/a",
                    },
                    {
                      label: "Principal",
                      value: readString(selectedApproval, "principal") ?? "n/a",
                    },
                    {
                      label: "Session",
                      value: readString(selectedApproval, "session_id") ?? "n/a",
                    },
                    { label: "Run", value: readString(selectedApproval, "run_id") ?? "n/a" },
                    {
                      label: "Requested",
                      value: formatUnixMs(readNumber(selectedApproval, "requested_at_unix_ms")),
                    },
                    {
                      label: "Decision",
                      value: readString(selectedApproval, "decision") ?? "pending",
                    },
                  ]}
                />
                <InlineNotice title="Request summary" tone="warning">
                  {readString(selectedApproval, "request_summary") ?? "No summary published."}
                </InlineNotice>
                {selectedApprovalPrompt !== null ? (
                  <KeyValueList
                    items={[
                      {
                        label: "Prompt title",
                        value: readString(selectedApprovalPrompt, "title") ?? "Untitled prompt",
                      },
                      {
                        label: "Prompt summary",
                        value:
                          readString(selectedApprovalPrompt, "summary") ??
                          "No prompt summary published.",
                      },
                      {
                        label: "Risk level",
                        value: readString(selectedApprovalPrompt, "risk_level") ?? "unspecified",
                      },
                      {
                        label: "Timeout",
                        value: `${readString(selectedApprovalPrompt, "timeout_seconds") ?? "n/a"}s`,
                      },
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
                    intent: "inspect_access",
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
                          <StatusChip tone={toneForDevice(device)}>{device.trust_state}</StatusChip>
                        </div>
                        <p className="desktop-muted">
                          {device.client_kind} · {device.platform ?? "unknown platform"}
                        </p>
                        <small className="desktop-muted">
                          {device.capability_summary.available}/{device.capability_summary.total}{" "}
                          capabilities · {formatUnixMs(device.updated_at_unix_ms)}
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
                        intent: "resume_session",
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
                    {
                      label: "Last seen",
                      value: formatUnixMs(selectedDevice.last_seen_at_unix_ms),
                    },
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
            title={t("desktop.onboarding.title")}
            description={t("desktop.onboarding.description")}
            actions={
              <ButtonGroup className="desktop-action-group">
                <Button variant="secondary" onPress={() => void openScopedHandoff("overview")}>
                  {t("desktop.onboarding.browserHandoff")}
                </Button>
                <Button
                  variant="ghost"
                  onPress={() =>
                    void toggleRollout({
                      companion_shell_enabled: !snapshot.rollout.companion_shell_enabled,
                    })
                  }
                >
                  {t("desktop.onboarding.toggleShell")}
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
              <InlineNotice title={t("desktop.onboarding.recoveryHint")} tone="warning">
                {snapshot.onboarding.recovery.message}
              </InlineNotice>
            ) : null}
          </SectionCard>

          <SectionCard
            title={t("desktop.onboarding.readiness.title")}
            description={t("desktop.onboarding.readiness.description")}
          >
            <div className="desktop-stack">
              <MetricCard
                label={t("desktop.onboarding.progress.label")}
                value={onboardingProgressLabel}
                detail={snapshot.onboarding.phase}
                tone={snapshot.onboarding.dashboard_handoff_completed ? "success" : "warning"}
              />
              <MetricCard
                label={t("desktop.onboarding.auth.label")}
                value={
                  snapshot.openai_status.ready
                    ? t("desktop.onboarding.auth.ready")
                    : t("desktop.onboarding.auth.attention")
                }
                detail={snapshot.openai_status.note ?? t("desktop.onboarding.auth.emptyNote")}
                tone={snapshot.openai_status.ready ? "success" : "warning"}
              />
              <MetricCard
                label={t("desktop.onboarding.completion.label")}
                value={formatDesktopDateTime(locale, snapshot.onboarding.completion_unix_ms)}
                detail={t("desktop.onboarding.completion.detail")}
              />
            </div>
          </SectionCard>
        </section>
      ) : null}
    </main>
  );
}

function labelForSection(section: DesktopCompanionSection, locale: DesktopLocale): string {
  switch (section) {
    case "home":
      return translateDesktopMessage(locale, "desktop.section.home");
    case "chat":
      return translateDesktopMessage(locale, "desktop.section.chat");
    case "approvals":
      return translateDesktopMessage(locale, "desktop.section.approvals");
    case "access":
      return translateDesktopMessage(locale, "desktop.section.access");
    case "onboarding":
      return translateDesktopMessage(locale, "desktop.section.onboarding");
  }
}

function formatExperimentStage(stage: string): string {
  switch (stage) {
    case "disabled":
      return "disabled";
    case "operator_preview":
      return "operator preview";
    case "limited_preview":
      return "limited preview";
    default:
      return "dark launch";
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

function toneForProfile(riskLevel: string): "success" | "warning" | "danger" | "default" {
  if (riskLevel === "critical" || riskLevel === "high") {
    return "danger";
  }
  if (riskLevel === "elevated") {
    return "warning";
  }
  if (riskLevel === "low") {
    return "success";
  }
  return "default";
}

function toneForDevice(
  device: InventoryDeviceRecord,
): "success" | "warning" | "danger" | "default" {
  if (device.trust_state === "trusted" && device.presence_state === "ok") {
    return "success";
  }
  if (device.trust_state === "revoked" || device.presence_state === "offline") {
    return "danger";
  }
  return "warning";
}

function findLatestAssistantNarration(
  transcript: DesktopSessionTranscriptEnvelope | null,
): string | null {
  if (transcript === null) {
    return null;
  }
  for (let index = transcript.records.length - 1; index >= 0; index -= 1) {
    const record = transcript.records[index];
    if (!record.event_type.includes("assistant")) {
      continue;
    }
    const summary = describeTranscriptRecord(record).trim();
    if (summary.length > 0) {
      return summary;
    }
  }
  return null;
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
  if (
    eventType.includes("complete") ||
    eventType.includes("summary") ||
    eventType.includes("assistant")
  ) {
    return "desktop-transcript-entry--success";
  }
  return "desktop-transcript-entry--default";
}

function describeTranscriptRecord(
  record: DesktopSessionTranscriptEnvelope["records"][number],
): string {
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

function readStoredConsent(storageKey: string): boolean {
  if (typeof window === "undefined") {
    return false;
  }
  try {
    return window.localStorage.getItem(storageKey) === "granted";
  } catch {
    return false;
  }
}

function storeConsent(storageKey: string): void {
  if (typeof window === "undefined") {
    return;
  }
  try {
    window.localStorage.setItem(storageKey, "granted");
  } catch {
    // Ignore preference persistence failures; explicit consent is still required in-session.
  }
}

function stopRecordingStream(stream: MediaStream | null): void {
  stream?.getTracks().forEach((track) => track.stop());
}

async function stopRecorderAndCollectBlob(recorder: MediaRecorder, chunks: Blob[]): Promise<Blob> {
  if (recorder.state === "inactive") {
    return new Blob(chunks, { type: recorder.mimeType || "audio/webm" });
  }
  return new Promise<Blob>((resolve, reject) => {
    recorder.addEventListener(
      "stop",
      () => {
        resolve(new Blob(chunks, { type: recorder.mimeType || "audio/webm" }));
      },
      { once: true },
    );
    recorder.addEventListener(
      "error",
      () => {
        reject(new Error("voice recorder reported an unexpected media error"));
      },
      { once: true },
    );
    recorder.stop();
  });
}

async function blobToBase64(blob: Blob): Promise<string> {
  const bytes = new Uint8Array(await blob.arrayBuffer());
  let binary = "";
  const chunkSize = 0x8000;
  for (let index = 0; index < bytes.length; index += chunkSize) {
    const chunk = bytes.subarray(index, Math.min(index + chunkSize, bytes.length));
    binary += String.fromCharCode(...chunk);
  }
  return btoa(binary);
}

function resolvePreferredVoiceMimeType(): string | null {
  if (typeof MediaRecorder === "undefined" || typeof MediaRecorder.isTypeSupported !== "function") {
    return null;
  }
  const preferredMimeTypes = [
    "audio/webm;codecs=opus",
    "audio/webm",
    "audio/mp4",
    "audio/ogg;codecs=opus",
  ];
  return preferredMimeTypes.find((value) => MediaRecorder.isTypeSupported(value)) ?? null;
}

function extensionForAudioMimeType(contentType: string): string {
  const normalized = contentType.toLowerCase();
  if (normalized.includes("ogg")) {
    return "ogg";
  }
  if (normalized.includes("mp4")) {
    return "m4a";
  }
  if (normalized.includes("mpeg")) {
    return "mp3";
  }
  return "webm";
}

function formatDurationMs(value: number): string {
  const totalSeconds = Math.max(0, Math.round(value / 1000));
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return `${minutes}:${String(seconds).padStart(2, "0")}`;
}
