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
  hideDesktopCompanionWindow,
  isDesktopHostAvailable,
  markDesktopCompanionNotificationsRead,
  openDashboard,
  openDesktopCompanionHandoff,
  openExternalUrl,
  repairDesktopNode,
  resetDesktopNode,
  removeDesktopCompanionOfflineDraft,
  resolveDesktopCompanionChatSession,
  restartPalyra,
  sendDesktopCompanionChatMessage,
  showDesktopCompanionWindow,
  startPalyra,
  stopPalyra,
  switchDesktopCompanionProfile,
  transcribeDesktopCompanionAudio,
  updateDesktopCompanionAmbient,
  updateDesktopCompanionPreferences,
  updateDesktopCompanionRollout,
  updateDesktopCompanionVoiceState,
  type ActionResult,
  type DesktopCompanionAudioTranscriptionResult,
  type DesktopCompanionSection,
  type DesktopCompanionSnapshot,
  type DesktopCompanionSurfaceMode,
  type DesktopSessionTranscriptEnvelope,
  type InventoryDeviceRecord,
  type JsonValue,
  type OnboardingPostureEnvelope,
  type OnboardingStepAction,
} from "./lib/desktopApi";
import {
  buildDesktopSessionDetailBadges,
  buildDesktopSessionDetailItems,
  buildDesktopSessionListBadges,
  buildDesktopSessionMeta,
  buildDesktopSessionRecap,
} from "./lib/sessionCatalogPresentation";
import {
  markDesktopFirstSuccessCompleted,
  readDesktopFirstSuccessCompleted,
} from "./firstSuccessState";
import { formatDesktopDateTime, translateDesktopMessage } from "./i18n";
import { readStoredDesktopLocale, type DesktopLocale } from "./preferences";

const SECTION_ORDER: DesktopCompanionSection[] = [
  "home",
  "chat",
  "approvals",
  "access",
  "onboarding",
];
const DESKTOP_FIRST_SUCCESS_PROMPTS = [
  "Summarize the current runtime posture and tell me what needs attention first.",
  "Verify my provider and model setup, then list anything still blocking a real run.",
  "Give me a safe first operator workflow I can run end-to-end from this environment.",
] as const;
const DEFAULT_DESKTOP_SESSION_LABEL = "Desktop companion";
const SILENCE_MONITOR_INTERVAL_MS = 200;
const SILENCE_MONITOR_THRESHOLD_RMS = 0.015;

type DesktopVoiceInputOption = {
  deviceId: string;
  label: string;
};

type DesktopVoiceOutputOption = {
  voiceURI: string;
  label: string;
  lang: string;
  default: boolean;
};

type VoiceCaptureStopReason = "manual" | "silence";

type VoiceSilenceMonitor = {
  audioContext: AudioContext;
  analyser: AnalyserNode;
  source: MediaStreamAudioSourceNode;
  intervalId: number;
  silenceStartedAt: number | null;
};

export function App() {
  const { snapshot, loading, error, previewMode, refresh } = useDesktopCompanion();
  const surfaceMode = useMemo(resolveDesktopSurfaceMode, []);
  const [locale, setLocale] = useState<DesktopLocale>(() => readStoredDesktopLocale());
  const [actionState, setActionState] = useState<ActionName>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [activeSection, setActiveSection] = useState<DesktopCompanionSection>("home");
  const [activeSessionId, setActiveSessionId] = useState("");
  const [activeDeviceId, setActiveDeviceId] = useState("");
  const [selectedApprovalId, setSelectedApprovalId] = useState("");
  const [approvalReason, setApprovalReason] = useState("");
  const [sessionLabelDraft, setSessionLabelDraft] = useState("");
  const [ambientHotkeyDraft, setAmbientHotkeyDraft] = useState("");
  const [composerText, setComposerText] = useState("");
  const [transcriptBusy, setTranscriptBusy] = useState(false);
  const [sendBusy, setSendBusy] = useState(false);
  const [firstSuccessCompleted, setFirstSuccessCompleted] = useState(() =>
    readDesktopFirstSuccessCompleted(),
  );
  const [voiceBusy, setVoiceBusy] = useState(false);
  const [voiceRecording, setVoiceRecording] = useState(false);
  const [voiceTranscript, setVoiceTranscript] =
    useState<DesktopCompanionAudioTranscriptionResult | null>(null);
  const [voiceDraftText, setVoiceDraftText] = useState("");
  const [voiceInputDevices, setVoiceInputDevices] = useState<DesktopVoiceInputOption[]>([]);
  const [voiceOutputVoices, setVoiceOutputVoices] = useState<DesktopVoiceOutputOption[]>([]);
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
  const mediaRecorderRef = useRef<MediaRecorder | null>(null);
  const recordingStreamRef = useRef<MediaStream | null>(null);
  const recordingChunksRef = useRef<Blob[]>([]);
  const recordingStartedAtRef = useRef<number | null>(null);
  const recordingSessionIdRef = useRef<string | null>(null);
  const speechUtteranceRef = useRef<SpeechSynthesisUtterance | null>(null);
  const silenceMonitorRef = useRef<VoiceSilenceMonitor | null>(null);

  const attentionItems = useMemo(
    () => collectAttentionItems(snapshot.control_center),
    [snapshot.control_center],
  );
  const selectedSession =
    snapshot.session_catalog.find((entry) => entry.session_id === activeSessionId) ??
    snapshot.session_catalog[0] ??
    null;
  const selectedProjectContext = selectedSession?.recap.project_context;
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
  const activeRun = snapshot.active_runs[0] ?? null;
  const currentVoiceTranscript = useMemo(
    () => deriveCurrentVoiceTranscript(snapshot, voiceTranscript),
    [snapshot, voiceTranscript],
  );
  const voiceConsentGranted = snapshot.voice.capture_consent_granted_at_unix_ms !== undefined;
  const ttsConsentGranted = snapshot.voice.tts_consent_granted_at_unix_ms !== undefined;
  const selectedVoiceInputLabel =
    snapshot.voice.microphone_device_label ??
    voiceInputDevices.find((device) => device.deviceId === snapshot.voice.microphone_device_id)
      ?.label ??
    "System default microphone";
  const selectedVoiceOutput =
    voiceOutputVoices.find((voice) => voice.voiceURI === snapshot.voice.tts_voice_uri) ??
    voiceOutputVoices.find((voice) => voice.default) ??
    voiceOutputVoices[0] ??
    null;
  const selectedVoiceOutputLabel =
    snapshot.voice.tts_voice_label ??
    (selectedVoiceOutput === null
      ? "System speech voice"
      : `${selectedVoiceOutput.label} (${selectedVoiceOutput.lang})`);
  const nativeCanvasExperiment = snapshot.control_center.diagnostics.experiments.native_canvas;
  const ambientCompanionEnabled = snapshot.rollout.ambient_companion_enabled;
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
  const refreshVoiceInputDevices = useEffectEvent(async () => {
    if (!voiceCaptureSupported || typeof navigator === "undefined") {
      setVoiceInputDevices([]);
      return;
    }
    try {
      const devices = await navigator.mediaDevices.enumerateDevices();
      setVoiceInputDevices(
        devices
          .filter((device) => device.kind === "audioinput")
          .map((device, index) => ({
            deviceId: device.deviceId,
            label: device.label.trim().length > 0 ? device.label : `Microphone ${index + 1}`,
          })),
      );
    } catch (failure) {
      console.warn("desktop companion could not enumerate microphone devices", failure);
      setVoiceInputDevices([]);
    }
  });
  const refreshVoiceOutputVoices = useEffectEvent(() => {
    if (!ttsPlaybackSupported || typeof window === "undefined") {
      setVoiceOutputVoices([]);
      return;
    }
    setVoiceOutputVoices(
      window.speechSynthesis.getVoices().map((voice) => ({
        voiceURI: voice.voiceURI,
        label: voice.name,
        lang: voice.lang,
        default: voice.default,
      })),
    );
  });

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
    setAmbientHotkeyDraft(snapshot.ambient.global_hotkey);
  }, [snapshot.ambient.global_hotkey]);

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

  useEffect(() => {
    if (!isDesktopHostAvailable() || surfaceMode === "main") {
      return;
    }
    void updateDesktopCompanionAmbient({
      lastSurface: surfaceMode,
      clearHotkeyRegistrationError: false,
    }).catch(() => {});
  }, [surfaceMode]);

  useEffect(() => {
    setVoiceDraftText(currentVoiceTranscript?.transcript_text ?? "");
  }, [currentVoiceTranscript?.transcript_text]);

  useEffect(() => {
    void refreshVoiceInputDevices();
  }, [refreshVoiceInputDevices, voiceCaptureSupported, voiceConsentGranted]);

  useEffect(() => {
    if (!voiceCaptureSupported || typeof navigator === "undefined") {
      return;
    }
    const handleDeviceChange = () => {
      void refreshVoiceInputDevices();
    };
    navigator.mediaDevices.addEventListener("devicechange", handleDeviceChange);
    return () => {
      navigator.mediaDevices.removeEventListener("devicechange", handleDeviceChange);
    };
  }, [refreshVoiceInputDevices, voiceCaptureSupported]);

  useEffect(() => {
    refreshVoiceOutputVoices();
    if (!ttsPlaybackSupported || typeof window === "undefined") {
      return;
    }
    const handleVoicesChanged = () => {
      refreshVoiceOutputVoices();
    };
    window.speechSynthesis.addEventListener("voiceschanged", handleVoicesChanged);
    return () => {
      window.speechSynthesis.removeEventListener("voiceschanged", handleVoicesChanged);
    };
  }, [refreshVoiceOutputVoices, ttsConsentGranted, ttsPlaybackSupported]);

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
      stopSilenceMonitor();
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
          sessionLabelDraft.trim().length > 0
            ? sessionLabelDraft.trim()
            : DEFAULT_DESKTOP_SESSION_LABEL,
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

  async function sendMessage(draftId?: string, overrideText?: string): Promise<void> {
    if (activeSessionId.trim().length === 0) {
      setNotice("Create or select a session before sending a message.");
      return;
    }
    const text =
      overrideText !== undefined
        ? overrideText.trim()
        : draftId === undefined
          ? composerText.trim()
          : (snapshot.offline_drafts.find((draft) => draft.draft_id === draftId)?.text.trim() ??
            "");
    if (text.length === 0) {
      setNotice("Message cannot be empty.");
      return;
    }
    setSendBusy(true);
    try {
      if (overrideText !== undefined) {
        await updateDesktopCompanionVoiceState({
          lifecycleState: "sending",
          draftText: text,
          draftSessionId: activeSessionId,
          auditKind: "send_requested",
          auditDetail: "Operator confirmed the reviewed voice draft for delivery.",
          auditSessionId: activeSessionId,
          auditRemoteProcessing: true,
          auditTtsPlayback: false,
        }).catch(() => {});
      }
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
        markDesktopFirstSuccessCompleted();
        setFirstSuccessCompleted(true);
      }
      if (overrideText !== undefined) {
        if (result.queued_offline) {
          await updateDesktopCompanionVoiceState({
            lifecycleState: "review",
            draftText: text,
            draftSessionId: activeSessionId,
            clearError: true,
            auditKind: "send_queued_offline",
            auditDetail:
              "Reviewed voice draft was queued as an offline draft and still requires reconnect before delivery.",
            auditSessionId: activeSessionId,
            auditRemoteProcessing: false,
            auditTtsPlayback: false,
          }).catch(() => {});
        } else {
          await finalizeVoiceDraftSend();
        }
      }
      await refresh();
      const nextTranscript = await getDesktopCompanionSessionTranscript(activeSessionId);
      setTranscript(nextTranscript);
    } catch (failure) {
      const message = failure instanceof Error ? failure.message : String(failure);
      if (overrideText !== undefined) {
        await updateDesktopCompanionVoiceState({
          lifecycleState: "error",
          lastError: message,
          draftText: text,
          draftSessionId: activeSessionId,
          auditKind: "send_failed",
          auditDetail: message,
          auditSessionId: activeSessionId,
          auditRemoteProcessing: true,
          auditTtsPlayback: false,
        }).catch(() => {});
      }
      setNotice(message);
    } finally {
      setSendBusy(false);
    }
  }

  function stopSpeechPlayback(): void {
    if (!ttsPlaybackSupported) {
      return;
    }
    const wasSpeaking = speaking || speechUtteranceRef.current !== null;
    window.speechSynthesis.cancel();
    speechUtteranceRef.current = null;
    setSpeaking(false);
    if (!wasSpeaking) {
      return;
    }
    void updateDesktopCompanionVoiceState({
      lifecycleState: "idle",
      auditKind: "tts_stopped",
      auditDetail: "Desktop speech playback was stopped by the operator.",
      auditSessionId: activeSessionId || undefined,
      auditRemoteProcessing: false,
      auditTtsPlayback: true,
    }).catch(() => {});
  }

  function stopSilenceMonitor(): void {
    const monitor = silenceMonitorRef.current;
    if (monitor === null) {
      return;
    }
    window.clearInterval(monitor.intervalId);
    monitor.source.disconnect();
    monitor.analyser.disconnect();
    void monitor.audioContext.close().catch(() => {});
    silenceMonitorRef.current = null;
  }

  function startSilenceMonitor(stream: MediaStream): void {
    stopSilenceMonitor();
    if (
      typeof AudioContext === "undefined" ||
      !snapshot.rollout.voice_silence_detection_enabled ||
      !snapshot.voice.silence_detection_enabled
    ) {
      return;
    }
    try {
      const audioContext = new AudioContext();
      const source = audioContext.createMediaStreamSource(stream);
      const analyser = audioContext.createAnalyser();
      analyser.fftSize = 2048;
      analyser.smoothingTimeConstant = 0.12;
      source.connect(analyser);
      const samples = new Uint8Array(analyser.fftSize);
      const monitor: VoiceSilenceMonitor = {
        audioContext,
        analyser,
        source,
        intervalId: 0,
        silenceStartedAt: null,
      };
      monitor.intervalId = window.setInterval(() => {
        const recorder = mediaRecorderRef.current;
        if (recorder === null || recorder.state === "inactive") {
          stopSilenceMonitor();
          return;
        }
        analyser.getByteTimeDomainData(samples);
        if (calculateVoiceSilenceRms(samples) < SILENCE_MONITOR_THRESHOLD_RMS) {
          if (monitor.silenceStartedAt === null) {
            monitor.silenceStartedAt = Date.now();
            return;
          }
          if (Date.now() - monitor.silenceStartedAt >= snapshot.voice.silence_timeout_ms) {
            stopSilenceMonitor();
            void stopVoiceCapture("silence");
          }
          return;
        }
        monitor.silenceStartedAt = null;
      }, SILENCE_MONITOR_INTERVAL_MS);
      silenceMonitorRef.current = monitor;
    } catch (failure) {
      console.warn("desktop companion silence detection is unavailable", failure);
    }
  }

  async function updateVoiceAudioSettings(
    payload: Parameters<typeof updateDesktopCompanionVoiceState>[0],
    successNotice?: string,
  ): Promise<void> {
    try {
      const result = await updateDesktopCompanionVoiceState(payload);
      setNotice(successNotice ?? result.message);
      await refresh();
    } catch (failure) {
      const message = failure instanceof Error ? failure.message : String(failure);
      setNotice(message);
    }
  }

  async function selectVoiceInputDevice(deviceId: string): Promise<void> {
    const selectedDevice = voiceInputDevices.find((device) => device.deviceId === deviceId) ?? null;
    await updateVoiceAudioSettings(
      {
        microphoneDeviceId: deviceId,
        microphoneDeviceLabel: selectedDevice?.label ?? "",
        auditKind: "input_device_selected",
        auditDetail:
          selectedDevice === null
            ? "Desktop microphone selection was reset to the system default device."
            : `Desktop microphone was set to ${selectedDevice.label}.`,
        auditSessionId: activeSessionId || undefined,
        auditRemoteProcessing: false,
        auditTtsPlayback: false,
      },
      selectedDevice === null
        ? "Desktop microphone reset to the system default input."
        : `Desktop microphone set to ${selectedDevice.label}.`,
    );
  }

  async function selectVoiceOutputVoice(voiceURI: string): Promise<void> {
    const selectedVoice = voiceOutputVoices.find((voice) => voice.voiceURI === voiceURI) ?? null;
    await updateVoiceAudioSettings(
      {
        ttsVoiceUri: voiceURI,
        ttsVoiceLabel: selectedVoice?.label ?? "",
        auditKind: "tts_voice_selected",
        auditDetail:
          selectedVoice === null
            ? "Desktop speech playback reverted to the system default voice."
            : `Desktop speech playback voice was set to ${selectedVoice.label}.`,
        auditSessionId: activeSessionId || undefined,
        auditRemoteProcessing: false,
        auditTtsPlayback: true,
      },
      selectedVoice === null
        ? "Desktop speech playback reverted to the system default voice."
        : `Desktop speech playback voice set to ${selectedVoice.label}.`,
    );
  }

  async function toggleTtsMute(): Promise<void> {
    const nextMuted = !snapshot.voice.tts_muted;
    if (nextMuted) {
      stopSpeechPlayback();
    }
    await updateVoiceAudioSettings(
      {
        ttsMuted: nextMuted,
        auditKind: nextMuted ? "tts_muted" : "tts_unmuted",
        auditDetail: nextMuted
          ? "Desktop speech playback was muted."
          : "Desktop speech playback was unmuted.",
        auditSessionId: activeSessionId || undefined,
        auditRemoteProcessing: false,
        auditTtsPlayback: true,
      },
      nextMuted ? "Desktop speech playback muted." : "Desktop speech playback unmuted.",
    );
  }

  async function toggleSilenceDetection(): Promise<void> {
    if (!snapshot.rollout.voice_silence_detection_enabled) {
      setNotice("Silence detection is disabled by rollout configuration.");
      return;
    }
    const nextEnabled = !snapshot.voice.silence_detection_enabled;
    await updateVoiceAudioSettings(
      {
        silenceDetectionEnabled: nextEnabled,
        auditKind: nextEnabled ? "silence_detection_enabled" : "silence_detection_disabled",
        auditDetail: nextEnabled
          ? "Optional silence detection was enabled for the desktop voice workflow."
          : "Optional silence detection was disabled for the desktop voice workflow.",
        auditSessionId: activeSessionId || undefined,
        auditRemoteProcessing: false,
        auditTtsPlayback: false,
      },
      nextEnabled
        ? "Silence detection will stop long pauses automatically."
        : "Silence detection disabled; push-to-talk stops only when you release it.",
    );
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
    await updateDesktopCompanionVoiceState({
      captureConsentGranted: true,
      lifecycleState: "idle",
      auditKind: "consent_granted",
      auditDetail: "Operator granted explicit push-to-talk consent from the desktop companion.",
      auditSessionId: activeSessionId || undefined,
      auditRemoteProcessing: false,
      auditTtsPlayback: false,
    });
    await refresh();
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
    await updateDesktopCompanionVoiceState({
      ttsConsentGranted: true,
      lifecycleState: "idle",
      auditKind: "tts_consent_granted",
      auditDetail:
        "Operator granted explicit desktop speech playback consent from the companion shell.",
      auditSessionId: activeSessionId || undefined,
      auditRemoteProcessing: false,
      auditTtsPlayback: true,
    });
    await refresh();
    return true;
  }

  async function startVoiceCapture(): Promise<void> {
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
    const sessionId = await ensureVoiceSession("voice capture");
    if (sessionId === null) {
      return;
    }
    setVoiceBusy(true);
    setVoiceTranscript(null);
    setVoiceDraftText("");
    try {
      const { stream, usedFallbackInput } = await requestVoiceCaptureStream(
        snapshot.voice.microphone_device_id,
      );
      const activeTrack = stream.getAudioTracks()[0] ?? null;
      const activeInputDeviceId = readVoiceTrackDeviceId(activeTrack);
      const activeInputDeviceLabel =
        activeTrack?.label.trim() ||
        voiceInputDevices.find((device) => device.deviceId === activeInputDeviceId)?.label;
      await updateDesktopCompanionVoiceState({
        lifecycleState: "recording",
        microphonePermissionState: "granted",
        microphoneDeviceId: activeInputDeviceId,
        microphoneDeviceLabel: activeInputDeviceLabel,
        clearError: true,
        auditKind: "capture_started",
        auditDetail: usedFallbackInput
          ? "Push-to-talk recording started on the default microphone because the preferred input was unavailable."
          : "Push-to-talk recording started from the desktop companion.",
        auditSessionId: sessionId,
        auditRemoteProcessing: false,
        auditTtsPlayback: false,
      });
      const preferredMimeType = resolvePreferredVoiceMimeType();
      const recorder =
        preferredMimeType === null
          ? new MediaRecorder(stream)
          : new MediaRecorder(stream, { mimeType: preferredMimeType });
      recordingStreamRef.current = stream;
      mediaRecorderRef.current = recorder;
      recordingChunksRef.current = [];
      recordingSessionIdRef.current = sessionId;
      recorder.ondataavailable = (event) => {
        if (event.data.size > 0) {
          recordingChunksRef.current.push(event.data);
        }
      };
      recorder.start();
      recordingStartedAtRef.current = Date.now();
      void refreshVoiceInputDevices();
      startSilenceMonitor(stream);
      setVoiceRecording(true);
      setNotice(
        usedFallbackInput
          ? "Preferred microphone was unavailable, so recording started on the default input."
          : "Voice capture started. Recording will upload only after you stop it.",
      );
    } catch (failure) {
      const message = failure instanceof Error ? failure.message : String(failure);
      stopRecordingStream(recordingStreamRef.current);
      mediaRecorderRef.current = null;
      recordingStreamRef.current = null;
      recordingSessionIdRef.current = null;
      await updateDesktopCompanionVoiceState({
        lifecycleState: "error",
        microphonePermissionState: deriveVoicePermissionState(failure),
        lastError: message,
        auditKind: "capture_failed",
        auditDetail: message,
        auditSessionId: sessionId,
        auditRemoteProcessing: false,
        auditTtsPlayback: false,
      }).catch(() => {});
      setNotice(`Voice capture could not start: ${message}`);
    } finally {
      setVoiceBusy(false);
    }
  }

  async function stopVoiceCapture(reason: VoiceCaptureStopReason = "manual"): Promise<void> {
    const recorder = mediaRecorderRef.current;
    const sessionId = recordingSessionIdRef.current ?? activeSessionId;
    stopSilenceMonitor();
    if (recorder === null) {
      setVoiceRecording(false);
      return;
    }
    if (sessionId.trim().length === 0) {
      setNotice("Voice recording does not have a valid session context to transcribe into.");
      mediaRecorderRef.current = null;
      recordingChunksRef.current = [];
      recordingStartedAtRef.current = null;
      recordingSessionIdRef.current = null;
      stopRecordingStream(recordingStreamRef.current);
      recordingStreamRef.current = null;
      setVoiceRecording(false);
      return;
    }
    setVoiceBusy(true);
    try {
      const recordingDurationMs =
        recordingStartedAtRef.current === null
          ? undefined
          : Date.now() - recordingStartedAtRef.current;
      await updateDesktopCompanionVoiceState({
        lifecycleState: "transcribing",
        draftSessionId: sessionId || undefined,
        draftDurationMs: recordingDurationMs,
        clearError: true,
        auditKind: reason === "silence" ? "capture_stopped_silence" : "capture_stopped",
        auditDetail:
          reason === "silence"
            ? "Silence detection stopped the current voice capture and moved it into transcription review."
            : "Operator stopped push-to-talk recording and queued transcription.",
        auditSessionId: sessionId || undefined,
        auditRemoteProcessing: false,
        auditTtsPlayback: false,
      }).catch(() => {});
      const audioBlob = await stopRecorderAndCollectBlob(recorder, recordingChunksRef.current);
      const contentType = audioBlob.type || recorder.mimeType || "audio/webm";
      const extension = extensionForAudioMimeType(contentType);
      const result = await transcribeDesktopCompanionAudio({
        sessionId,
        filename: `desktop-voice-${Date.now()}.${extension}`,
        contentType,
        bytesBase64: await blobToBase64(audioBlob),
        consentAcknowledged: true,
      });
      setVoiceTranscript(result);
      setVoiceDraftText(result.transcript_text);
      await updateDesktopCompanionVoiceState({
        lifecycleState: "review",
        draftSessionId: sessionId,
        draftText: result.transcript_text,
        draftSummary: result.transcript_summary,
        draftLanguage: result.transcript_language,
        draftDurationMs: result.transcript_duration_ms,
        clearError: true,
        auditKind: "transcript_ready",
        auditDetail:
          reason === "silence"
            ? "Voice transcript is ready after silence detection stopped the recording."
            : "Voice transcript is ready for explicit operator review.",
        auditSessionId: sessionId,
        auditRemoteProcessing: true,
        auditTtsPlayback: false,
      });
      setNotice(
        reason === "silence"
          ? "Silence detected. The transcript is ready for review before sending."
          : "Voice capture uploaded and transcribed. Review the transcript before sending it.",
      );
      await refresh();
    } catch (failure) {
      const message = failure instanceof Error ? failure.message : String(failure);
      await updateDesktopCompanionVoiceState({
        lifecycleState: "error",
        lastError: message,
        auditKind: "transcription_failed",
        auditDetail: message,
        auditSessionId: sessionId || undefined,
        auditRemoteProcessing: true,
        auditTtsPlayback: false,
      }).catch(() => {});
      setNotice(`Voice transcription failed: ${message}`);
    } finally {
      mediaRecorderRef.current = null;
      recordingChunksRef.current = [];
      recordingStartedAtRef.current = null;
      recordingSessionIdRef.current = null;
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
    if (snapshot.voice.tts_muted) {
      setNotice("Desktop speech playback is muted. Unmute it before starting TTS.");
      return;
    }
    if (!(await ensureTtsConsent())) {
      setNotice("Speech playback remains disabled until you explicitly grant consent.");
      return;
    }

    stopSpeechPlayback();
    const utterance = new SpeechSynthesisUtterance(latestAssistantNarration);
    if (selectedVoiceOutput !== null) {
      const selectedVoice = window.speechSynthesis
        .getVoices()
        .find((voice) => voice.voiceURI === selectedVoiceOutput.voiceURI);
      if (selectedVoice !== undefined) {
        utterance.voice = selectedVoice;
      }
    }
    utterance.onend = () => {
      speechUtteranceRef.current = null;
      setSpeaking(false);
      void updateDesktopCompanionVoiceState({
        lifecycleState: "idle",
        auditKind: "tts_finished",
        auditDetail: "Desktop speech playback completed.",
        auditSessionId: activeSessionId || undefined,
        auditRemoteProcessing: false,
        auditTtsPlayback: true,
      }).catch(() => {});
    };
    utterance.onerror = () => {
      speechUtteranceRef.current = null;
      setSpeaking(false);
      void updateDesktopCompanionVoiceState({
        lifecycleState: "error",
        lastError: "Desktop speech playback failed before completion.",
        auditKind: "tts_failed",
        auditDetail: "Desktop speech playback failed before completion.",
        auditSessionId: activeSessionId || undefined,
        auditRemoteProcessing: false,
        auditTtsPlayback: true,
      }).catch(() => {});
      setNotice("Desktop speech playback failed before completion.");
    };
    void updateDesktopCompanionVoiceState({
      lifecycleState: "speaking",
      ttsVoiceUri: selectedVoiceOutput?.voiceURI,
      ttsVoiceLabel: selectedVoiceOutput?.label,
      clearError: true,
      auditKind: "tts_started",
      auditDetail: "Desktop speech playback started from explicit operator intent.",
      auditSessionId: activeSessionId || undefined,
      auditRemoteProcessing: false,
      auditTtsPlayback: true,
    }).catch(() => {});
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

  async function runSharedOnboardingAction(action: OnboardingStepAction): Promise<void> {
    try {
      switch (action.kind) {
        case "open_console_path":
          await openScopedHandoff(mapConsolePathToDesktopHandoffSection(action.target), {
            intent: "onboarding",
            source: "desktop",
          });
          return;
        case "run_cli_command":
          setNotice(`Run in terminal: ${action.target}`);
          return;
        case "open_desktop_section":
          if (isDesktopCompanionSection(action.target)) {
            setActiveSection(action.target);
            setNotice(`Opened desktop section: ${action.label}.`);
          } else {
            setNotice(`Desktop action target is not available: ${action.target}`);
          }
          return;
        case "read_docs": {
          const result = await openExternalUrl(action.target);
          setNotice(result.message);
          return;
        }
      }
    } catch (failure) {
      const message = failure instanceof Error ? failure.message : String(failure);
      setNotice(message);
    }
  }

  async function toggleRollout(next: {
    companion_shell_enabled?: boolean;
    ambient_companion_enabled?: boolean;
    desktop_notifications_enabled?: boolean;
    offline_drafts_enabled?: boolean;
    voice_capture_enabled?: boolean;
    voice_overlay_enabled?: boolean;
    voice_silence_detection_enabled?: boolean;
    tts_playback_enabled?: boolean;
    release_channel?: string;
  }): Promise<void> {
    try {
      const result = await updateDesktopCompanionRollout({
        companionShellEnabled: next.companion_shell_enabled,
        ambientCompanionEnabled: next.ambient_companion_enabled,
        desktopNotificationsEnabled: next.desktop_notifications_enabled,
        offlineDraftsEnabled: next.offline_drafts_enabled,
        voiceCaptureEnabled: next.voice_capture_enabled,
        voiceOverlayEnabled: next.voice_overlay_enabled,
        voiceSilenceDetectionEnabled: next.voice_silence_detection_enabled,
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

  async function openMainCompanion(): Promise<void> {
    try {
      const result = await showDesktopCompanionWindow("main");
      setNotice(result.message);
    } catch (failure) {
      const message = failure instanceof Error ? failure.message : String(failure);
      setNotice(message);
    }
  }

  async function openQuickPanelSurface(): Promise<void> {
    try {
      const result = await showDesktopCompanionWindow("quick_panel");
      setNotice(result.message);
    } catch (failure) {
      const message = failure instanceof Error ? failure.message : String(failure);
      setNotice(message);
    }
  }

  async function openVoiceOverlaySurface(): Promise<void> {
    try {
      const result = await showDesktopCompanionWindow("voice_overlay");
      setNotice(result.message);
      if (activeSessionId.trim().length === 0) {
        void ensureVoiceSession("voice overlay");
      }
    } catch (failure) {
      const message = failure instanceof Error ? failure.message : String(failure);
      setNotice(message);
    }
  }

  async function closeAmbientSurface(): Promise<void> {
    if (surfaceMode === "main") {
      return;
    }
    try {
      const result = await hideDesktopCompanionWindow(surfaceMode);
      setNotice(result.message);
    } catch (failure) {
      const message = failure instanceof Error ? failure.message : String(failure);
      setNotice(message);
    }
  }

  async function updateAmbientSettings(payload: {
    startOnLoginEnabled?: boolean;
    globalHotkeyEnabled?: boolean;
    globalHotkey?: string;
    clearHotkeyRegistrationError?: boolean;
    lastSurface?: DesktopCompanionSurfaceMode;
  }): Promise<void> {
    try {
      const result = await updateDesktopCompanionAmbient(payload);
      setNotice(result.message);
      await refresh();
    } catch (failure) {
      const message = failure instanceof Error ? failure.message : String(failure);
      setNotice(message);
    }
  }

  async function saveAmbientHotkey(): Promise<void> {
    const nextHotkey = ambientHotkeyDraft.trim();
    if (nextHotkey.length === 0) {
      setNotice("Global hotkey cannot be empty.");
      return;
    }
    await updateAmbientSettings({
      globalHotkey: nextHotkey,
      clearHotkeyRegistrationError: true,
    });
  }

  async function ensureVoiceSession(source: string): Promise<string | null> {
    if (activeSessionId.trim().length > 0) {
      return activeSessionId;
    }
    if (previewMode) {
      setNotice("Voice session bootstrap is unavailable while preview data is active.");
      return null;
    }
    try {
      const session = await resolveDesktopCompanionChatSession({
        sessionLabel:
          sessionLabelDraft.trim().length > 0
            ? sessionLabelDraft.trim()
            : DEFAULT_DESKTOP_SESSION_LABEL,
      });
      setActiveSessionId(session.session_id);
      setActiveSection("chat");
      setSessionLabelDraft(session.session_label ?? "");
      setNotice(`Created a quick session for ${source}.`);
      await emitUxEvent({
        name: "ux.session.resumed",
        section: "chat",
        sessionId: session.session_id,
        source: "desktop",
        summary: `Desktop companion created a quick session for ${source}`,
      });
      void refresh();
      return session.session_id;
    } catch (failure) {
      const message = failure instanceof Error ? failure.message : String(failure);
      setNotice(message);
      return null;
    }
  }

  async function finalizeVoiceDraftSend(): Promise<void> {
    setVoiceTranscript(null);
    setVoiceDraftText("");
    try {
      await updateDesktopCompanionVoiceState({
        clearDraft: true,
        clearError: true,
        lifecycleState: "idle",
        auditKind: "send_completed",
        auditDetail: "Reviewed voice draft was delivered to the active session.",
        auditSessionId: activeSessionId || undefined,
        auditRemoteProcessing: true,
        auditTtsPlayback: false,
      });
      await refresh();
    } catch (failure) {
      console.warn("desktop companion could not finalize the voice draft state", failure);
    }
  }

  async function clearVoiceDraft(): Promise<void> {
    setVoiceTranscript(null);
    setVoiceDraftText("");
    try {
      const result = await updateDesktopCompanionVoiceState({
        clearDraft: true,
        clearError: true,
        lifecycleState: "cancelled",
        auditKind: "review_discarded",
        auditDetail: "Operator discarded the pending voice transcript review.",
        auditSessionId: activeSessionId || undefined,
        auditRemoteProcessing: true,
        auditTtsPlayback: false,
      });
      setNotice(result.message);
      await refresh();
    } catch (failure) {
      const message = failure instanceof Error ? failure.message : String(failure);
      setNotice(message);
    }
  }

  function updateReviewedVoiceDraft(nextValue: string): void {
    setVoiceDraftText(nextValue);
    void updateDesktopCompanionVoiceState({
      lifecycleState: "review",
      draftText: nextValue,
      draftSessionId: activeSessionId || undefined,
      draftSummary: currentVoiceTranscript?.transcript_summary,
      draftLanguage: currentVoiceTranscript?.transcript_language,
      draftDurationMs: currentVoiceTranscript?.transcript_duration_ms,
    }).catch(() => {});
  }

  const selectedApprovalPrompt = readObject(selectedApproval, "prompt");
  const selectedApprovalPolicy = readObject(selectedApproval, "policy_snapshot");
  const selectedApprovalPromptDetails = readObject(selectedApprovalPrompt, "details_json");
  const selectedApprovalToolName =
    readString(selectedApproval, "tool_name") ??
    readString(selectedApprovalPromptDetails, "tool_name");
  const selectedApprovalRiskLevel =
    readString(selectedApprovalPrompt, "risk_level") ?? "unspecified";
  const selectedApprovalPolicyExplanation =
    readString(selectedApprovalPrompt, "policy_explanation") ??
    "This action requires an explicit operator decision under the current tool posture.";
  const selectedApprovalNextStep = buildDesktopApprovalNextStep({
    toolName: selectedApprovalToolName,
    riskLevel: selectedApprovalRiskLevel,
  });

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
  const sharedOnboarding = snapshot.shared_onboarding ?? null;
  const sharedRecommendedStep =
    sharedOnboarding?.steps.find((step) => step.step_id === sharedOnboarding.recommended_step_id) ??
    sharedOnboarding?.steps.find((step) => step.status !== "done" && step.status !== "skipped") ??
    null;
  const sharedRecommendedAction = sharedRecommendedStep?.action ?? null;
  const sharedOnboardingItems = useMemo(
    () => buildSharedOnboardingItems(sharedOnboarding),
    [sharedOnboarding],
  );

  if (surfaceMode === "quick_panel") {
    return (
      <QuickPanelSurface
        activeRun={activeRun}
        activeSessionId={activeSessionId}
        composerText={composerText}
        connectionState={snapshot.connection_state}
        loading={loading}
        notice={notice}
        offlineDrafts={offlineDraftsForSession}
        pendingApprovals={snapshot.approvals}
        previewMode={previewMode}
        quickFacts={snapshot.control_center.quick_facts}
        sendBusy={sendBusy}
        sessionCatalog={snapshot.session_catalog}
        selectedSession={selectedSession}
        setActiveSessionId={setActiveSessionId}
        setComposerText={setComposerText}
        unreadNotifications={unreadNotifications}
        voiceBusy={voiceBusy}
        voiceOverlayEnabled={snapshot.rollout.voice_overlay_enabled}
        voiceRecording={voiceRecording}
        onClose={closeAmbientSurface}
        onCreateSession={createSession}
        onOpenActiveRun={() =>
          void openScopedHandoff("chat", {
            sessionId: activeRun?.session_id,
            runId: activeRun?.run_id,
            intent: "inspect-run",
            source: "desktop",
          })
        }
        onOpenApprovals={() =>
          void openScopedHandoff("approvals", {
            sessionId: selectedSession?.session_id,
            runId: activeRun?.run_id,
            intent: "review-approvals",
            source: "desktop",
          })
        }
        onOpenDashboard={() => void openScopedHandoff("overview", { source: "desktop" })}
        onOpenFullCompanion={openMainCompanion}
        onOpenSelectedSession={() =>
          void openScopedHandoff("chat", {
            sessionId: selectedSession?.session_id,
            runId: selectedSession?.last_run_id,
            intent: "resume-session",
            source: "desktop",
          })
        }
        onOpenVoiceOverlay={openVoiceOverlaySurface}
        onRemoveDraft={(draftId) => void removeOfflineDraft(draftId)}
        onSendMessage={() => void sendMessage()}
        onSendOfflineDraft={(draftId) => void sendMessage(draftId)}
      />
    );
  }

  if (surfaceMode === "voice_overlay") {
    return (
      <VoiceOverlaySurface
        currentVoiceTranscript={currentVoiceTranscript}
        draftText={voiceDraftText}
        lifecycleState={snapshot.voice.lifecycle_state}
        loading={loading}
        notice={notice}
        recordingElapsedMs={recordingElapsedMs}
        selectedSession={selectedSession}
        sendBusy={sendBusy}
        selectedVoiceInputId={snapshot.voice.microphone_device_id ?? ""}
        selectedVoiceInputLabel={selectedVoiceInputLabel}
        selectedVoiceOutputLabel={selectedVoiceOutputLabel}
        selectedVoiceOutputUri={snapshot.voice.tts_voice_uri ?? ""}
        speaking={speaking}
        ttsMuted={snapshot.voice.tts_muted}
        ttsPlaybackSupported={ttsPlaybackSupported}
        voiceAuditLog={snapshot.voice.audit_log}
        voiceBusy={voiceBusy}
        voiceCaptureEnabled={snapshot.rollout.voice_capture_enabled}
        voiceCaptureSupported={voiceCaptureSupported}
        voiceInputDevices={voiceInputDevices}
        voiceRecording={voiceRecording}
        voiceOutputVoices={voiceOutputVoices}
        voicePermissionState={snapshot.voice.microphone_permission_state}
        voiceSilenceDetectionEnabled={snapshot.voice.silence_detection_enabled}
        voiceSilenceDetectionRolloutEnabled={snapshot.rollout.voice_silence_detection_enabled}
        voiceSilenceTimeoutMs={snapshot.voice.silence_timeout_ms}
        onClearVoiceDraft={clearVoiceDraft}
        onClose={closeAmbientSurface}
        onCreateSession={() => void ensureVoiceSession("voice overlay")}
        onOpenFullCompanion={openMainCompanion}
        onSendMessage={() => void sendMessage(undefined, voiceDraftText)}
        onSelectVoiceInputDevice={(deviceId) => void selectVoiceInputDevice(deviceId)}
        onSelectVoiceOutputVoice={(voiceURI) => void selectVoiceOutputVoice(voiceURI)}
        onSpeakLatestAssistant={() => void speakLatestAssistant()}
        onStartVoiceCapture={() => void startVoiceCapture()}
        onStopSpeechPlayback={stopSpeechPlayback}
        onStopVoiceCapture={() => void stopVoiceCapture()}
        onToggleTtsMute={() => void toggleTtsMute()}
        onToggleSilenceDetection={() => void toggleSilenceDetection()}
        onUpdateDraftText={updateReviewedVoiceDraft}
        onUseTranscript={(text) => updateReviewedVoiceDraft(text)}
      />
    );
  }

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
              title="Ambient runtime"
              description="Rollout flags and ambient settings keep tray, quick panel, voice overlay, and global hotkey governed without weakening the existing control-plane guardrails."
              footer={
                <div className="desktop-stack">
                  <div className="desktop-stack desktop-stack--compact">
                    <p className="desktop-label">Global hotkey</p>
                    <Input
                      placeholder="CommandOrControl+Shift+Space"
                      value={ambientHotkeyDraft}
                      variant="secondary"
                      onChange={(event) => setAmbientHotkeyDraft(event.currentTarget.value)}
                    />
                    {snapshot.ambient.hotkey_registration_error ? (
                      <InlineNotice title="Hotkey registration" tone="warning">
                        {snapshot.ambient.hotkey_registration_error}
                      </InlineNotice>
                    ) : null}
                  </div>
                  <div className="desktop-inline-row">
                    <Button variant="secondary" onPress={() => void saveAmbientHotkey()}>
                      Save hotkey
                    </Button>
                    <Button variant="ghost" onPress={() => void openQuickPanelSurface()}>
                      Open quick panel
                    </Button>
                    <Button variant="ghost" onPress={() => void openVoiceOverlaySurface()}>
                      Open voice overlay
                    </Button>
                  </div>
                </div>
              }
            >
              <KeyValueList
                items={[
                  {
                    label: "Companion shell",
                    value: snapshot.rollout.companion_shell_enabled ? "enabled" : "disabled",
                  },
                  {
                    label: "Ambient companion",
                    value: snapshot.rollout.ambient_companion_enabled ? "enabled" : "disabled",
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
                    label: "Start on login",
                    value: snapshot.ambient.start_on_login_enabled ? "enabled" : "disabled",
                  },
                  {
                    label: "Global hotkey",
                    value: snapshot.ambient.global_hotkey_enabled ? "enabled" : "disabled",
                  },
                  { label: "Preferred invoke surface", value: snapshot.ambient.last_surface },
                  {
                    label: "Voice capture",
                    value: snapshot.rollout.voice_capture_enabled ? "enabled" : "disabled",
                  },
                  {
                    label: "Voice overlay",
                    value: snapshot.rollout.voice_overlay_enabled ? "enabled" : "disabled",
                  },
                  {
                    label: "Silence detection rollout",
                    value: snapshot.rollout.voice_silence_detection_enabled
                      ? "enabled"
                      : "disabled",
                  },
                  {
                    label: "TTS playback",
                    value: snapshot.rollout.tts_playback_enabled ? "enabled" : "disabled",
                  },
                  { label: "Saved hotkey", value: snapshot.ambient.global_hotkey },
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
                      ambient_companion_enabled: !snapshot.rollout.ambient_companion_enabled,
                    })
                  }
                >
                  Toggle ambient mode
                </Button>
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
                      voice_overlay_enabled: !snapshot.rollout.voice_overlay_enabled,
                    })
                  }
                >
                  Toggle voice overlay
                </Button>
                <Button
                  variant="secondary"
                  onPress={() =>
                    void toggleRollout({
                      voice_silence_detection_enabled:
                        !snapshot.rollout.voice_silence_detection_enabled,
                    })
                  }
                >
                  Toggle silence detection rollout
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
                <Button
                  variant="ghost"
                  onPress={() =>
                    void updateAmbientSettings({
                      startOnLoginEnabled: !snapshot.ambient.start_on_login_enabled,
                    })
                  }
                >
                  Toggle start on login
                </Button>
                <Button
                  variant="ghost"
                  onPress={() =>
                    void updateAmbientSettings({
                      globalHotkeyEnabled: !snapshot.ambient.global_hotkey_enabled,
                      clearHotkeyRegistrationError: true,
                    })
                  }
                >
                  Toggle global hotkey
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
                      intent: "resume-session",
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
                        {buildDesktopSessionListBadges(session).length > 0 ? (
                          <div className="desktop-inline-row">
                            {buildDesktopSessionListBadges(session).map((badge) => (
                              <StatusChip
                                key={`${session.session_id}:${badge.label}`}
                                tone={badge.tone}
                              >
                                {badge.label}
                              </StatusChip>
                            ))}
                          </div>
                        ) : null}
                        <p className="desktop-muted">
                          {session.preview ?? "No transcript preview published yet."}
                        </p>
                        <small className="desktop-muted">
                          {session.device_id} · {buildDesktopSessionMeta(session)} ·{" "}
                          {formatUnixMs(session.updated_at_unix_ms)}
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
                  {currentVoiceTranscript !== null ? (
                    <article className="desktop-timeline-item">
                      <div className="desktop-inline-row">
                        <strong>Latest voice transcript</strong>
                        <StatusChip tone="success">
                          {currentVoiceTranscript.transcript_language ?? "unknown language"}
                        </StatusChip>
                      </div>
                      <p>{currentVoiceTranscript.transcript_text}</p>
                      {currentVoiceTranscript.transcript_summary ? (
                        <p className="desktop-muted">{currentVoiceTranscript.transcript_summary}</p>
                      ) : null}
                      <p className="desktop-muted">{currentVoiceTranscript.privacy_note}</p>
                      {currentVoiceTranscript.warnings.length > 0 ? (
                        <ul className="desktop-list">
                          {currentVoiceTranscript.warnings.map((warning) => (
                            <li key={warning}>{warning}</li>
                          ))}
                        </ul>
                      ) : null}
                      <div className="desktop-inline-row">
                        <Button
                          variant="secondary"
                          isDisabled={sendBusy}
                          onPress={() => setComposerText(currentVoiceTranscript.transcript_text)}
                        >
                          Use transcript
                        </Button>
                        <Button variant="ghost" onPress={() => void clearVoiceDraft()}>
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
                {buildDesktopSessionDetailBadges(selectedSession).length > 0 ? (
                  <div className="desktop-inline-row">
                    {buildDesktopSessionDetailBadges(selectedSession).map((badge) => (
                      <StatusChip
                        key={`${selectedSession.session_id}:${badge.label}`}
                        tone={badge.tone}
                      >
                        {badge.label}
                      </StatusChip>
                    ))}
                  </div>
                ) : null}
                <KeyValueList items={buildDesktopSessionDetailItems(selectedSession)} />
                {buildDesktopSessionRecap(selectedSession) ? (
                  <InlineNotice title="Resume recap" tone="default">
                    {buildDesktopSessionRecap(selectedSession)}
                  </InlineNotice>
                ) : null}
                {selectedProjectContext !== undefined ? (
                  <div className="desktop-stack desktop-stack--compact">
                    <div className="desktop-inline-row">
                      <p className="desktop-label">Project context</p>
                      <small className="desktop-muted">
                        {selectedProjectContext.active_estimated_tokens.toLocaleString()} est.
                        tokens
                      </small>
                    </div>
                    <div className="desktop-inline-row">
                      <StatusChip tone="accent">
                        {selectedProjectContext.active_entries} active
                      </StatusChip>
                      <StatusChip
                        tone={selectedProjectContext.blocked_entries > 0 ? "danger" : "default"}
                      >
                        {selectedProjectContext.blocked_entries} blocked
                      </StatusChip>
                      <StatusChip
                        tone={selectedProjectContext.warnings.length > 0 ? "warning" : "default"}
                      >
                        {selectedProjectContext.warnings.length} warnings
                      </StatusChip>
                    </div>
                    {selectedProjectContext.focus_paths.length > 0 ? (
                      <div className="desktop-stack desktop-stack--compact">
                        <p className="desktop-label">Discovery</p>
                        {selectedProjectContext.focus_paths.slice(0, 4).map((focus) => (
                          <p key={`${focus.reason}:${focus.path}`} className="desktop-muted">
                            {focus.reason}: {focus.path}
                          </p>
                        ))}
                      </div>
                    ) : null}
                    {selectedProjectContext.entries.slice(0, 4).map((entry) => (
                      <article
                        key={entry.entry_id}
                        className="desktop-transcript-entry desktop-transcript-entry--meta"
                      >
                        <div className="desktop-inline-row">
                          <strong>
                            {entry.order}. {entry.path}
                          </strong>
                          <small className="desktop-muted">
                            {entry.status.replaceAll("_", " ")} · {entry.content_hash.slice(0, 10)}
                          </small>
                        </div>
                        <p>{entry.preview_text}</p>
                        {entry.warnings.length > 0 ? (
                          <p className="desktop-muted">{entry.warnings.join(" ")}</p>
                        ) : null}
                      </article>
                    ))}
                  </div>
                ) : null}
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

          <SectionCard
            title="Voice privacy and audio"
            description="Push-to-talk stays explicit, auditable, and tied to the same session model as the text composer."
            actions={
              <ButtonGroup className="desktop-action-group">
                <Button variant="secondary" onPress={() => void openVoiceOverlaySurface()}>
                  Open voice overlay
                </Button>
                <Button variant="ghost" onPress={() => void refreshVoiceInputDevices()}>
                  Refresh devices
                </Button>
              </ButtonGroup>
            }
            footer={
              <div className="desktop-stack">
                <div className="desktop-grid desktop-grid--details">
                  <label className="desktop-stack desktop-stack--compact">
                    <span className="desktop-label">Microphone device</span>
                    <select
                      aria-label="Desktop microphone device"
                      className="desktop-select"
                      value={snapshot.voice.microphone_device_id ?? ""}
                      onChange={(event) => void selectVoiceInputDevice(event.currentTarget.value)}
                    >
                      <option value="">System default microphone</option>
                      {voiceInputDevices.map((device) => (
                        <option key={device.deviceId} value={device.deviceId}>
                          {device.label}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label className="desktop-stack desktop-stack--compact">
                    <span className="desktop-label">Speech voice</span>
                    <select
                      aria-label="Desktop speech voice"
                      className="desktop-select"
                      value={snapshot.voice.tts_voice_uri ?? ""}
                      onChange={(event) => void selectVoiceOutputVoice(event.currentTarget.value)}
                    >
                      <option value="">System default voice</option>
                      {voiceOutputVoices.map((voice) => (
                        <option key={voice.voiceURI} value={voice.voiceURI}>
                          {voice.label} ({voice.lang})
                        </option>
                      ))}
                    </select>
                  </label>
                </div>
                <div className="desktop-inline-row">
                  <Button variant="secondary" onPress={() => void toggleTtsMute()}>
                    {snapshot.voice.tts_muted ? "Unmute TTS" : "Mute TTS"}
                  </Button>
                  <Button
                    variant="secondary"
                    onPress={() => void toggleSilenceDetection()}
                    isDisabled={!snapshot.rollout.voice_silence_detection_enabled}
                  >
                    {snapshot.voice.silence_detection_enabled
                      ? "Disable silence detection"
                      : "Enable silence detection"}
                  </Button>
                </div>
              </div>
            }
          >
            <KeyValueList
              items={[
                {
                  label: "Capture consent",
                  value: voiceConsentGranted
                    ? formatUnixMs(snapshot.voice.capture_consent_granted_at_unix_ms)
                    : "required before first use",
                },
                {
                  label: "TTS consent",
                  value: ttsConsentGranted
                    ? formatUnixMs(snapshot.voice.tts_consent_granted_at_unix_ms)
                    : "required before first playback",
                },
                {
                  label: "Mic permission",
                  value: snapshot.voice.microphone_permission_state,
                },
                { label: "Input device", value: selectedVoiceInputLabel },
                { label: "Speech voice", value: selectedVoiceOutputLabel },
                { label: "TTS mute", value: snapshot.voice.tts_muted ? "muted" : "live" },
                {
                  label: "Silence detection",
                  value:
                    snapshot.rollout.voice_silence_detection_enabled &&
                    snapshot.voice.silence_detection_enabled
                      ? `${formatDurationMs(snapshot.voice.silence_timeout_ms)} timeout`
                      : snapshot.rollout.voice_silence_detection_enabled
                        ? "disabled"
                        : "rollout disabled",
                },
                { label: "Lifecycle", value: snapshot.voice.lifecycle_state },
              ]}
            />
            <InlineNotice title="Privacy posture" tone="default">
              Audio is captured only during explicit push-to-talk, uploaded only after you stop
              recording, and follows the existing media retention/redaction pipeline. Ambient
              listening stays disabled.
            </InlineNotice>
            {snapshot.voice.last_error ? (
              <InlineNotice title="Last voice error" tone="warning">
                {snapshot.voice.last_error}
              </InlineNotice>
            ) : null}
            {snapshot.voice.audit_log.length === 0 ? (
              <EmptyState
                compact
                title="No voice audit trail yet"
                description="Voice capture, review, send, and playback events will appear here."
              />
            ) : (
              <div className="desktop-stack desktop-stack--compact">
                {snapshot.voice.audit_log
                  .slice()
                  .reverse()
                  .slice(0, 6)
                  .map((entry) => (
                    <article key={entry.audit_id} className="desktop-timeline-item">
                      <div className="desktop-inline-row">
                        <strong>{entry.kind.replaceAll("_", " ")}</strong>
                        <small className="desktop-muted">
                          {formatUnixMs(entry.created_at_unix_ms)}
                        </small>
                      </div>
                      <p>{entry.detail}</p>
                      <div className="desktop-inline-row">
                        <StatusChip tone={entry.remote_processing ? "warning" : "default"}>
                          {entry.remote_processing ? "remote processing" : "local only"}
                        </StatusChip>
                        <StatusChip tone={entry.tts_playback ? "accent" : "default"}>
                          {entry.tts_playback ? "tts" : "mic"}
                        </StatusChip>
                      </div>
                      {entry.input_device_label || entry.output_voice_label ? (
                        <p className="desktop-muted">
                          {[entry.input_device_label, entry.output_voice_label]
                            .filter((value): value is string => Boolean(value))
                            .join(" · ")}
                        </p>
                      ) : null}
                    </article>
                  ))}
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
                  Open permissions in browser
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
                    {
                      label: "Tool",
                      value: selectedApprovalToolName ?? "n/a",
                    },
                    {
                      label: "Risk",
                      value: selectedApprovalRiskLevel,
                    },
                  ]}
                />
                <InlineNotice title="Request summary" tone="warning">
                  {readString(selectedApproval, "request_summary") ?? "No summary published."}
                </InlineNotice>
                <InlineNotice title="Why this approval appeared" tone="default">
                  {selectedApprovalPolicyExplanation}
                </InlineNotice>
                <InlineNotice title="Next safe action" tone="default">
                  {selectedApprovalNextStep}
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
                    intent: "inspect-access",
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
                        intent: "resume-session",
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
                {sharedRecommendedAction !== null ? (
                  <Button
                    variant="primary"
                    onPress={() => void runSharedOnboardingAction(sharedRecommendedAction)}
                  >
                    {sharedRecommendedAction.label}
                  </Button>
                ) : null}
                <Button variant="secondary" onPress={() => void openScopedHandoff("overview")}>
                  {t("desktop.onboarding.browserHandoff")}
                </Button>
                <Button variant="secondary" onPress={() => void openScopedHandoff("onboarding")}>
                  Advanced setup
                </Button>
                <Button
                  variant="secondary"
                  onPress={() =>
                    void openScopedHandoff("operations", {
                      intent: "inspect-diagnostics",
                      source: "desktop",
                    })
                  }
                >
                  Open diagnostics
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
            {sharedOnboardingItems.length > 0 ? (
              <KeyValueList items={sharedOnboardingItems} />
            ) : null}
            <InlineNotice
              title={snapshot.onboarding.current_step_title}
              tone={snapshot.onboarding.dashboard_handoff_completed ? "success" : "warning"}
            >
              {snapshot.onboarding.current_step_detail}
            </InlineNotice>
            {sharedOnboarding ? (
              <InlineNotice
                title={`${formatDesktopOnboardingStatus(sharedOnboarding.status)} · ${sharedRecommendedStep?.title ?? "Shared onboarding"}`}
                tone={toneForDesktopOnboardingStatus(sharedOnboarding.status)}
              >
                {sharedRecommendedStep?.blocked?.repair_hint ??
                  sharedRecommendedStep?.summary ??
                  sharedOnboarding.first_success_hint ??
                  "Desktop is following the shared onboarding posture from the control plane."}
              </InlineNotice>
            ) : null}
            {snapshot.onboarding.recovery?.message ? (
              <InlineNotice title={t("desktop.onboarding.recoveryHint")} tone="warning">
                {snapshot.onboarding.recovery.message}
              </InlineNotice>
            ) : null}
            {sharedOnboarding?.steps.length ? (
              <div className="desktop-stack">
                {sharedOnboarding.steps.map((step) => (
                  <InlineNotice
                    key={step.step_id}
                    title={`${formatDesktopOnboardingStepStatus(step.status)} · ${step.title}`}
                    tone={toneForDesktopOnboardingStepStatus(step.status)}
                  >
                    {step.blocked?.repair_hint ?? step.summary}
                  </InlineNotice>
                ))}
              </div>
            ) : null}
            {sharedOnboarding?.ready_for_first_success && !firstSuccessCompleted ? (
              <ButtonGroup className="desktop-action-group">
                {DESKTOP_FIRST_SUCCESS_PROMPTS.map((prompt) => (
                  <Button
                    key={prompt}
                    variant="secondary"
                    onPress={() => {
                      setActiveSection("chat");
                      setComposerText(prompt);
                      setNotice("Starter prompt loaded into the desktop chat composer.");
                    }}
                  >
                    {prompt}
                  </Button>
                ))}
                <Button variant="ghost" onPress={() => setActiveSection("approvals")}>
                  Review approvals
                </Button>
              </ButtonGroup>
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

function QuickPanelSurface(props: {
  activeRun: DesktopCompanionSnapshot["active_runs"][number] | null;
  activeSessionId: string;
  composerText: string;
  connectionState: DesktopCompanionSnapshot["connection_state"];
  loading: boolean;
  notice: string | null;
  offlineDrafts: DesktopCompanionSnapshot["offline_drafts"];
  pendingApprovals: JsonValue[];
  previewMode: boolean;
  quickFacts: DesktopCompanionSnapshot["control_center"]["quick_facts"];
  sendBusy: boolean;
  sessionCatalog: DesktopCompanionSnapshot["session_catalog"];
  selectedSession: DesktopCompanionSnapshot["session_catalog"][number] | null;
  setActiveSessionId: (value: string) => void;
  setComposerText: (value: string) => void;
  unreadNotifications: DesktopCompanionSnapshot["notifications"];
  voiceBusy: boolean;
  voiceOverlayEnabled: boolean;
  voiceRecording: boolean;
  onClose: () => void;
  onCreateSession: () => void;
  onOpenActiveRun: () => void;
  onOpenApprovals: () => void;
  onOpenDashboard: () => void;
  onOpenFullCompanion: () => void;
  onOpenSelectedSession: () => void;
  onOpenVoiceOverlay: () => void;
  onRemoveDraft: (draftId: string) => void;
  onSendMessage: () => void;
  onSendOfflineDraft: (draftId: string) => void;
}) {
  return (
    <main className="desktop-root desktop-root--companion desktop-root--quick-panel">
      <section className="desktop-stack">
        <PageHeader
          eyebrow="Ambient companion"
          title="Quick panel"
          description="Recent sessions, pending approvals, active runs and the mini composer stay available without opening the full companion."
          status={
            <>
              <StatusChip tone={toneForConnection(props.connectionState)}>
                {props.connectionState}
              </StatusChip>
              <StatusChip tone={props.activeRun ? "warning" : "default"}>
                {props.activeRun ? "active run" : "idle"}
              </StatusChip>
            </>
          }
        />
        {props.notice ? (
          <InlineNotice title="Desktop status" tone="default">
            {props.notice}
          </InlineNotice>
        ) : null}
        <div className="desktop-inline-row">
          <Button variant="primary" onPress={props.onOpenFullCompanion}>
            Open full companion
          </Button>
          <Button variant="secondary" onPress={props.onOpenDashboard}>
            Browser handoff
          </Button>
          <Button variant="ghost" onPress={props.onClose}>
            Hide panel
          </Button>
        </div>
      </section>

      <section className="desktop-grid desktop-grid--details">
        <SectionCard
          title="Mini composer"
          description="Stay in the current session or start a new one without leaving the ambient surface."
          actions={
            <ButtonGroup className="desktop-action-group">
              <Button variant="secondary" onPress={props.onCreateSession}>
                New session
              </Button>
              <Button
                variant="ghost"
                isDisabled={props.selectedSession === null}
                onPress={props.onOpenSelectedSession}
              >
                Open selected session
              </Button>
              <Button
                variant="ghost"
                isDisabled={!props.voiceOverlayEnabled || props.voiceBusy || props.voiceRecording}
                onPress={props.onOpenVoiceOverlay}
              >
                Voice overlay
              </Button>
            </ButtonGroup>
          }
        >
          <div className="desktop-stack desktop-stack--compact">
            <div className="desktop-stack desktop-stack--compact">
              <span className="desktop-label">Selected session</span>
              <div className="desktop-code-block">
                {props.selectedSession?.title ?? (props.activeSessionId || "No session selected")}
              </div>
            </div>
            <label className="desktop-stack desktop-stack--compact">
              <span className="desktop-label">Quick panel composer</span>
              <textarea
                aria-label="Quick panel composer"
                className="desktop-textarea"
                placeholder="Send a fast operator prompt…"
                rows={5}
                value={props.composerText}
                onChange={(event) => props.setComposerText(event.currentTarget.value)}
              />
            </label>
            <Button
              variant="primary"
              isDisabled={props.sendBusy || props.composerText.trim().length === 0}
              onPress={props.onSendMessage}
            >
              Send prompt
            </Button>
          </div>
        </SectionCard>

        <SectionCard
          title="Recent sessions"
          description="Jump straight into the most recent working context."
        >
          {props.loading && props.sessionCatalog.length === 0 ? (
            <div className="desktop-loading-stack">
              <Spinner size="sm" />
              <p className="desktop-muted">Loading sessions…</p>
            </div>
          ) : props.sessionCatalog.length === 0 ? (
            <EmptyState
              compact
              title="No recent sessions"
              description="Create a new desktop session to start the quick panel workflow."
            />
          ) : (
            <div className="desktop-list">
              {props.sessionCatalog.slice(0, 4).map((session) => (
                <button
                  key={session.session_id}
                  className={`desktop-list-button${
                    session.session_id === props.activeSessionId ? " is-active" : ""
                  }`}
                  type="button"
                  onClick={() => props.setActiveSessionId(session.session_id)}
                >
                  <div className="desktop-stack desktop-stack--compact">
                    <div className="desktop-inline-row">
                      <strong>{session.title}</strong>
                      {buildDesktopSessionListBadges(session)
                        .slice(0, 2)
                        .map((badge) => (
                          <StatusChip
                            key={`${session.session_id}:${badge.label}`}
                            tone={badge.tone}
                          >
                            {badge.label}
                          </StatusChip>
                        ))}
                    </div>
                    <small className="desktop-muted">{buildDesktopSessionMeta(session)}</small>
                    {session.preview ? <p className="desktop-muted">{session.preview}</p> : null}
                  </div>
                </button>
              ))}
            </div>
          )}
        </SectionCard>
      </section>

      <section className="desktop-grid desktop-grid--details">
        <SectionCard
          title="Ambient status"
          description="The quick panel keeps reconnect, approvals and background work visible."
        >
          <KeyValueList
            items={[
              { label: "Dashboard", value: props.quickFacts.dashboard_access_mode },
              { label: "Runtime", value: props.quickFacts.gateway_version ?? "starting" },
              {
                label: "Unread notifications",
                value: String(props.unreadNotifications.filter((entry) => !entry.read).length),
              },
              { label: "Offline drafts", value: String(props.offlineDrafts.length) },
            ]}
          />
          {props.activeRun ? (
            <InlineNotice title={`Active run · ${props.activeRun.session_title}`} tone="warning">
              {props.activeRun.preview ??
                `Run ${props.activeRun.run_id} is ${props.activeRun.status} and has ${props.activeRun.pending_approvals} pending approvals.`}
              <div className="desktop-inline-row">
                <Button variant="secondary" onPress={props.onOpenActiveRun}>
                  Open run
                </Button>
                <Button variant="ghost" onPress={props.onOpenApprovals}>
                  Open approvals
                </Button>
              </div>
            </InlineNotice>
          ) : (
            <InlineNotice title="Active run" tone="default">
              No active run is currently published into the ambient surface.
            </InlineNotice>
          )}
        </SectionCard>

        <SectionCard
          title="Pending approvals"
          description="Approval context stays visible, but detailed resolution still hands off safely."
          actions={
            <Button
              variant="ghost"
              isDisabled={props.pendingApprovals.length === 0}
              onPress={props.onOpenApprovals}
            >
              Open approvals
            </Button>
          }
        >
          {props.pendingApprovals.length === 0 ? (
            <EmptyState
              compact
              title="No approvals waiting"
              description="New approval requests will surface here with a handoff into the full workflow."
            />
          ) : (
            <div className="desktop-stack desktop-stack--compact">
              {props.pendingApprovals.slice(0, 3).map((approval, index) => (
                <InlineNotice
                  key={readString(approval, "approval_id") ?? `approval-${index}`}
                  title={readString(approval, "request_summary") ?? "Approval waiting"}
                  tone="warning"
                >
                  {readString(approval, "subject_type") ??
                    "Tool approval requires explicit review."}
                </InlineNotice>
              ))}
            </div>
          )}
        </SectionCard>
      </section>

      <section className="desktop-grid desktop-grid--details">
        <SectionCard
          title="Offline drafts"
          description="Queued prompts stay visible and actionable across reconnect and restart."
        >
          {props.offlineDrafts.length === 0 ? (
            <EmptyState
              compact
              title="No offline drafts"
              description="Failed sends that were queued for retry will appear here."
            />
          ) : (
            <div className="desktop-stack desktop-stack--compact">
              {props.offlineDrafts.map((draft) => (
                <article key={draft.draft_id} className="desktop-timeline-item">
                  <div className="desktop-inline-row">
                    <strong>{draft.session_id ?? "Unbound draft"}</strong>
                    <small className="desktop-muted">
                      {formatUnixMs(draft.created_at_unix_ms)}
                    </small>
                  </div>
                  <p>{draft.text}</p>
                  <p className="desktop-muted">{draft.reason}</p>
                  <div className="desktop-inline-row">
                    <Button
                      variant="secondary"
                      isDisabled={props.sendBusy}
                      onPress={() => props.onSendOfflineDraft(draft.draft_id)}
                    >
                      Retry send
                    </Button>
                    <Button variant="ghost" onPress={() => props.onRemoveDraft(draft.draft_id)}>
                      Discard
                    </Button>
                  </div>
                </article>
              ))}
            </div>
          )}
        </SectionCard>

        {props.previewMode ? (
          <InlineNotice title="Preview data" tone="warning">
            Desktop preview data is active, so quick panel actions are representative rather than
            live.
          </InlineNotice>
        ) : null}
      </section>
    </main>
  );
}

function VoiceOverlaySurface(props: {
  currentVoiceTranscript: DesktopCompanionAudioTranscriptionResult | null;
  draftText: string;
  lifecycleState: string;
  loading: boolean;
  notice: string | null;
  recordingElapsedMs: number;
  selectedSession: DesktopCompanionSnapshot["session_catalog"][number] | null;
  sendBusy: boolean;
  selectedVoiceInputId: string;
  selectedVoiceInputLabel: string;
  selectedVoiceOutputLabel: string;
  selectedVoiceOutputUri: string;
  speaking: boolean;
  ttsMuted: boolean;
  ttsPlaybackSupported: boolean;
  voiceAuditLog: DesktopCompanionSnapshot["voice"]["audit_log"];
  voiceBusy: boolean;
  voiceCaptureEnabled: boolean;
  voiceCaptureSupported: boolean;
  voiceInputDevices: DesktopVoiceInputOption[];
  voiceRecording: boolean;
  voiceOutputVoices: DesktopVoiceOutputOption[];
  voicePermissionState: string;
  voiceSilenceDetectionEnabled: boolean;
  voiceSilenceDetectionRolloutEnabled: boolean;
  voiceSilenceTimeoutMs: number;
  onClearVoiceDraft: () => void;
  onClose: () => void;
  onCreateSession: () => void;
  onOpenFullCompanion: () => void;
  onSendMessage: () => void;
  onSelectVoiceInputDevice: (deviceId: string) => void;
  onSelectVoiceOutputVoice: (voiceURI: string) => void;
  onSpeakLatestAssistant: () => void;
  onStartVoiceCapture: () => void;
  onStopSpeechPlayback: () => void;
  onStopVoiceCapture: () => void;
  onToggleTtsMute: () => void;
  onToggleSilenceDetection: () => void;
  onUpdateDraftText: (value: string) => void;
  onUseTranscript: (text: string) => void;
}) {
  return (
    <main className="desktop-root desktop-root--companion desktop-root--voice-overlay">
      <section className="desktop-stack">
        <PageHeader
          eyebrow="Ambient voice"
          title="Voice overlay"
          description="Push-to-talk stays explicit: record, review, then decide whether to send."
          status={
            <>
              <StatusChip tone={props.voiceRecording ? "warning" : "default"}>
                {props.lifecycleState}
              </StatusChip>
              <StatusChip tone={props.speaking ? "success" : "default"}>
                {props.speaking ? "speaking" : "silent"}
              </StatusChip>
            </>
          }
        />
        {props.notice ? (
          <InlineNotice title="Voice status" tone="default">
            {props.notice}
          </InlineNotice>
        ) : null}
      </section>

      <section className="desktop-grid desktop-grid--details">
        <SectionCard
          title="Push-to-talk"
          description="Ambient listening stays disabled. Recording begins only from explicit operator action."
          actions={
            <ButtonGroup className="desktop-action-group">
              <Button
                variant="primary"
                isDisabled={
                  props.voiceBusy ||
                  !props.voiceCaptureEnabled ||
                  !props.voiceCaptureSupported ||
                  props.selectedSession === null
                }
                onPress={
                  props.voiceRecording ? props.onStopVoiceCapture : props.onStartVoiceCapture
                }
              >
                {props.voiceRecording ? "Stop recording" : "Start recording"}
              </Button>
              <Button
                variant="secondary"
                isDisabled={props.selectedSession !== null}
                onPress={props.onCreateSession}
              >
                Create quick session
              </Button>
              <Button variant="ghost" onPress={props.onClose}>
                Hide overlay
              </Button>
              <Button variant="ghost" onPress={props.onOpenFullCompanion}>
                Full companion
              </Button>
            </ButtonGroup>
          }
        >
          <KeyValueList
            items={[
              { label: "Session", value: props.selectedSession?.title ?? "No session selected" },
              {
                label: "Recording",
                value: props.voiceRecording
                  ? formatDurationMs(props.recordingElapsedMs)
                  : "stopped",
              },
              { label: "Lifecycle", value: props.lifecycleState },
              { label: "Mic permission", value: props.voicePermissionState },
              { label: "Input device", value: props.selectedVoiceInputLabel },
              {
                label: "Speech playback",
                value: props.ttsPlaybackSupported ? "available" : "unavailable",
              },
              {
                label: "Silence detection",
                value: props.voiceSilenceDetectionRolloutEnabled
                  ? props.voiceSilenceDetectionEnabled
                    ? `${formatDurationMs(props.voiceSilenceTimeoutMs)} timeout`
                    : "disabled"
                  : "rollout disabled",
              },
            ]}
          />
          {props.selectedSession === null ? (
            <InlineNotice title="Session required" tone="warning">
              Voice overlay can safely create a quick chat session for you before recording starts.
            </InlineNotice>
          ) : null}
          <button
            className="desktop-list-button"
            type="button"
            disabled={
              props.voiceBusy ||
              !props.voiceCaptureEnabled ||
              !props.voiceCaptureSupported ||
              props.selectedSession === null
            }
            onMouseDown={() => {
              if (!props.voiceRecording) {
                props.onStartVoiceCapture();
              }
            }}
            onMouseUp={() => {
              if (props.voiceRecording) {
                props.onStopVoiceCapture();
              }
            }}
            onMouseLeave={() => {
              if (props.voiceRecording) {
                props.onStopVoiceCapture();
              }
            }}
            onKeyDown={(event) => {
              if (!props.voiceRecording && (event.key === " " || event.key === "Enter")) {
                event.preventDefault();
                props.onStartVoiceCapture();
              }
            }}
            onKeyUp={(event) => {
              if (props.voiceRecording && (event.key === " " || event.key === "Enter")) {
                event.preventDefault();
                props.onStopVoiceCapture();
              }
            }}
          >
            <strong>{props.voiceRecording ? "Release to stop recording" : "Hold to talk"}</strong>
            <small className="desktop-muted">
              Push-to-talk starts only from explicit press-and-hold or the start button above.
            </small>
          </button>
        </SectionCard>

        <SectionCard
          title="Transcript review"
          description="Nothing is sent automatically. The transcript must be explicitly reviewed first."
        >
          {props.currentVoiceTranscript === null &&
          props.draftText.trim().length === 0 &&
          props.lifecycleState !== "error" ? (
            <EmptyState
              compact
              title="No transcript yet"
              description="Record a short prompt to populate the review pane."
            />
          ) : (
            <article className="desktop-timeline-item">
              <div className="desktop-inline-row">
                <strong>
                  {props.currentVoiceTranscript === null ? "Manual fallback" : "Ready for review"}
                </strong>
                <StatusChip tone={props.currentVoiceTranscript === null ? "warning" : "success"}>
                  {props.currentVoiceTranscript?.transcript_language ?? props.lifecycleState}
                </StatusChip>
              </div>
              {props.currentVoiceTranscript ? (
                <p>{props.currentVoiceTranscript.transcript_text}</p>
              ) : null}
              {props.currentVoiceTranscript?.transcript_summary ? (
                <p className="desktop-muted">{props.currentVoiceTranscript.transcript_summary}</p>
              ) : null}
              <p className="desktop-muted">
                {props.currentVoiceTranscript?.privacy_note ??
                  "Transcription failed or was cancelled. You can still type a manual fallback draft before sending."}
              </p>
              <label className="desktop-stack desktop-stack--compact">
                <span className="desktop-label">Editable draft</span>
                <textarea
                  aria-label="Editable voice draft"
                  className="desktop-textarea"
                  rows={6}
                  value={props.draftText}
                  onChange={(event) => props.onUpdateDraftText(event.currentTarget.value)}
                />
              </label>
              <div className="desktop-inline-row">
                {props.currentVoiceTranscript ? (
                  <Button
                    variant="secondary"
                    isDisabled={props.sendBusy}
                    onPress={() =>
                      props.onUseTranscript(props.currentVoiceTranscript!.transcript_text)
                    }
                  >
                    Use transcript
                  </Button>
                ) : null}
                <Button
                  variant="primary"
                  isDisabled={props.sendBusy || props.draftText.trim().length === 0}
                  onPress={props.onSendMessage}
                >
                  Send reviewed draft
                </Button>
                <Button variant="ghost" onPress={props.onClearVoiceDraft}>
                  Discard
                </Button>
              </div>
            </article>
          )}
        </SectionCard>

        <SectionCard
          title="Speech playback"
          description="TTS stays opt-in and reads only explicit assistant output selections. Output routing follows the OS default audio device."
        >
          <div className="desktop-grid desktop-grid--details">
            <label className="desktop-stack desktop-stack--compact">
              <span className="desktop-label">Microphone device</span>
              <select
                aria-label="Desktop microphone device"
                className="desktop-select"
                value={props.selectedVoiceInputId}
                onChange={(event) => props.onSelectVoiceInputDevice(event.currentTarget.value)}
              >
                <option value="">System default microphone</option>
                {props.voiceInputDevices.map((device) => (
                  <option key={device.deviceId} value={device.deviceId}>
                    {device.label}
                  </option>
                ))}
              </select>
            </label>
            <label className="desktop-stack desktop-stack--compact">
              <span className="desktop-label">Speech voice</span>
              <select
                aria-label="Desktop speech voice"
                className="desktop-select"
                value={props.selectedVoiceOutputUri}
                onChange={(event) => props.onSelectVoiceOutputVoice(event.currentTarget.value)}
              >
                <option value="">System default voice</option>
                {props.voiceOutputVoices.map((voice) => (
                  <option key={voice.voiceURI} value={voice.voiceURI}>
                    {voice.label} ({voice.lang})
                  </option>
                ))}
              </select>
            </label>
          </div>
          <KeyValueList
            items={[
              { label: "Selected voice", value: props.selectedVoiceOutputLabel },
              { label: "Mute", value: props.ttsMuted ? "muted" : "live" },
            ]}
          />
          <div className="desktop-inline-row">
            <Button variant="secondary" onPress={props.onSpeakLatestAssistant}>
              Speak latest assistant
            </Button>
            <Button
              variant="ghost"
              isDisabled={!props.speaking}
              onPress={props.onStopSpeechPlayback}
            >
              Stop speaking
            </Button>
            <Button variant="ghost" onPress={props.onToggleTtsMute}>
              {props.ttsMuted ? "Unmute TTS" : "Mute TTS"}
            </Button>
            <Button
              variant="ghost"
              isDisabled={!props.voiceSilenceDetectionRolloutEnabled}
              onPress={props.onToggleSilenceDetection}
            >
              {props.voiceSilenceDetectionEnabled
                ? "Disable silence detection"
                : "Enable silence detection"}
            </Button>
          </div>
          {props.voiceAuditLog.length > 0 ? (
            <div className="desktop-stack desktop-stack--compact">
              {props.voiceAuditLog
                .slice()
                .reverse()
                .slice(0, 3)
                .map((entry) => (
                  <article key={entry.audit_id} className="desktop-timeline-item">
                    <div className="desktop-inline-row">
                      <strong>{entry.kind.replaceAll("_", " ")}</strong>
                      <small className="desktop-muted">
                        {formatUnixMs(entry.created_at_unix_ms)}
                      </small>
                    </div>
                    <p>{entry.detail}</p>
                  </article>
                ))}
            </div>
          ) : null}
        </SectionCard>
      </section>
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

function buildDesktopApprovalNextStep(args: {
  toolName: string | null;
  riskLevel: string;
}): string {
  const toolLabel = args.toolName ?? "this tool";
  if (args.riskLevel === "high") {
    return `Review the request carefully before approving ${toolLabel}. If it repeats often, open the permissions center in the browser and decide whether a safer long-lived posture is justified.`;
  }
  return `Approve once if this request is expected, or open the permissions center in the browser to decide whether ${toolLabel} should stay ask-each-time or move to a broader posture.`;
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

function resolveDesktopSurfaceMode(): DesktopCompanionSurfaceMode {
  if (typeof window === "undefined") {
    return "main";
  }
  const surface = new URLSearchParams(window.location.search).get("surface");
  if (surface === "quick-panel") {
    return "quick_panel";
  }
  if (surface === "voice-overlay") {
    return "voice_overlay";
  }
  return "main";
}

function deriveCurrentVoiceTranscript(
  snapshot: DesktopCompanionSnapshot,
  fallback: DesktopCompanionAudioTranscriptionResult | null,
): DesktopCompanionAudioTranscriptionResult | null {
  if (snapshot.voice.draft_text) {
    return {
      attachment_id: "desktop-voice-draft",
      artifact_id: "desktop-voice-draft",
      transcript_text: snapshot.voice.draft_text,
      transcript_summary: snapshot.voice.draft_summary,
      transcript_language: snapshot.voice.draft_language,
      transcript_duration_ms: snapshot.voice.draft_duration_ms,
      transcript_processing_ms: undefined,
      derived_artifact_id: undefined,
      privacy_note:
        "Push-to-talk audio is uploaded only after you stop recording, follows the existing media retention/redaction pipeline, and ambient listening remains disabled.",
      warnings: [],
    };
  }
  return fallback;
}

async function requestVoiceCaptureStream(
  preferredDeviceId?: string,
): Promise<{ stream: MediaStream; usedFallbackInput: boolean }> {
  if (preferredDeviceId && preferredDeviceId.trim().length > 0) {
    try {
      const stream = await navigator.mediaDevices.getUserMedia({
        audio: buildVoiceCaptureConstraints(preferredDeviceId),
      });
      return { stream, usedFallbackInput: false };
    } catch (failure) {
      if (!shouldRetryVoiceCaptureWithDefault(failure)) {
        throw failure;
      }
    }
  }
  const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
  return {
    stream,
    usedFallbackInput: Boolean(preferredDeviceId && preferredDeviceId.trim().length > 0),
  };
}

function buildVoiceCaptureConstraints(deviceId?: string): MediaTrackConstraints | boolean {
  if (!deviceId || deviceId.trim().length === 0) {
    return true;
  }
  return {
    deviceId: {
      exact: deviceId,
    },
    noiseSuppression: true,
    echoCancellation: true,
  };
}

function shouldRetryVoiceCaptureWithDefault(failure: unknown): boolean {
  return (
    failure instanceof DOMException &&
    (failure.name === "NotFoundError" || failure.name === "OverconstrainedError")
  );
}

function readVoiceTrackDeviceId(track: MediaStreamTrack | null): string | undefined {
  const deviceId = track?.getSettings().deviceId;
  return typeof deviceId === "string" && deviceId.trim().length > 0 ? deviceId : undefined;
}

function deriveVoicePermissionState(value: unknown): string {
  if (value instanceof DOMException) {
    if (value.name === "NotAllowedError" || value.name === "SecurityError") {
      return "denied";
    }
    if (value.name === "NotFoundError" || value.name === "OverconstrainedError") {
      return "unavailable";
    }
  }
  const text = value instanceof Error ? value.message : String(value);
  const normalized = text.toLowerCase();
  if (normalized.includes("denied") || normalized.includes("notallowed")) {
    return "denied";
  }
  if (normalized.includes("notfound") || normalized.includes("overconstrained")) {
    return "unavailable";
  }
  return "unknown";
}

function calculateVoiceSilenceRms(samples: Uint8Array): number {
  if (samples.length === 0) {
    return 0;
  }
  let total = 0;
  for (const sample of samples) {
    const centered = sample / 128 - 1;
    total += centered * centered;
  }
  return Math.sqrt(total / samples.length);
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

function buildSharedOnboardingItems(
  posture: OnboardingPostureEnvelope | null,
): Array<{ label: string; value: string }> {
  if (posture === null) {
    return [];
  }
  return [
    { label: "Shared flow", value: posture.flow_variant },
    { label: "Shared status", value: posture.status },
    {
      label: "Shared progress",
      value: `${posture.counts.done}/${posture.steps.length}`,
    },
    {
      label: "Blocked steps",
      value: String(posture.counts.blocked),
    },
  ];
}

function formatDesktopOnboardingStatus(status: OnboardingPostureEnvelope["status"]): string {
  switch (status) {
    case "not_started":
      return "Not started";
    case "in_progress":
      return "In progress";
    case "blocked":
      return "Blocked";
    case "ready":
      return "Ready";
    case "complete":
      return "Complete";
  }
}

function toneForDesktopOnboardingStatus(
  status: OnboardingPostureEnvelope["status"],
): "default" | "success" | "warning" | "danger" {
  switch (status) {
    case "blocked":
      return "danger";
    case "ready":
    case "complete":
      return "success";
    case "in_progress":
      return "warning";
    default:
      return "default";
  }
}

function formatDesktopOnboardingStepStatus(status: string): string {
  switch (status) {
    case "done":
      return "Done";
    case "blocked":
      return "Blocked";
    case "in_progress":
      return "In progress";
    case "skipped":
      return "Skipped";
    default:
      return "Todo";
  }
}

function toneForDesktopOnboardingStepStatus(
  status: string,
): "default" | "success" | "warning" | "danger" {
  switch (status) {
    case "done":
      return "success";
    case "blocked":
      return "danger";
    case "in_progress":
      return "warning";
    default:
      return "default";
  }
}

function mapConsolePathToDesktopHandoffSection(target: string): string {
  const normalized = target.trim();
  if (normalized.includes("/#/chat/canvas")) {
    return "canvas";
  }
  if (normalized.includes("/#/chat")) {
    return "chat";
  }
  if (normalized.includes("/#/control/approvals")) {
    return "approvals";
  }
  if (normalized.includes("/#/settings/access")) {
    return "access";
  }
  if (normalized.includes("/#/settings/profiles")) {
    return "onboarding";
  }
  if (normalized.includes("/#/control/browser")) {
    return "browser";
  }
  if (normalized.includes("/#/control/channels")) {
    return "channels";
  }
  if (normalized.includes("/#/settings/diagnostics")) {
    return "operations";
  }
  return "overview";
}

function isDesktopCompanionSection(value: string): value is DesktopCompanionSection {
  return SECTION_ORDER.includes(value as DesktopCompanionSection);
}
