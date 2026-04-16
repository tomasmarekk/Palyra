export const SILENCE_MONITOR_INTERVAL_MS = 200;
export const SILENCE_MONITOR_THRESHOLD_RMS = 0.015;

export type DesktopVoiceInputOption = {
  deviceId: string;
  label: string;
};

export type DesktopVoiceOutputOption = {
  voiceURI: string;
  label: string;
  lang: string;
  default: boolean;
};

export type VoiceCaptureStopReason = "manual" | "silence";

export async function requestVoiceCaptureStream(
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

export function readVoiceTrackDeviceId(track: MediaStreamTrack | null): string | undefined {
  const deviceId = track?.getSettings().deviceId;
  return typeof deviceId === "string" && deviceId.trim().length > 0 ? deviceId : undefined;
}

export function deriveVoicePermissionState(value: unknown): string {
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

export function calculateVoiceSilenceRms(samples: Uint8Array): number {
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

export function resolvePreferredVoiceMimeType(): string | null {
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

export function extensionForAudioMimeType(contentType: string): string {
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

export function formatDurationMs(value: number): string {
  const totalSeconds = Math.max(0, Math.round(value / 1000));
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return `${minutes}:${String(seconds).padStart(2, "0")}`;
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
