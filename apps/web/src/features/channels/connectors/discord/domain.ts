import type { Dispatch, FormEvent, SetStateAction } from "react";

import { type ConsoleApiClient, type JsonValue } from "../../../../consoleApi";
import type { DiscordChannelState } from "./controller";
import {
  applyDiscordOnboarding as applyDiscordOnboardingRequest,
  probeDiscordOnboarding,
  refreshDiscordChannelHealth,
  sendDiscordTest,
} from "./api";
import {
  emptyToUndefined,
  isJsonObject,
  parseInteger,
  readString,
  toErrorMessage,
} from "../../../../console/shared";

type Setter<T> = Dispatch<SetStateAction<T>>;

export type DiscordChannelDomainDeps = {
  api: ConsoleApiClient;
  channelsSelectedConnectorId: string;
  discordChannelState: DiscordChannelState;
  setChannelsBusy: Setter<boolean>;
  setError: Setter<string | null>;
  setNotice: Setter<string | null>;
  setSelectedChannelStatusPayload: (payload: JsonValue) => void;
  refreshChannels: (preferredConnectorId?: string) => Promise<void>;
  loadChannel: (connectorId: string) => Promise<void>;
  refreshChannelLogs: (connectorId: string) => Promise<void>;
};

function discordWizardConnectorId(accountId: string): string | null {
  const normalized = accountId.trim().toLowerCase();
  if (normalized.length === 0) {
    return "discord:default";
  }
  if (!/^[a-z0-9._-]+$/.test(normalized)) {
    return null;
  }
  return `discord:${normalized}`;
}

function parseDiscordWizardSenderList(raw: string): string[] {
  const entries: string[] = [];
  for (const candidate of raw.split(",")) {
    const normalized = candidate.trim().toLowerCase();
    if (normalized.length === 0) {
      continue;
    }
    if (!entries.includes(normalized)) {
      entries.push(normalized);
    }
  }
  return entries;
}

export function createDiscordChannelDomain(deps: DiscordChannelDomainDeps) {
  const {
    api,
    channelsSelectedConnectorId,
    discordChannelState,
    setChannelsBusy,
    setError,
    setNotice,
    setSelectedChannelStatusPayload,
    refreshChannels,
    loadChannel,
    refreshChannelLogs,
  } = deps;
  const {
    channelsDiscordTarget,
    channelsDiscordText,
    channelsDiscordAutoReaction,
    channelsDiscordThreadId,
    channelsDiscordConfirm,
    setChannelsDiscordConfirm,
    setDiscordWizardBusy,
    discordWizardAccountId,
    discordWizardMode,
    discordWizardToken,
    setDiscordWizardToken,
    discordWizardScope,
    discordWizardAllowFrom,
    discordWizardDenyFrom,
    discordWizardRequireMention,
    discordWizardBroadcast,
    discordWizardConcurrency,
    discordWizardConfirmOpen,
    discordWizardVerifyChannelId,
    setDiscordWizardPreflight,
    setDiscordWizardApply,
    discordWizardVerifyTarget,
    discordWizardVerifyText,
    discordWizardVerifyConfirm,
    setDiscordWizardVerifyConfirm,
  } = discordChannelState;

  function parsedDiscordWizardConcurrency(): number {
    const parsed = parseInteger(discordWizardConcurrency);
    if (parsed === null || parsed <= 0) {
      return 2;
    }
    return Math.min(Math.max(parsed, 1), 32);
  }

  function parseDiscordWizardVerifyChannelId(): { value?: string; error?: string } {
    const normalized = discordWizardVerifyChannelId.trim();
    if (normalized.length === 0) {
      return {};
    }
    if (!/^[0-9]+$/.test(normalized)) {
      return { error: "Verify channel ID must contain decimal digits only." };
    }
    if (normalized.length < 16 || normalized.length > 24) {
      return {
        error: "Verify channel ID must be a canonical Discord snowflake (16-24 digits).",
      };
    }
    return { value: normalized };
  }

  function buildDiscordWizardPayload(verifyChannelId?: string) {
    const normalized = discordWizardAccountId.trim().toLowerCase();
    return {
      account_id: normalized.length > 0 ? normalized : undefined,
      token: discordWizardToken.trim(),
      mode: discordWizardMode,
      inbound_scope: discordWizardScope,
      allow_from: parseDiscordWizardSenderList(discordWizardAllowFrom),
      deny_from: parseDiscordWizardSenderList(discordWizardDenyFrom),
      require_mention: discordWizardRequireMention,
      concurrency_limit: parsedDiscordWizardConcurrency(),
      broadcast_strategy: discordWizardBroadcast,
      confirm_open_guild_channels: discordWizardConfirmOpen,
      verify_channel_id: verifyChannelId,
    } as const;
  }

  async function submitChannelDiscordTestSend(event: FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    if (channelsSelectedConnectorId.trim().length === 0) {
      setError("Select a connector before dispatching Discord test send.");
      return;
    }
    if (!channelsSelectedConnectorId.trim().startsWith("discord:")) {
      setError("Discord test send is available only for Discord connectors.");
      return;
    }
    if (channelsDiscordTarget.trim().length === 0) {
      setError("Discord test target cannot be empty.");
      return;
    }
    if (!channelsDiscordConfirm) {
      setError("Discord test send requires explicit confirmation.");
      return;
    }

    setChannelsBusy(true);
    setError(null);
    try {
      const connectorId = channelsSelectedConnectorId.trim();
      await sendDiscordTest(api, connectorId, {
        target: channelsDiscordTarget.trim(),
        text: emptyToUndefined(channelsDiscordText),
        confirm: true,
        auto_reaction: emptyToUndefined(channelsDiscordAutoReaction),
        thread_id: emptyToUndefined(channelsDiscordThreadId),
      });
      setNotice("Discord test send dispatched.");
      setChannelsDiscordConfirm(false);
      await refreshChannelLogs(connectorId);
      await refreshChannels(connectorId);
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setChannelsBusy(false);
    }
  }

  async function refreshChannelHealth(): Promise<void> {
    if (channelsSelectedConnectorId.trim().length === 0) {
      setError("Select a connector before running health refresh.");
      return;
    }
    setChannelsBusy(true);
    setError(null);
    try {
      const response = await refreshDiscordChannelHealth(api, channelsSelectedConnectorId.trim(), {
        verify_channel_id: emptyToUndefined(discordWizardVerifyChannelId),
      });
      setSelectedChannelStatusPayload(response as JsonValue);
      setNotice("Channel health refresh completed.");
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setChannelsBusy(false);
    }
  }

  async function runDiscordOnboardingProbe(): Promise<void> {
    if (discordWizardToken.trim().length === 0) {
      setError("Discord onboarding token cannot be empty.");
      return;
    }
    const verifyChannel = parseDiscordWizardVerifyChannelId();
    if (verifyChannel.error !== undefined) {
      setError(verifyChannel.error);
      return;
    }
    const connectorId = discordWizardConnectorId(discordWizardAccountId);
    if (connectorId === null) {
      setError("Discord account ID contains unsupported characters.");
      return;
    }
    setDiscordWizardBusy(true);
    setError(null);
    try {
      const response = await probeDiscordOnboarding(
        api,
        buildDiscordWizardPayload(verifyChannel.value),
      );
      setDiscordWizardPreflight(isJsonObject(response) ? response : null);
      const botId = isJsonObject(response.bot) ? readString(response.bot, "id") : null;
      const botUsername = isJsonObject(response.bot) ? readString(response.bot, "username") : null;
      setNotice(
        botId !== null && botUsername !== null
          ? `Discord preflight OK for ${botUsername} (${botId}).`
          : "Discord preflight completed.",
      );
      await refreshChannels(connectorId);
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setDiscordWizardBusy(false);
    }
  }

  async function applyDiscordOnboarding(): Promise<void> {
    if (discordWizardToken.trim().length === 0) {
      setError("Discord onboarding token cannot be empty.");
      return;
    }
    const verifyChannel = parseDiscordWizardVerifyChannelId();
    if (verifyChannel.error !== undefined) {
      setError(verifyChannel.error);
      return;
    }
    const connectorId = discordWizardConnectorId(discordWizardAccountId);
    if (connectorId === null) {
      setError("Discord account ID contains unsupported characters.");
      return;
    }
    if (discordWizardScope === "open_guild_channels" && !discordWizardConfirmOpen) {
      setError("Open guild channels require explicit confirmation.");
      return;
    }

    setDiscordWizardBusy(true);
    setError(null);
    try {
      const response = await applyDiscordOnboardingRequest(
        api,
        buildDiscordWizardPayload(verifyChannel.value),
      );
      setDiscordWizardApply(isJsonObject(response) ? response : null);
      const preflight = isJsonObject(response.preflight) ? response.preflight : null;
      const bot = preflight !== null && isJsonObject(preflight.bot) ? preflight.bot : null;
      const botId = bot !== null ? readString(bot, "id") : null;
      const botUsername = bot !== null ? readString(bot, "username") : null;
      setNotice(
        botId !== null && botUsername !== null
          ? `Discord onboarding applied for ${botUsername} (${botId}).`
          : "Discord onboarding applied.",
      );
      setDiscordWizardToken("");
      await refreshChannels(connectorId);
      await loadChannel(connectorId);
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setDiscordWizardBusy(false);
    }
  }

  async function verifyDiscordOnboardingTarget(): Promise<void> {
    const connectorId = discordWizardConnectorId(discordWizardAccountId);
    if (connectorId === null) {
      setError("Discord account ID contains unsupported characters.");
      return;
    }
    if (discordWizardVerifyTarget.trim().length === 0) {
      setError("Verification target cannot be empty.");
      return;
    }
    if (!discordWizardVerifyConfirm) {
      setError("Verification send requires explicit confirmation.");
      return;
    }
    setDiscordWizardBusy(true);
    setError(null);
    try {
      const response = await sendDiscordTest(api, connectorId, {
        target: discordWizardVerifyTarget.trim(),
        text: emptyToUndefined(discordWizardVerifyText),
        confirm: true,
      });
      const dispatch = isJsonObject(response.dispatch) ? response.dispatch : null;
      const delivered = dispatch !== null ? readString(dispatch, "delivered") : null;
      setNotice(
        delivered !== null
          ? `Discord verification dispatched (delivered=${delivered}).`
          : "Discord verification dispatched.",
      );
      setDiscordWizardVerifyConfirm(false);
      await refreshChannels(connectorId);
      await refreshChannelLogs(connectorId);
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setDiscordWizardBusy(false);
    }
  }

  return {
    submitChannelDiscordTestSend,
    refreshChannelHealth,
    runDiscordOnboardingProbe,
    applyDiscordOnboarding,
    verifyDiscordOnboardingTarget,
  };
}
