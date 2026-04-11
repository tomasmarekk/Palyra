import type { FormEvent } from "react";

import type { createDiscordChannelDomain } from "./domain";
import type { useDiscordChannelState } from "./useDiscordChannelState";

export type DiscordChannelState = ReturnType<typeof useDiscordChannelState>;
export type DiscordChannelActions = ReturnType<typeof createDiscordChannelDomain>;

export type DiscordChannelController = DiscordChannelState & {
  isBusy: boolean;
  sendTest: (event: FormEvent<HTMLFormElement>) => Promise<void>;
  refreshHealth: () => Promise<void>;
  runPreflight: () => Promise<void>;
  applyOnboarding: () => Promise<void>;
  runVerification: () => Promise<void>;
};

export function createDiscordChannelController(
  state: DiscordChannelState,
  actions: DiscordChannelActions,
  isBusy: boolean,
): DiscordChannelController {
  return {
    ...state,
    isBusy,
    sendTest: actions.submitChannelDiscordTestSend,
    refreshHealth: actions.refreshChannelHealth,
    runPreflight: actions.runDiscordOnboardingProbe,
    applyOnboarding: actions.applyDiscordOnboarding,
    runVerification: actions.verifyDiscordOnboardingTarget,
  };
}
