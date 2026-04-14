import { describe, expect, it } from "vite-plus/test";

import {
  aggregateUxTelemetry,
  buildConsoleHandoffHref,
  nearestSupportedHandoffSection,
  parseConsoleHandoff,
  toSystemEventPayload,
} from "./contracts";

describe("phase 1 contracts", () => {
  it("builds and parses handoff URLs with the canonical identifier set", () => {
    const href = buildConsoleHandoffHref({
      section: "chat",
      sessionId: "session-1",
      runId: "run-2",
      deviceId: "device-3",
      objectiveId: "objective-4",
      canvasId: "canvas-5",
      intent: "approve",
      source: "desktop",
    });

    expect(href).toBe(
      "/chat?sessionId=session-1&runId=run-2&deviceId=device-3&objectiveId=objective-4&canvasId=canvas-5&intent=approve&source=desktop",
    );
    expect(parseConsoleHandoff(href)).toEqual({
      section: "chat",
      sessionId: "session-1",
      runId: "run-2",
      deviceId: "device-3",
      objectiveId: "objective-4",
      canvasId: "canvas-5",
      intent: "approve",
      source: "desktop",
    });
  });

  it("falls back to the nearest safe section when a target is unsupported", () => {
    expect(nearestSupportedHandoffSection({ section: "home" })).toBe("overview");
    expect(nearestSupportedHandoffSection({ section: "overview", canvasId: "canvas-1" })).toBe(
      "overview",
    );
  });

  it("serializes UX telemetry using the bounded system-event payload shape", () => {
    expect(
      toSystemEventPayload({
        name: "ux.handoff.opened",
        surface: "web",
        section: "chat",
        intent: "resume-session",
        sessionId: "session-1",
        summary: "Opened a scoped session handoff.",
      }),
    ).toEqual({
      name: "ux.handoff.opened",
      summary: "Opened a scoped session handoff.",
      details: {
        surface: "web",
        section: "chat",
        sessionId: "session-1",
        intent: "resume-session",
      },
    });
  });

  it("aggregates funnel, approval fatigue, and friction from UX system events", () => {
    const aggregate = aggregateUxTelemetry([
      {
        operator_event: "system.operator.ux.onboarding.step",
        payload_json: { details: { surface: "desktop", step: "setup_started" } },
      },
      {
        operator_event: "system.operator.ux.onboarding.step",
        payload_json: { details: { surface: "desktop", step: "provider_verified" } },
      },
      {
        operator_event: "system.operator.ux.chat.prompt_submitted",
        payload_json: { details: { surface: "web", sessionId: "session-a" } },
      },
      {
        operator_event: "system.operator.ux.approval.resolved",
        payload_json: {
          details: {
            surface: "web",
            toolName: "palyra.fs.apply_patch",
            sessionId: "session-a",
            outcome: "blocked",
          },
        },
      },
      {
        operator_event: "system.operator.ux.run.inspected",
        payload_json: { details: { surface: "web", runId: "run-a" } },
      },
      {
        operator_event: "system.operator.ux.session.resumed",
        payload_json: { details: { surface: "tui", sessionId: "session-b" } },
      },
    ]);

    expect(aggregate.totalEvents).toBe(6);
    expect(aggregate.countsBySurface.desktop).toBe(2);
    expect(aggregate.countsBySurface.web).toBe(3);
    expect(aggregate.countsBySurface.tui).toBe(1);
    expect(aggregate.approvalFatigueByTool["palyra.fs.apply_patch"]).toBe(1);
    expect(aggregate.approvalFatigueBySession["session-a"]).toBe(1);
    expect(aggregate.frictionBySurface.web).toBe(1);
    expect(aggregate.funnel.setup_started).toBe(1);
    expect(aggregate.funnel.provider_verified).toBe(1);
    expect(aggregate.funnel.first_prompt_sent).toBe(1);
    expect(aggregate.funnel.first_approval_resolved).toBe(1);
    expect(aggregate.funnel.first_run_inspected).toBe(1);
    expect(aggregate.funnel.second_session_resumed).toBe(1);
  });
});
