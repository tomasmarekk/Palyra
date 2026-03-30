import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import type { ReactElement } from "react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import {
  type ChatRunStatusRecord,
  type ChatRunTapeSnapshot,
  type ChatSessionRecord,
  type ChatStreamLine,
  type ConsoleApiClient,
} from "../consoleApi";
import { ChatConsolePanel } from "./ChatConsolePanel";

afterEach(() => {
  cleanup();
});

describe("ChatConsolePanel", () => {
  it("keeps the latest run details when older requests resolve out of order", async () => {
    const session = sampleSession();
    const runOneId = "01ARZ3NDEKTSV4RRFFQ69G5FA1";
    const runTwoId = "01ARZ3NDEKTSV4RRFFQ69G5FA2";

    const runOneStatus = createDeferred<{ run: ChatRunStatusRecord }>();
    const runOneEvents = createDeferred<{ run: ChatRunStatusRecord; tape: ChatRunTapeSnapshot }>();
    const runTwoFreshStatus = createDeferred<{ run: ChatRunStatusRecord }>();
    const runTwoFreshEvents = createDeferred<{
      run: ChatRunStatusRecord;
      tape: ChatRunTapeSnapshot;
    }>();

    let runTwoStatusCalls = 0;
    let runTwoEventCalls = 0;
    const listChatSessions = vi.fn().mockResolvedValue({ sessions: [session] });
    const streamChatMessage = vi.fn(
      (
        _sessionId: string,
        payload: { text: string },
        options: { onLine: (line: ChatStreamLine) => void },
      ) => {
        if (payload.text === "first run") {
          emitCompletedRun(options.onLine, session.session_id, runOneId, "first token");
          return Promise.resolve();
        }
        emitCompletedRun(options.onLine, session.session_id, runTwoId, "second token");
        return Promise.resolve();
      },
    );
    const chatRunStatus = vi.fn((runId: string) => {
      if (runId === runOneId) {
        return runOneStatus.promise;
      }
      if (runId === runTwoId) {
        runTwoStatusCalls += 1;
        if (runTwoStatusCalls === 1) {
          return Promise.resolve({
            run: createRunStatus(runTwoId, "initial-two", 11, 200),
          });
        }
        return runTwoFreshStatus.promise;
      }
      throw new Error(`Unhandled run status request for ${runId}`);
    });
    const chatRunEvents = vi.fn((runId: string) => {
      if (runId === runOneId) {
        return runOneEvents.promise;
      }
      if (runId === runTwoId) {
        runTwoEventCalls += 1;
        if (runTwoEventCalls === 1) {
          return Promise.resolve({
            run: createRunStatus(runTwoId, "initial-two", 11, 200),
            tape: createRunTape(runTwoId, "run-two-initial"),
          });
        }
        return runTwoFreshEvents.promise;
      }
      throw new Error(`Unhandled run events request for ${runId}`);
    });

    const api = {
      listChatSessions,
      streamChatMessage,
      chatRunStatus,
      chatRunEvents,
    } as unknown as ConsoleApiClient;

    renderWithRouter(
      <ChatConsolePanel
        api={api}
        revealSensitiveValues={false}
        setError={vi.fn()}
        setNotice={vi.fn()}
      />,
    );

    await waitFor(() => {
      expect(screen.getByRole("button", { name: "Send" })).toBeEnabled();
    });

    fireEvent.change(screen.getByLabelText("Message"), { target: { value: "first run" } });
    fireEvent.click(screen.getByRole("button", { name: "Send" }));
    expect(await screen.findByText("first token")).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText("Message"), { target: { value: "second run" } });
    fireEvent.click(screen.getByRole("button", { name: "Send" }));
    expect(await screen.findByText("second token")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Run details" }));
    expect(await screen.findByText("initial-two")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("tab", { name: "Tape" }));
    expect(await screen.findByText(/run-two-initial/)).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: / Run$/ }));
    fireEvent.click(await screen.findByRole("option", { name: runOneId }));
    fireEvent.click(screen.getByRole("button", { name: / Run$/ }));
    fireEvent.click(await screen.findByRole("option", { name: runTwoId }));

    await waitFor(() => {
      expect(chatRunStatus).toHaveBeenCalledTimes(3);
      expect(chatRunEvents).toHaveBeenCalledTimes(3);
    });

    runTwoFreshStatus.resolve({
      run: createRunStatus(runTwoId, "fresh-two", 22, 300),
    });
    runTwoFreshEvents.resolve({
      run: createRunStatus(runTwoId, "fresh-two", 22, 300),
      tape: createRunTape(runTwoId, "run-two-fresh"),
    });

    fireEvent.click(screen.getByRole("tab", { name: "Status" }));
    await waitFor(() => {
      expect(screen.getByText("fresh-two")).toBeInTheDocument();
    });

    fireEvent.click(screen.getByRole("tab", { name: "Tape" }));
    await waitFor(() => {
      expect(screen.getByText(/run-two-fresh/)).toBeInTheDocument();
    });

    runOneStatus.resolve({
      run: createRunStatus(runOneId, "stale-one", 99, 400),
    });
    runOneEvents.resolve({
      run: createRunStatus(runOneId, "stale-one", 99, 400),
      tape: createRunTape(runOneId, "run-one-stale"),
    });

    await waitFor(() => {
      expect(screen.getByText(/run-two-fresh/)).toBeInTheDocument();
      expect(screen.queryByText(/run-one-stale/)).not.toBeInTheDocument();
    });

    fireEvent.click(screen.getByRole("tab", { name: "Status" }));
    await waitFor(() => {
      expect(screen.getByText("fresh-two")).toBeInTheDocument();
      expect(screen.queryByText("stale-one")).not.toBeInTheDocument();
    });
  }, 15_000);

  it("honors deep-linked session and run query parameters", async () => {
    const primarySession = sampleSession();
    const deepLinkedSession = {
      ...sampleSession(),
      session_id: "01ARZ3NDEKTSV4RRFFQ69G5FB0",
      session_label: "Ops session",
      updated_at_unix_ms: 150,
      last_run_id: "01ARZ3NDEKTSV4RRFFQ69G5RUN",
    } satisfies ChatSessionRecord;
    const deepLinkedRunId = deepLinkedSession.last_run_id ?? "01ARZ3NDEKTSV4RRFFQ69G5RUN";

    const api = {
      listChatSessions: vi.fn().mockResolvedValue({
        sessions: [primarySession, deepLinkedSession],
      }),
      chatRunStatus: vi.fn().mockResolvedValue({
        run: createRunStatus(deepLinkedRunId, "deep-state", 17, 250),
      }),
      chatRunEvents: vi.fn().mockResolvedValue({
        run: createRunStatus(deepLinkedRunId, "deep-state", 17, 250),
        tape: createRunTape(deepLinkedRunId, "deep-linked-marker"),
      }),
    } as unknown as ConsoleApiClient;

    renderWithRouter(
      <ChatConsolePanel
        api={api}
        revealSensitiveValues={false}
        setError={vi.fn()}
        setNotice={vi.fn()}
      />,
      [`/control/chat?sessionId=${deepLinkedSession.session_id}&runId=${deepLinkedRunId}`],
    );

    expect((await screen.findAllByRole("heading", { name: "Ops session" })).length).toBeGreaterThan(0);
    expect(await screen.findByText("deep-state")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("tab", { name: "Tape" }));
    expect(await screen.findByText(/deep-linked-marker/)).toBeInTheDocument();
  });
});

function renderWithRouter(ui: ReactElement, initialEntries = ["/control/chat"]) {
  return render(<MemoryRouter initialEntries={initialEntries}>{ui}</MemoryRouter>);
}

function sampleSession(): ChatSessionRecord {
  return {
    session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
    session_key: "web",
    principal: "admin:web-console",
    device_id: "device-1",
    channel: "web",
    created_at_unix_ms: 100,
    updated_at_unix_ms: 100,
  };
}

function emitCompletedRun(
  onLine: (line: ChatStreamLine) => void,
  sessionId: string,
  runId: string,
  token: string,
): void {
  onLine({
    type: "meta",
    run_id: runId,
    session_id: sessionId,
  });
  onLine({
    type: "event",
    event: {
      run_id: runId,
      event_type: "model_token",
      model_token: {
        token,
        is_final: true,
      },
    },
  });
  onLine({
    type: "complete",
    run_id: runId,
    status: "done",
  });
}

function createRunStatus(
  runId: string,
  state: string,
  promptTokens: number,
  updatedAtUnixMs: number,
): ChatRunStatusRecord {
  return {
    run_id: runId,
    session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
    state,
    cancel_requested: false,
    principal: "admin:web-console",
    device_id: "device-1",
    channel: "web",
    prompt_tokens: promptTokens,
    completion_tokens: 5,
    total_tokens: promptTokens + 5,
    created_at_unix_ms: 100,
    started_at_unix_ms: 110,
    updated_at_unix_ms: updatedAtUnixMs,
    tape_events: 1,
  };
}

function createRunTape(runId: string, marker: string): ChatRunTapeSnapshot {
  return {
    run_id: runId,
    limit: 256,
    max_response_bytes: 65_536,
    returned_bytes: marker.length,
    events: [
      {
        seq: 1,
        event_type: "status",
        payload_json: JSON.stringify({ marker }),
      },
    ],
  };
}

function createDeferred<T>(): {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (error: unknown) => void;
} {
  let resolve: (value: T) => void = () => {};
  let reject: (error: unknown) => void = () => {};
  const promise = new Promise<T>((innerResolve, innerReject) => {
    resolve = innerResolve;
    reject = innerReject;
  });
  return { promise, resolve, reject };
}
