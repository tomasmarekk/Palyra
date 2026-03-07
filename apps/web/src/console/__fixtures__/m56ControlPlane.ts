export function controlPlaneContract() {
  return { contract_version: "control-plane.v1" };
}

export function capabilityCatalogFixture() {
  return {
    contract: controlPlaneContract(),
    version: "capability-catalog.v2",
    generated_at_unix_ms: 1700000000000,
    capabilities: [
      capability("chat.sessions", "chat", "chat", "Chat sessions and run status"),
      capability("approvals", "approvals", "approvals", "Approval inbox and decisions"),
      capability("cron", "cron", "cron", "Cron job create, update, run-now, and logs"),
      capability("channels", "channels", "channels", "Channel connector status, test, and enablement"),
      capability("channel.router", "channels", "channels", "Router previews, pairings, and warnings"),
      capability("browser.profiles", "browser", "browser", "Browser profile lifecycle"),
      capability("memory", "memory", "memory", "Memory status, search, and purge"),
      capability("skills", "skills", "skills", "Skill install, verify, audit, quarantine, and enable"),
      capability("auth.openai", "auth", "auth", "OpenAI provider auth contract surface"),
      capability("config.mutate", "config", "config", "Config mutate, migrate, and recover", {
        notes: "Dashboard executes redacted inspect, validate, mutate, migrate, and recover flows without raw config hand edits.",
      }),
      capability("secrets", "secrets", "config", "Secret metadata, reveal, write, and delete"),
      capability("pairing", "pairing", "access", "DM pairing codes and approval state"),
      capability("gateway.access", "deployment", "access", "Gateway access and deployment posture summary"),
      capability("gateway.access.verify_remote", "deployment", "access", "Remote dashboard URL verification", {
        owner: "palyra-cli",
        execution_mode: "generated_cli",
        dashboard_exposure: "cli_handoff",
        cli_handoff_commands: [
          "cargo run -p palyra-cli -- daemon dashboard-url --verify-remote --json",
        ],
        notes: "Remote verification stays CLI-driven because it may require host-local trust material.",
      }),
      capability("gateway.access.tunnel", "deployment", "access", "SSH tunnel helper", {
        owner: "palyra-cli",
        execution_mode: "generated_cli",
        dashboard_exposure: "cli_handoff",
        cli_handoff_commands: [
          "cargo run -p palyra-cli -- tunnel --ssh <user>@<host> --remote-port 7142 --local-port 7142",
        ],
        notes: "Tunnel setup remains a CLI handoff because it depends on operator-specific SSH topology.",
      }),
      capability("runtime.health", "runtime", "operations", "Daemon and runtime health"),
      capability("runtime.doctor", "runtime", "operations", "Doctor JSON diagnostics export", {
        owner: "palyra-cli",
        execution_mode: "generated_cli",
        dashboard_exposure: "cli_handoff",
        cli_handoff_commands: ["cargo run -p palyra-cli -- doctor --json"],
      }),
      capability("protocol.contracts", "protocol", "operations", "Protocol validation utilities", {
        owner: "scripts",
        execution_mode: "generated_cli",
        dashboard_exposure: "cli_handoff",
        cli_handoff_commands: [
          "bash scripts/protocol/check-generated-stubs.sh",
          "pwsh scripts/protocol/check-generated-stubs.ps1",
        ],
      }),
      capability("policy.explain", "policy", "operations", "Policy explain developer surface", {
        surfaces: ["backend", "internal"],
        execution_mode: "internal",
        dashboard_exposure: "internal_only",
        notes: "Policy explain stays admin-only because it exposes low-level evaluation detail.",
      }),
      capability("support.bundle", "support", "support", "Support bundle export jobs"),
    ],
    migration_notes: [
      {
        id: "m56-capability-exposure",
        message: "M56 publishes dashboard exposure and CLI handoff metadata for every current capability.",
      },
    ],
  };
}

export function deploymentPostureFixture(overrides: Partial<Record<string, unknown>> = {}) {
  return {
    contract: controlPlaneContract(),
    mode: "local",
    bind_profile: "loopback",
    bind_addresses: {
      admin: "127.0.0.1:7142",
      grpc: "127.0.0.1:50051",
      quic: "127.0.0.1:7443",
    },
    tls: {
      gateway_enabled: true,
    },
    admin_auth_required: true,
    dangerous_remote_bind_ack: {
      config: false,
      env: false,
      env_name: "PALYRA_GATEWAY_DANGEROUS_REMOTE_BIND_ACK",
    },
    remote_bind_detected: false,
    warnings: [
      "Remote gateway exposure requires explicit verification and operator acknowledgement.",
    ],
    ...overrides,
  };
}

export function pairingSummaryFixture() {
  return {
    contract: controlPlaneContract(),
    channels: [
      {
        channel: "discord:default",
        pending: [
          {
            channel: "discord:default",
            sender_identity: "discord:user:pending",
            code: "654321",
            requested_at_unix_ms: 1700000001000,
            expires_at_unix_ms: 1700000601000,
            approval_id: "APR-PAIR-1",
          },
        ],
        paired: [
          {
            channel: "discord:default",
            sender_identity: "discord:user:paired",
            approved_at_unix_ms: 1700000000000,
            approval_id: "APR-PAIR-2",
          },
        ],
        active_codes: [
          {
            code: "123456",
            channel: "discord:default",
            issued_by: "admin:web-console",
            created_at_unix_ms: 1700000000000,
            expires_at_unix_ms: 1700000600000,
          },
        ],
      },
    ],
  };
}

export function supportBundleJobsFixture() {
  return {
    contract: controlPlaneContract(),
    jobs: [
      {
        job_id: "support-job-1",
        state: "queued",
        requested_at_unix_ms: 1700000002000,
        command_output: "",
      },
      {
        job_id: "support-job-0",
        state: "succeeded",
        requested_at_unix_ms: 1699999999000,
        completed_at_unix_ms: 1700000001000,
        output_path: "state/support-bundles/support-job-0.zip",
        command_output: "bundle ready",
      },
    ],
    page: {
      limit: 20,
      returned: 2,
      has_more: false,
    },
  };
}

export function supportBundleJobFixture(jobId = "support-job-1") {
  return {
    contract: controlPlaneContract(),
    job: {
      job_id: jobId,
      state: "queued",
      requested_at_unix_ms: 1700000002000,
      command_output: "",
    },
  };
}

export function diagnosticsFixture() {
  return {
    generated_at_unix_ms: 1700000003000,
    model_provider: {
      provider: "openai",
      state: "ready",
    },
    rate_limits: {
      request_budget: "30/min",
      reset_at_unix_ms: 1700000009000,
    },
    auth_profiles: {
      state: "ok",
      profiles: [{ profile_id: "openai-default" }],
    },
    browserd: {
      state: "ready",
      engine_mode: "chromium",
    },
  };
}

export function auditEventsFixture() {
  return {
    events: [
      {
        event_id: "evt-1",
        principal: "admin:web-console",
        event: "message.routed",
        payload: { connector: "discord:default", status: "accepted" },
      },
      {
        event_id: "evt-2",
        principal: "admin:web-console",
        event: "support.bundle.created",
        payload: { job_id: "support-job-1" },
      },
    ],
    page: {
      limit: 50,
      returned: 2,
      has_more: false,
    },
  };
}

type CapabilityOverrides = Partial<{
  owner: string;
  surfaces: string[];
  execution_mode: string;
  dashboard_exposure: "direct_action" | "cli_handoff" | "internal_only";
  cli_handoff_commands: string[];
  notes: string;
}>;

function capability(
  id: string,
  domain: string,
  dashboardSection: string,
  title: string,
  overrides: CapabilityOverrides = {},
) {
  return {
    id,
    domain,
    dashboard_section: dashboardSection,
    title,
    owner: overrides.owner ?? "palyrad",
    surfaces: overrides.surfaces ?? ["backend", "dashboard"],
    execution_mode: overrides.execution_mode ?? "direct_ui",
    dashboard_exposure: overrides.dashboard_exposure ?? "direct_action",
    cli_handoff_commands: overrides.cli_handoff_commands ?? [],
    mutation_classes: ["deployment"],
    test_refs: ["apps/web/src/App.test.tsx"],
    contract_paths: ["/console/v1/example"],
    notes: overrides.notes,
  };
}
