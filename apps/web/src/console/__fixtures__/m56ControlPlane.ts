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
      capability(
        "channels",
        "channels",
        "channels",
        "Channel connector status, test, and enablement",
      ),
      capability(
        "channel.router",
        "channels",
        "channels",
        "Router previews, pairings, and warnings",
      ),
      capability("browser.profiles", "browser", "browser", "Browser profile lifecycle"),
      capability("memory", "memory", "memory", "Memory status, search, and purge"),
      capability(
        "skills",
        "skills",
        "skills",
        "Skill install, verify, audit, quarantine, and enable",
      ),
      capability("auth.openai", "auth", "auth", "OpenAI provider auth contract surface"),
      capability("config.mutate", "config", "config", "Config mutate, migrate, and recover", {
        notes:
          "Dashboard executes redacted inspect, validate, mutate, migrate, and recover flows without raw config hand edits.",
      }),
      capability("secrets", "secrets", "config", "Secret metadata, reveal, write, and delete"),
      capability("pairing", "pairing", "access", "DM pairing codes and approval state"),
      capability(
        "gateway.access",
        "deployment",
        "access",
        "Gateway access and deployment posture summary",
      ),
      capability(
        "gateway.access.verify_remote",
        "deployment",
        "access",
        "Remote dashboard URL verification",
        {
          owner: "palyra-cli",
          execution_mode: "generated_cli",
          dashboard_exposure: "cli_handoff",
          cli_handoff_commands: [
            "cargo run -p palyra-cli -- daemon dashboard-url --verify-remote --json",
          ],
          notes:
            "Remote verification stays CLI-driven because it may require host-local trust material.",
        },
      ),
      capability("gateway.access.tunnel", "deployment", "access", "SSH tunnel helper", {
        owner: "palyra-cli",
        execution_mode: "generated_cli",
        dashboard_exposure: "cli_handoff",
        cli_handoff_commands: [
          "cargo run -p palyra-cli -- tunnel --ssh <user>@<host> --remote-port 7142 --local-port 7142",
        ],
        notes:
          "Tunnel setup remains a CLI handoff because it depends on operator-specific SSH topology.",
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
        message:
          "M56 publishes dashboard exposure and CLI handoff metadata for every current capability.",
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
    last_remote_admin_access_attempt: {
      observed_at_unix_ms: 1700000002500,
      remote_ip_fingerprint: "sha256:remote-admin-1",
      method: "GET",
      path: "/console",
      status_code: 200,
      outcome: "allowed",
    },
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

export function doctorRecoveryJobsFixture() {
  return {
    contract: controlPlaneContract(),
    jobs: [
      {
        job_id: "doctor-job-1",
        state: "queued",
        requested_at_unix_ms: 1700000002600,
        command: ["doctor", "--json", "--repair", "--dry-run"],
        command_output: "",
      },
      {
        job_id: "doctor-job-0",
        state: "succeeded",
        requested_at_unix_ms: 1700000001200,
        completed_at_unix_ms: 1700000002000,
        command: ["doctor", "--json", "--repair", "--dry-run"],
        report: {
          mode: "repair_preview",
          recovery: {
            requested: true,
            dry_run: true,
            force: false,
            run_id: "01HRECOVERYRUN0",
            backup_manifest_path: "state/recovery/runs/01HRECOVERYRUN0/manifest.json",
            planned_steps: [{ id: "config.initialize" }, { id: "node_runtime.normalize" }],
            applied_steps: [],
            available_runs: [
              {
                run_id: "01HRECOVERYRUN0",
                rollback_command: "cargo run -p palyra-cli -- doctor --rollback-run 01HRECOVERYRUN0",
              },
            ],
            next_steps: ["Review the preview before applying repairs."],
          },
        },
        command_output: "{\\n  \\\"mode\\\": \\\"repair_preview\\\"\\n}",
      },
    ],
    page: {
      limit: 20,
      returned: 2,
      has_more: false,
    },
  };
}

export function doctorRecoveryJobFixture(jobId = "doctor-job-1") {
  return {
    contract: controlPlaneContract(),
    job:
      doctorRecoveryJobsFixture().jobs.find((entry) => entry.job_id === jobId) ??
      doctorRecoveryJobsFixture().jobs[0],
  };
}

export function nodePairingListFixture() {
  return {
    contract: controlPlaneContract(),
    codes: [
      {
        code: "221144",
        method: "pin",
        issued_by: "admin:web-console",
        created_at_unix_ms: 1700000000000,
        expires_at_unix_ms: 1700000600000,
      },
    ],
    requests: [
      {
        request_id: "pair-req-pending",
        session_id: "session-pending",
        device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAZ",
        client_kind: "node",
        method: "pin",
        code_issued_by: "admin:web-console",
        requested_at_unix_ms: 1700000001000,
        expires_at_unix_ms: 1700000601000,
        approval_id: "APR-PENDING-1",
        state: "pending_approval",
        identity_fingerprint: "sha256:pending-node-fingerprint",
        transcript_hash_hex: "pending-transcript-hash",
        cert_expires_at_unix_ms: 1700086401000,
      },
      {
        request_id: "pair-req-approved",
        session_id: "session-approved",
        device_id: "01ARZ3NDEKTSV4RRFFQ69G5FBZ",
        client_kind: "node",
        method: "pin",
        code_issued_by: "admin:web-console",
        requested_at_unix_ms: 1700000001200,
        expires_at_unix_ms: 1700000601200,
        approval_id: "APR-APPROVED-1",
        state: "approved",
        identity_fingerprint: "sha256:approved-node-fingerprint",
        transcript_hash_hex: "approved-transcript-hash",
        cert_expires_at_unix_ms: 1700086401200,
      },
      {
        request_id: "pair-req-rejected",
        session_id: "session-rejected",
        device_id: "01ARZ3NDEKTSV4RRFFQ69G5FCZ",
        client_kind: "desktop",
        method: "qr",
        code_issued_by: "admin:web-console",
        requested_at_unix_ms: 1700000001300,
        expires_at_unix_ms: 1700000601300,
        approval_id: "APR-REJECTED-1",
        state: "rejected",
        decision_reason: "operator rejected test device",
        identity_fingerprint: "sha256:rejected-device-fingerprint",
        transcript_hash_hex: "rejected-transcript-hash",
      },
      {
        request_id: "pair-req-expired",
        session_id: "session-expired",
        device_id: "01ARZ3NDEKTSV4RRFFQ69G5FDZ",
        client_kind: "node",
        method: "pin",
        code_issued_by: "admin:web-console",
        requested_at_unix_ms: 1700000001400,
        expires_at_unix_ms: 1700000101400,
        approval_id: "APR-EXPIRED-1",
        state: "expired",
        identity_fingerprint: "sha256:expired-node-fingerprint",
        transcript_hash_hex: "expired-transcript-hash",
      },
    ],
    page: {
      limit: 20,
      returned: 4,
      has_more: false,
    },
  };
}

export function inventoryListFixture() {
  return {
    contract: controlPlaneContract(),
    generated_at_unix_ms: 1700000003000,
    summary: {
      devices: 3,
      trusted_devices: 1,
      pending_pairings: 1,
      ok_devices: 1,
      stale_devices: 1,
      degraded_devices: 0,
      offline_devices: 1,
      ok_instances: 1,
      stale_instances: 0,
      degraded_instances: 0,
      offline_instances: 0,
    },
    devices: [
      {
        device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAZ",
        client_kind: "node",
        device_status: "paired",
        trust_state: "trusted",
        presence_state: "stale",
        paired_at_unix_ms: 1700000001000,
        updated_at_unix_ms: 1700000001500,
        last_seen_at_unix_ms: 1700000001800,
        heartbeat_age_ms: 600000,
        latest_session_id: "session-pending",
        pending_pairings: 1,
        issued_by: "admin:web-console",
        approval_id: "APR-PENDING-1",
        identity_fingerprint: "sha256:pending-node-fingerprint",
        transcript_hash_hex: "pending-transcript-hash",
        current_certificate_fingerprint: "sha256:cert-pending-2",
        certificate_fingerprint_history: ["sha256:cert-pending-1", "sha256:cert-pending-2"],
        platform: "windows",
        capabilities: [{ name: "ping", available: true }],
        capability_summary: { total: 1, available: 1, unavailable: 0 },
        current_certificate_expires_at_unix_ms: 1700086401000,
        warnings: ["heartbeat stale"],
        actions: {
          can_rotate: true,
          can_revoke: true,
          can_remove: true,
          can_invoke: true,
        },
      },
      {
        device_id: "01ARZ3NDEKTSV4RRFFQ69G5FBZ",
        client_kind: "node",
        device_status: "paired",
        trust_state: "trusted",
        presence_state: "ok",
        paired_at_unix_ms: 1700000001200,
        updated_at_unix_ms: 1700000001800,
        last_seen_at_unix_ms: 1700000002200,
        heartbeat_age_ms: 1000,
        latest_session_id: "session-approved",
        pending_pairings: 0,
        issued_by: "admin:web-console",
        approval_id: "APR-APPROVED-1",
        identity_fingerprint: "sha256:approved-node-fingerprint",
        transcript_hash_hex: "approved-transcript-hash",
        current_certificate_fingerprint: "sha256:cert-approved-1",
        certificate_fingerprint_history: ["sha256:cert-approved-1"],
        platform: "macos",
        capabilities: [{ name: "ping", available: true }],
        capability_summary: { total: 1, available: 1, unavailable: 0 },
        current_certificate_expires_at_unix_ms: 1700086401200,
        warnings: [],
        actions: {
          can_rotate: true,
          can_revoke: true,
          can_remove: true,
          can_invoke: true,
        },
      },
      {
        device_id: "01ARZ3NDEKTSV4RRFFQ69G5FCZ",
        client_kind: "desktop",
        device_status: "revoked",
        trust_state: "revoked",
        presence_state: "offline",
        paired_at_unix_ms: 1700000001300,
        updated_at_unix_ms: 1700000002300,
        pending_pairings: 0,
        issued_by: "admin:web-console",
        approval_id: "APR-REJECTED-1",
        identity_fingerprint: "sha256:rejected-device-fingerprint",
        transcript_hash_hex: "rejected-transcript-hash",
        current_certificate_fingerprint: undefined,
        certificate_fingerprint_history: [],
        capabilities: [],
        capability_summary: { total: 0, available: 0, unavailable: 0 },
        revoked_reason: "operator rejected test device",
        warnings: ["device revoked"],
        actions: {
          can_rotate: false,
          can_revoke: false,
          can_remove: true,
          can_invoke: false,
        },
      },
    ],
    pending_pairings: [nodePairingListFixture().requests[0]],
    instances: [
      {
        instance_id: "palyrad",
        label: "palyrad",
        kind: "daemon",
        presence_state: "ok",
        observed_at_unix_ms: 1700000003000,
        state_label: "running",
        detail: "operator console reachable",
        capability_summary: { total: 0, available: 0, unavailable: 0 },
      },
    ],
    page: {
      limit: 20,
      returned: 3,
      has_more: false,
    },
  };
}

export function inventoryDeviceDetailFixture(deviceId = "01ARZ3NDEKTSV4RRFFQ69G5FAZ") {
  const inventory = inventoryListFixture();
  return {
    contract: controlPlaneContract(),
    generated_at_unix_ms: inventory.generated_at_unix_ms,
    device:
      inventory.devices.find((record) => record.device_id === deviceId) ?? inventory.devices[0],
    pairings: nodePairingListFixture().requests.filter((record) => record.device_id === deviceId),
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
    observability: {
      failure_classes: {
        config_failure: 1,
        upstream_provider_failure: 2,
        product_failure: 1,
      },
      provider_auth: {
        attempts: 8,
        failures: 2,
        failure_rate_bps: 2500,
        refresh_failures: 1,
        state: "degraded",
      },
      dashboard: {
        attempts: 12,
        successes: 10,
        failures: 2,
        failure_rate_bps: 1666,
      },
      support_bundle: {
        attempts: 4,
        successes: 3,
        failures: 1,
        success_rate_bps: 7500,
        last_job: {
          job_id: "support-job-0",
          state: "succeeded",
          requested_at_unix_ms: 1699999999000,
          completed_at_unix_ms: 1700000001000,
          output_path: "state/support-bundles/support-job-0.zip",
        },
      },
      doctor_recovery: {
        queued: 1,
        running: 0,
        succeeded: 1,
        failed: 0,
        last_job: {
          job_id: "doctor-job-0",
          state: "succeeded",
          requested_at_unix_ms: 1700000001200,
          completed_at_unix_ms: 1700000002000,
          command: ["doctor", "--json", "--repair", "--dry-run"],
          mode: "repair_preview",
          requested: true,
          dry_run: true,
          force: false,
          run_id: "01HRECOVERYRUN0",
          backup_manifest_path: "state/recovery/runs/01HRECOVERYRUN0/manifest.json",
          planned_step_count: 2,
          applied_step_count: 0,
          available_run_count: 1,
          next_steps: ["Review the preview before applying repairs."],
        },
      },
      connector: {
        connectors: 1,
        degraded_connectors: 1,
        paused_connectors: 0,
        queue_depth: 6,
        dead_letters: 2,
        upload_failures: 1,
        upload_failure_rate_bps: 10000,
        recent_errors: [
          {
            connector_id: "discord:default",
            message: "attachment.upload.failed: remote upload rejected",
          },
        ],
      },
      browser: {
        relay_actions: {
          attempts: 5,
          failures: 1,
          failure_rate_bps: 2000,
        },
        recent_failure_samples: ["relay action timed out while switching tabs"],
      },
      recent_failures: [
        {
          operation: "provider_auth_refresh",
          failure_class: "upstream_provider_failure",
          message: "provider auth request failed with http 502",
        },
        {
          operation: "discord_upload",
          failure_class: "product_failure",
          message: "attachment upload failed after retries",
        },
      ],
      triage: {
        failure_classes: ["config_failure", "upstream_provider_failure", "product_failure"],
        common_order: [
          "Check deployment posture and operator auth first.",
          "Check OpenAI profile health and refresh metrics next.",
          "Check Discord queue depth, dead letters, and upload failures next.",
        ],
      },
    },
    media: {
      policy: {
        download_enabled: false,
        outbound_upload_enabled: false,
        allowed_source_hosts: ["cdn.discordapp.com"],
      },
      usage: {
        artifact_count: 2,
        stored_bytes: 16384,
      },
      retention: {
        max_store_bytes: 67108864,
        ttl_ms: 604800000,
      },
      recent_blocked_reasons: [
        {
          connector_id: "discord:default",
          event_type: "attachment.download.blocked",
          reason: "attachment.download disabled by config",
        },
      ],
      recent_upload_failures: [],
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
