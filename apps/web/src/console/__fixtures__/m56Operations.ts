import { controlPlaneContract } from "./m56ControlPlane";

export function configInspectFixture(documentToml = "version = 1\n[model_provider]\nauth_profile_id = \"openai-default\"\n") {
  return {
    contract: controlPlaneContract(),
    source_path: "palyra.toml",
    config_version: 1,
    migrated_from_version: 0,
    redacted: true,
    document_toml: documentToml,
    backups: [
      { index: 1, path: "palyra.toml.bak.1", exists: true },
      { index: 2, path: "palyra.toml.bak.2", exists: true },
    ],
  };
}

export function configValidationFixture(valid = true) {
  return {
    contract: controlPlaneContract(),
    source_path: "palyra.toml",
    valid,
    config_version: 1,
    migrated_from_version: 0,
  };
}

export function configMutationFixture(operation = "set", changedKey = "model_provider.auth_profile_id") {
  return {
    contract: controlPlaneContract(),
    operation,
    source_path: "palyra.toml",
    backups_retained: 3,
    config_version: 1,
    migrated_from_version: 0,
    changed_key: changedKey,
  };
}

export function secretMetadataListFixture() {
  return {
    contract: controlPlaneContract(),
    scope: "global",
    secrets: [
      {
        scope: "global",
        key: "openai_api_key",
        created_at_unix_ms: 1700000001000,
        updated_at_unix_ms: 1700000002000,
        value_bytes: 32,
      },
    ],
    page: {
      limit: 50,
      returned: 1,
      has_more: false,
    },
  };
}

export function secretMetadataFixture() {
  return {
    contract: controlPlaneContract(),
    secret: secretMetadataListFixture().secrets[0],
  };
}

export function secretRevealFixture() {
  return {
    contract: controlPlaneContract(),
    scope: "global",
    key: "openai_api_key",
    value_bytes: 11,
    value_base64: "c2stdGVzdC1rZXk=",
    value_utf8: "sk-test-key",
  };
}

export function cronJobsFixture() {
  return {
    jobs: [
      { job_id: "cron-1", name: "nightly", enabled: true },
    ],
  };
}

export function cronRunsFixture() {
  return {
    runs: [
      {
        run_id: "cron-run-1",
        job_id: "cron-1",
        state: "succeeded",
        started_at_unix_ms: 1700000003000,
        completed_at_unix_ms: 1700000004000,
      },
    ],
  };
}

export function channelsListFixture() {
  return {
    connectors: [
      {
        connector_id: "discord:default",
        kind: "discord",
        availability: "supported",
        enabled: true,
        readiness: "ready",
        liveness: "running",
      },
    ],
  };
}

export function channelStatusFixture() {
  return {
    connector: {
      connector_id: "discord:default",
      kind: "discord",
      availability: "supported",
      enabled: true,
      readiness: "ready",
      liveness: "running",
    },
  };
}

export function channelLogsFixture() {
  return {
    events: [
      {
        event_id: 1,
        connector_id: "discord:default",
        event_type: "message.received",
        message: "received",
      },
    ],
    dead_letters: [
      {
        dead_letter_id: 1,
        connector_id: "discord:default",
        reason: "oversized",
      },
    ],
  };
}

export function routerRulesFixture() {
  return {
    config: {
      mention_required: true,
      dm_pairing_required: true,
    },
    config_hash: "router-hash-1",
  };
}

export function routerWarningsFixture() {
  return {
    warnings: ["Broadcast messages remain denied by default."],
    config_hash: "router-hash-1",
  };
}

export function routerPreviewFixture() {
  return {
    preview: {
      accepted: true,
      reason: "paired_dm",
    },
  };
}

export function routerPairingsFixture() {
  return {
    pairings: [
      {
        channel: "discord:default",
        sender_identity: "discord:user:paired",
        approval_id: "APR-PAIR-2",
      },
    ],
  };
}

export function routerMintFixture() {
  return {
    code: {
      code: "777888",
      channel: "discord:default",
      issued_by: "admin:web-console",
      expires_at_unix_ms: 1700000600000,
    },
  };
}

export function discordPreflightFixture() {
  return {
    preflight: {
      invite_url_template: "https://discord.test/oauth",
      required_permissions: ["ViewChannel", "SendMessages"],
      egress_allowlist: ["discord.com", "cdn.discordapp.com"],
      security_defaults: ["dm_only", "deny_broadcast"],
    },
  };
}

export function discordApplyFixture() {
  return {
    status: "applied",
    connector_id: "discord:default",
  };
}

export function memoryStatusFixture() {
  return {
    usage: {
      item_count: 12,
      vector_count: 12,
    },
    retention: {
      ttl_days: 30,
    },
    maintenance: {
      last_vacuum_at_unix_ms: 1700000000000,
    },
  };
}

export function memoryHitsFixture() {
  return {
    hits: [
      {
        memory_id: "mem-1",
        channel: "discord:default",
        content: "paired sender prefers concise replies",
      },
    ],
  };
}

export function skillsFixture() {
  return {
    entries: [
      {
        record: {
          skill_id: "acme.echo_http",
          version: "1.2.3",
        },
        status: "active",
      },
    ],
  };
}

export function browserProfilesFixture() {
  return {
    principal: "admin:web-console",
    active_profile_id: "profile-1",
    profiles: [
      {
        profile_id: "profile-1",
        name: "Primary Browser",
        principal: "admin:web-console",
        persistence_enabled: true,
        private_profile: false,
      },
    ],
  };
}

export function browserRelayTokenFixture() {
  return {
    relay_token: "relay-token-1",
    expires_at_unix_ms: 1700000600000,
  };
}

export function browserRelayActionFixture() {
  return {
    success: true,
    action: "capture_selection",
    error: "",
    result: {
      selected_text: "ok",
    },
  };
}

export function browserDownloadsFixture() {
  return {
    artifacts: [
      {
        artifact_id: "artifact-1",
        session_id: "browser-session-1",
        quarantined: false,
        file_name: "report.csv",
      },
    ],
    error: "",
  };
}
