import { controlPlaneContract } from "./m56ControlPlane";

export function configInspectFixture(
  documentToml = 'version = 1\n[model_provider]\nauth_profile_id = "openai-default"\n',
) {
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

export function configMutationFixture(
  operation = "set",
  changedKey = "model_provider.auth_profile_id",
) {
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

export function configuredSecretListFixture() {
  return {
    contract: controlPlaneContract(),
    generated_at_unix_ms: 1700000002500,
    snapshot_generation: 2,
    secrets: [
      {
        secret_id: "model_provider.openai_api_key_secret_ref:fp-1",
        component: "model_provider",
        config_path: "model_provider.openai_api_key_secret_ref",
        status: "healthy",
        resolution_scope: "startup",
        reload_action: "hot_safe",
        snapshot_generation: 2,
        source: {
          kind: "vault",
          fingerprint: "fp-1",
          required: true,
          refresh_policy: "startup_only",
          snapshot_policy: "runtime_snapshot",
          description: "Vault-backed OpenAI API key",
          display_name: "OpenAI API key",
          redaction_label: "OpenAI API key",
        },
        affected_components: ["model_provider"],
        last_resolved_at_unix_ms: 1700000002000,
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

export function configuredSecretDetailFixture() {
  return {
    contract: controlPlaneContract(),
    generated_at_unix_ms: 1700000002500,
    snapshot_generation: 2,
    secret: configuredSecretListFixture().secrets[0],
  };
}

export function cronJobsFixture() {
  return {
    routines: [
      {
        routine_id: "cron-1",
        job_id: "cron-1",
        name: "nightly",
        enabled: true,
        trigger_kind: "schedule",
        schedule_type: "every",
        schedule_payload: { interval_ms: 3600000 },
        delivery_mode: "same_channel",
        channel: "web",
        last_run: {
          run_id: "cron-run-1",
          outcome_kind: "success_with_output",
          status: "succeeded",
          outcome_message: "queued",
        },
      },
    ],
  };
}

export function cronRunsFixture() {
  return {
    runs: [
      {
        run_id: "cron-run-1",
        routine_id: "cron-1",
        status: "succeeded",
        outcome_kind: "success_with_output",
        outcome_message: "queued",
        started_at_unix_ms: 1700000003000,
        finished_at_unix_ms: 1700000004000,
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
      runtime: {
        media: {
          policy: {
            download_enabled: false,
            outbound_upload_enabled: false,
          },
          usage: {
            artifact_count: 1,
            stored_bytes: 4096,
          },
          recent_blocked_reasons: [
            {
              event_type: "attachment.metadata.blocked",
              reason: "attachment_metadata_type_blocked",
            },
          ],
          recent_upload_failures: [
            {
              event_type: "attachment.upload.failed",
              reason: "attachment.upload disabled by config",
            },
          ],
        },
      },
    },
    operations: {
      queue: {
        pending_outbox: 2,
        due_outbox: 1,
        claimed_outbox: 0,
        dead_letters: 1,
        paused: true,
        pause_reason: "operator requested queue pause via console",
      },
      saturation: {
        state: "paused",
        reasons: ["queue_paused", "pause_reason=operator requested queue pause via console"],
      },
      last_auth_failure: "discord token validation failed",
      rate_limits: {
        global_retry_after_ms: 500,
        active_route_limits: 1,
      },
      discord: {
        last_permission_failure: "missing permissions: send messages",
      },
    },
    health_refresh: {
      supported: true,
      refreshed: false,
      message: "discord token missing",
      required_permissions: ["ViewChannel", "SendMessages"],
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
    learning: {
      enabled: true,
      sampling_percent: 100,
      cooldown_ms: 300000,
      budget_tokens: 1200,
      max_candidates_per_run: 24,
      durable_fact_review_min_confidence_bps: 7500,
      durable_fact_auto_write_threshold_bps: 9000,
      preference_review_min_confidence_bps: 8000,
      procedure_min_occurrences: 2,
      procedure_review_min_confidence_bps: 8500,
      thresholds: {
        durable_fact: {
          review_min_confidence_bps: 7500,
          auto_apply_confidence_bps: 9000,
        },
        preference: {
          review_min_confidence_bps: 8000,
        },
        procedure: {
          review_min_confidence_bps: 8500,
          min_occurrences: 2,
        },
      },
      counters: {
        reflections_scheduled: 4,
        reflections_completed: 3,
        candidates_created: 5,
        candidates_auto_applied: 1,
      },
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

export function builderCandidateRecordFixture(overrides: Partial<Record<string, unknown>> = {}) {
  return {
    candidate_id: "builder-candidate-1",
    skill_id: "palyra.generated.builder.release_check",
    version: "0.1.0",
    publisher: "palyra.generated",
    name: "Release check builder candidate",
    source_kind: "prompt",
    source_ref: "prompt:01buildercandidate",
    summary: "Summarize deployment readiness and release blockers.",
    status: "quarantined",
    rollout_flag: "PALYRA_EXPERIMENTAL_DYNAMIC_TOOL_BUILDER",
    rollout_enabled: true,
    scaffold_root: "state/skills/builder-candidates/palyra.generated.builder.release_check/0.1.0",
    manifest_path:
      "state/skills/builder-candidates/palyra.generated.builder.release_check/0.1.0/skill.toml",
    capability_declaration_path:
      "state/skills/builder-candidates/palyra.generated.builder.release_check/0.1.0/builder-capabilities.json",
    provenance_path:
      "state/skills/builder-candidates/palyra.generated.builder.release_check/0.1.0/provenance.json",
    test_harness_path:
      "state/skills/builder-candidates/palyra.generated.builder.release_check/0.1.0/tests/smoke.test.json",
    capability_profile: {
      http_hosts: ["api.example.test"],
      secrets: ["release_api_key"],
      storage_prefixes: ["workspace/release"],
      channels: ["discord:default"],
    },
    generated_at_unix_ms: 1700000005000,
    updated_at_unix_ms: 1700000005000,
    ...overrides,
  };
}

export function skillBuilderCandidatesFixture(
  entries = [builderCandidateRecordFixture()],
  rolloutEnabled = true,
) {
  return {
    rollout_flag: "PALYRA_EXPERIMENTAL_DYNAMIC_TOOL_BUILDER",
    rollout_enabled: rolloutEnabled,
    count: entries.length,
    entries,
    skills_root: "state/skills",
  };
}

export function learningCandidatesFixture() {
  return {
    candidates: [
      {
        candidate_id: "candidate-pref-1",
        candidate_kind: "preference",
        title: "Preference: interaction.style",
        summary: "Please use concise status updates for release triage.",
        status: "queued",
        confidence: 0.83,
        risk_level: "normal",
        auto_applied: false,
        content_json:
          '{"key":"interaction.style","value":"Please use concise status updates for release triage.","source_kind":"explicit"}',
        provenance_json:
          '[{"run_id":"run-learning-1","seq":2,"event_type":"message.received","created_at_unix_ms":1700000001200,"excerpt":"Please use concise status updates for release triage."}]',
      },
      {
        candidate_id: "candidate-proc-1",
        candidate_kind: "procedure",
        title: "Procedure candidate: palyra.fs.apply_patch -> palyra.http.fetch",
        summary: "Observed 2 successful runs with the same tool sequence.",
        status: "queued",
        confidence: 0.88,
        risk_level: "review",
        auto_applied: false,
        content_json:
          '{"signature":"palyra.fs.apply_patch -> palyra.http.fetch","successful_runs":["run-1","run-2"]}',
        provenance_json:
          '[{"run_id":"run-1","excerpt":"proposed palyra.fs.apply_patch"},{"run_id":"run-2","excerpt":"proposed palyra.http.fetch"}]',
      },
    ],
  };
}

export function learningCandidateHistoryFixture() {
  return {
    history: [
      {
        history_id: "hist-1",
        candidate_id: "candidate-pref-1",
        status: "accepted",
        reviewed_by_principal: "admin:web-console",
        action_summary: "confirmed preference",
        created_at_unix_ms: 1700000002200,
      },
    ],
  };
}

export function learningPreferencesFixture() {
  return {
    preferences: [
      {
        preference_id: "pref-1",
        key: "interaction.style",
        value: "direct",
        scope_kind: "profile",
        scope_id: "admin:web-console",
        status: "active",
        source_kind: "confirmed",
        confidence: 0.95,
      },
    ],
  };
}

export function procedurePromotionFixture() {
  const builderCandidate = builderCandidateRecordFixture({
    candidate_id: "builder-candidate-procedure-1",
    skill_id: "palyra.generated.ops.release",
    name: "Procedure candidate: palyra.fs.apply_patch -> palyra.http.fetch",
    source_kind: "procedure",
    source_ref: "candidate-proc-1",
    summary: "Observed 2 successful runs with the same tool sequence.",
    scaffold_root: "state/skills/builder-candidates/palyra.generated.ops.release/0.1.0",
    manifest_path: "state/skills/builder-candidates/palyra.generated.ops.release/0.1.0/skill.toml",
    capability_declaration_path:
      "state/skills/builder-candidates/palyra.generated.ops.release/0.1.0/builder-capabilities.json",
    provenance_path:
      "state/skills/builder-candidates/palyra.generated.ops.release/0.1.0/provenance.json",
    test_harness_path:
      "state/skills/builder-candidates/palyra.generated.ops.release/0.1.0/tests/smoke.test.json",
  });
  return {
    candidate: {
      candidate_id: "candidate-proc-1",
      status: "accepted",
      candidate_kind: "procedure",
      title: "Procedure candidate: palyra.fs.apply_patch -> palyra.http.fetch",
    },
    skill: {
      skill_id: "palyra.generated.ops.release",
      version: "0.1.0",
      publisher: "palyra.generated",
      name: "Procedure candidate: palyra.fs.apply_patch -> palyra.http.fetch",
      scaffold_root: builderCandidate.scaffold_root,
      files: [
        `${builderCandidate.scaffold_root}/skill.toml`,
        `${builderCandidate.scaffold_root}/README.md`,
        `${builderCandidate.scaffold_root}/builder-request.json`,
        builderCandidate.capability_declaration_path,
        builderCandidate.test_harness_path,
        `${builderCandidate.scaffold_root}/sbom.cdx.json`,
        builderCandidate.provenance_path,
      ],
      quarantine_status: {
        skill_id: "palyra.generated.ops.release",
        version: "0.1.0",
        status: "quarantined",
        reason: "generated_from_learning_candidate:candidate-proc-1",
        detected_at_ms: 1700000005400,
        operator_principal: "admin:web-console",
      },
    },
    builder_candidate: builderCandidate,
  };
}

export function builderCandidateCreateFixture() {
  const candidate = builderCandidateRecordFixture({
    candidate_id: "builder-candidate-prompt-2",
    skill_id: "palyra.generated.builder.triage_briefing",
    name: "Daily triage briefing",
    source_ref: "prompt:01buildercandidateprompt",
    summary: "Collect overnight incidents and summarize operator actions.",
    scaffold_root: "state/skills/builder-candidates/palyra.generated.builder.triage_briefing/0.1.0",
    manifest_path:
      "state/skills/builder-candidates/palyra.generated.builder.triage_briefing/0.1.0/skill.toml",
    capability_declaration_path:
      "state/skills/builder-candidates/palyra.generated.builder.triage_briefing/0.1.0/builder-capabilities.json",
    provenance_path:
      "state/skills/builder-candidates/palyra.generated.builder.triage_briefing/0.1.0/provenance.json",
    test_harness_path:
      "state/skills/builder-candidates/palyra.generated.builder.triage_briefing/0.1.0/tests/smoke.test.json",
  });
  return {
    rollout_flag: "PALYRA_EXPERIMENTAL_DYNAMIC_TOOL_BUILDER",
    rollout_enabled: true,
    candidate,
    skill: {
      skill_id: candidate.skill_id,
      version: candidate.version,
      publisher: candidate.publisher,
      name: candidate.name,
      scaffold_root: candidate.scaffold_root,
      files: [
        `${candidate.scaffold_root}/skill.toml`,
        `${candidate.scaffold_root}/README.md`,
        `${candidate.scaffold_root}/builder-request.json`,
        candidate.capability_declaration_path,
        candidate.test_harness_path,
        `${candidate.scaffold_root}/sbom.cdx.json`,
        candidate.provenance_path,
      ],
      quarantine_status: {
        skill_id: candidate.skill_id,
        version: candidate.version,
        status: "quarantined",
        reason: `dynamic_builder_candidate:${candidate.candidate_id}`,
        detected_at_ms: 1700000005600,
        operator_principal: "admin:web-console",
      },
    },
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
