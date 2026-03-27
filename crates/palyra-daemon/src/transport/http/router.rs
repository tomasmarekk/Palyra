use axum::{
    extract::DefaultBodyLimit,
    middleware,
    routing::{get, post},
    Router,
};

use crate::{
    app::state::AppState,
    transport::http::{
        handlers::{admin, canvas, console, health, web_ui},
        middleware as http_middleware,
    },
    HTTP_MAX_REQUEST_BODY_BYTES,
};

pub(crate) fn build_router(state: AppState) -> Router {
    let admin_routes = Router::new()
        .route("/admin/v1/status", get(admin::core::admin_status_handler))
        .route("/admin/v1/journal/recent", get(admin::core::admin_journal_recent_handler))
        .route("/admin/v1/policy/explain", get(admin::core::admin_policy_explain_handler))
        .route("/admin/v1/runs/{run_id}", get(admin::core::admin_run_status_handler))
        .route("/admin/v1/runs/{run_id}/tape", get(admin::core::admin_run_tape_handler))
        .route("/admin/v1/runs/{run_id}/cancel", post(admin::core::admin_run_cancel_handler))
        .route("/admin/v1/channels", get(admin::channels::admin_channels_list_handler))
        .route(
            "/admin/v1/channels/logs/query",
            post(admin::channels::admin_channel_logs_query_handler),
        )
        .route(
            "/admin/v1/channels/{connector_id}",
            get(admin::channels::admin_channel_status_handler),
        )
        .route(
            "/admin/v1/channels/{connector_id}/enabled",
            post(admin::channels::admin_channel_set_enabled_handler),
        )
        .route(
            "/admin/v1/channels/{connector_id}/logs",
            get(admin::channels::admin_channel_logs_handler),
        )
        .route(
            "/admin/v1/channels/{connector_id}/operations/health-refresh",
            post(admin::channels::admin_channel_health_refresh_handler),
        )
        .route(
            "/admin/v1/channels/{connector_id}/operations/queue/pause",
            post(admin::channels::admin_channel_queue_pause_handler),
        )
        .route(
            "/admin/v1/channels/{connector_id}/operations/queue/resume",
            post(admin::channels::admin_channel_queue_resume_handler),
        )
        .route(
            "/admin/v1/channels/{connector_id}/operations/queue/drain",
            post(admin::channels::admin_channel_queue_drain_handler),
        )
        .route(
            "/admin/v1/channels/{connector_id}/operations/dead-letters/{dead_letter_id}/replay",
            post(admin::channels::admin_channel_dead_letter_replay_handler),
        )
        .route(
            "/admin/v1/channels/{connector_id}/operations/dead-letters/{dead_letter_id}/discard",
            post(admin::channels::admin_channel_dead_letter_discard_handler),
        )
        .route(
            "/admin/v1/channels/{connector_id}/test",
            post(admin::channels::admin_channel_test_handler),
        )
        .route(
            "/admin/v1/channels/{connector_id}/test-send",
            post(admin::channels::admin_channel_test_send_handler),
        )
        .route(
            "/admin/v1/channels/router/rules",
            get(admin::channels::admin_channel_router_rules_handler),
        )
        .route(
            "/admin/v1/channels/router/warnings",
            get(admin::channels::admin_channel_router_warnings_handler),
        )
        .route(
            "/admin/v1/channels/router/preview",
            post(admin::channels::admin_channel_router_preview_handler),
        )
        .route(
            "/admin/v1/channels/router/pairings",
            get(admin::channels::admin_channel_router_pairings_handler),
        )
        .route(
            "/admin/v1/channels/router/pairing-codes",
            post(admin::channels::admin_channel_router_pairing_code_mint_handler),
        )
        .route(
            "/admin/v1/channels/discord/onboarding/probe",
            post(admin::channels::connectors::discord::admin_discord_onboarding_probe_handler),
        )
        .route(
            "/admin/v1/channels/discord/onboarding/apply",
            post(admin::channels::connectors::discord::admin_discord_onboarding_apply_handler),
        )
        .route(
            "/admin/v1/channels/discord/accounts/logout",
            post(admin::channels::connectors::discord::admin_discord_account_logout_action_handler),
        )
        .route(
            "/admin/v1/channels/discord/accounts/remove",
            post(admin::channels::connectors::discord::admin_discord_account_remove_action_handler),
        )
        .route(
            "/admin/v1/channels/discord/accounts/{account_id}/logout",
            post(admin::channels::connectors::discord::admin_discord_account_logout_handler),
        )
        .route(
            "/admin/v1/channels/discord/accounts/{account_id}/remove",
            post(admin::channels::connectors::discord::admin_discord_account_remove_handler),
        )
        .route(
            "/admin/v1/skills/{skill_id}/quarantine",
            post(admin::skills::admin_skill_quarantine_handler),
        )
        .route(
            "/admin/v1/skills/{skill_id}/enable",
            post(admin::skills::admin_skill_enable_handler),
        )
        .layer(DefaultBodyLimit::max(HTTP_MAX_REQUEST_BODY_BYTES))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            http_middleware::admin_rate_limit_middleware,
        ))
        .route_layer(middleware::from_fn(
            http_middleware::admin_console_security_headers_middleware,
        ));
    let console_routes = Router::new()
        .route("/console/v1/auth/login", post(console::auth::console_login_handler))
        .route("/console/v1/auth/logout", post(console::auth::console_logout_handler))
        .route("/console/v1/auth/session", get(console::auth::console_session_handler))
        .route(
            "/console/v1/auth/browser-handoff",
            post(console::auth::console_browser_handoff_handler),
        )
        .route(
            "/console/v1/auth/browser-handoff/consume",
            get(console::auth::console_browser_bootstrap_handler),
        )
        .route(
            "/console/v1/auth/browser-handoff/session",
            post(console::auth::console_browser_session_bootstrap_handler),
        )
        .route(
            "/console/v1/control-plane/capabilities",
            get(console::auth::console_capability_catalog_handler),
        )
        .route(
            "/console/v1/deployment/posture",
            get(console::auth::console_deployment_posture_handler),
        )
        .route("/console/v1/agents", get(console::agents::console_agents_list_handler))
        .route("/console/v1/agents", post(console::agents::console_agent_create_handler))
        .route("/console/v1/agents/{agent_id}", get(console::agents::console_agent_get_handler))
        .route(
            "/console/v1/agents/{agent_id}/set-default",
            post(console::agents::console_agent_set_default_handler),
        )
        .route("/console/v1/auth/profiles", get(console::auth::console_auth_profiles_list_handler))
        .route(
            "/console/v1/auth/profiles",
            post(console::auth::console_auth_profile_upsert_handler),
        )
        .route(
            "/console/v1/auth/profiles/{profile_id}",
            get(console::auth::console_auth_profile_get_handler),
        )
        .route(
            "/console/v1/auth/profiles/{profile_id}/delete",
            post(console::auth::console_auth_profile_delete_handler),
        )
        .route("/console/v1/auth/health", get(console::auth::console_auth_health_handler))
        .route(
            "/console/v1/auth/providers/openai",
            get(console::auth::console_openai_provider_state_handler),
        )
        .route(
            "/console/v1/auth/providers/openai/api-key",
            post(console::auth::console_openai_provider_api_key_handler),
        )
        .route(
            "/console/v1/auth/providers/openai/bootstrap",
            post(console::auth::console_openai_provider_bootstrap_handler),
        )
        .route(
            "/console/v1/auth/providers/openai/callback-state",
            get(console::auth::console_openai_provider_callback_state_handler),
        )
        .route(
            "/console/v1/auth/providers/openai/callback",
            get(console::auth::console_openai_provider_callback_handler),
        )
        .route(
            "/console/v1/auth/providers/openai/reconnect",
            post(console::auth::console_openai_provider_reconnect_handler),
        )
        .route(
            "/console/v1/auth/providers/openai/refresh",
            post(console::auth::console_openai_provider_refresh_handler),
        )
        .route(
            "/console/v1/auth/providers/openai/revoke",
            post(console::auth::console_openai_provider_revoke_handler),
        )
        .route(
            "/console/v1/auth/providers/openai/default-profile",
            post(console::auth::console_openai_provider_default_profile_handler),
        )
        .route("/console/v1/config/inspect", post(console::config::console_config_inspect_handler))
        .route(
            "/console/v1/config/validate",
            post(console::config::console_config_validate_handler),
        )
        .route("/console/v1/config/mutate", post(console::config::console_config_mutate_handler))
        .route("/console/v1/config/migrate", post(console::config::console_config_migrate_handler))
        .route("/console/v1/config/recover", post(console::config::console_config_recover_handler))
        .route("/console/v1/secrets", get(console::secrets::console_secrets_list_handler))
        .route("/console/v1/secrets", post(console::secrets::console_secret_set_handler))
        .route(
            "/console/v1/secrets/metadata",
            get(console::secrets::console_secret_metadata_handler),
        )
        .route("/console/v1/secrets/reveal", post(console::secrets::console_secret_reveal_handler))
        .route("/console/v1/secrets/delete", post(console::secrets::console_secret_delete_handler))
        .route("/console/v1/webhooks", get(console::webhooks::console_webhooks_list_handler))
        .route("/console/v1/webhooks", post(console::webhooks::console_webhook_upsert_handler))
        .route(
            "/console/v1/webhooks/{integration_id}",
            get(console::webhooks::console_webhook_get_handler),
        )
        .route(
            "/console/v1/webhooks/{integration_id}/enabled",
            post(console::webhooks::console_webhook_set_enabled_handler),
        )
        .route(
            "/console/v1/webhooks/{integration_id}/delete",
            post(console::webhooks::console_webhook_delete_handler),
        )
        .route(
            "/console/v1/webhooks/{integration_id}/test",
            post(console::webhooks::console_webhook_test_handler),
        )
        .route("/console/v1/pairing", get(console::pairing::console_pairing_summary_handler))
        .route(
            "/console/v1/pairing/codes",
            post(console::pairing::console_pairing_code_mint_handler),
        )
        .route(
            "/console/v1/support-bundle/jobs",
            get(console::support_bundle::console_support_bundle_jobs_list_handler),
        )
        .route(
            "/console/v1/support-bundle/jobs",
            post(console::support_bundle::console_support_bundle_job_create_handler),
        )
        .route(
            "/console/v1/support-bundle/jobs/{job_id}",
            get(console::support_bundle::console_support_bundle_job_get_handler),
        )
        .route("/console/v1/diagnostics", get(console::diagnostics::console_diagnostics_handler))
        .route("/console/v1/chat/sessions", get(console::chat::console_chat_sessions_list_handler))
        .route(
            "/console/v1/chat/sessions",
            post(console::chat::console_chat_session_resolve_handler),
        )
        .route(
            "/console/v1/chat/sessions/{session_id}/rename",
            post(console::chat::console_chat_session_rename_handler),
        )
        .route(
            "/console/v1/chat/sessions/{session_id}/reset",
            post(console::chat::console_chat_session_reset_handler),
        )
        .route(
            "/console/v1/chat/sessions/{session_id}/messages/stream",
            post(console::chat::console_chat_message_stream_handler),
        )
        .route(
            "/console/v1/chat/runs/{run_id}/events",
            get(console::chat::console_chat_run_events_handler),
        )
        .route(
            "/console/v1/chat/runs/{run_id}/status",
            get(console::chat::console_chat_run_status_handler),
        )
        .route("/console/v1/approvals", get(console::approvals::console_approvals_list_handler))
        .route(
            "/console/v1/approvals/{approval_id}",
            get(console::approvals::console_approval_get_handler),
        )
        .route(
            "/console/v1/approvals/{approval_id}/decision",
            post(console::approvals::console_approval_decision_handler),
        )
        .route("/console/v1/cron/jobs", get(console::cron::console_cron_list_handler))
        .route("/console/v1/cron/jobs", post(console::cron::console_cron_create_handler))
        .route(
            "/console/v1/cron/jobs/{job_id}/enabled",
            post(console::cron::console_cron_set_enabled_handler),
        )
        .route(
            "/console/v1/cron/jobs/{job_id}/run-now",
            post(console::cron::console_cron_run_now_handler),
        )
        .route("/console/v1/cron/jobs/{job_id}/runs", get(console::cron::console_cron_runs_handler))
        .route("/console/v1/memory/status", get(console::memory::console_memory_status_handler))
        .route("/console/v1/memory/index", post(console::memory::console_memory_index_handler))
        .route("/console/v1/memory/search", get(console::memory::console_memory_search_handler))
        .route("/console/v1/memory/purge", post(console::memory::console_memory_purge_handler))
        .route("/console/v1/channels", get(console::channels::console_channels_list_handler))
        .route(
            "/console/v1/channels/{connector_id}",
            get(console::channels::console_channel_status_handler),
        )
        .route(
            "/console/v1/channels/{connector_id}/enabled",
            post(console::channels::console_channel_set_enabled_handler),
        )
        .route(
            "/console/v1/channels/{connector_id}/logs",
            get(console::channels::console_channel_logs_handler),
        )
        .route(
            "/console/v1/channels/{connector_id}/operations/health-refresh",
            post(console::channels::console_channel_health_refresh_handler),
        )
        .route(
            "/console/v1/channels/{connector_id}/operations/queue/pause",
            post(console::channels::console_channel_queue_pause_handler),
        )
        .route(
            "/console/v1/channels/{connector_id}/operations/queue/resume",
            post(console::channels::console_channel_queue_resume_handler),
        )
        .route(
            "/console/v1/channels/{connector_id}/operations/queue/drain",
            post(console::channels::console_channel_queue_drain_handler),
        )
        .route(
            "/console/v1/channels/{connector_id}/operations/dead-letters/{dead_letter_id}/replay",
            post(console::channels::console_channel_dead_letter_replay_handler),
        )
        .route(
            "/console/v1/channels/{connector_id}/operations/dead-letters/{dead_letter_id}/discard",
            post(console::channels::console_channel_dead_letter_discard_handler),
        )
        .route(
            "/console/v1/channels/{connector_id}/test",
            post(console::channels::console_channel_test_handler),
        )
        .route(
            "/console/v1/channels/{connector_id}/test-send",
            post(console::channels::console_channel_test_send_handler),
        )
        .route(
            "/console/v1/channels/router/rules",
            get(console::channels::console_channel_router_rules_handler),
        )
        .route(
            "/console/v1/channels/router/warnings",
            get(console::channels::console_channel_router_warnings_handler),
        )
        .route(
            "/console/v1/channels/router/preview",
            post(console::channels::console_channel_router_preview_handler),
        )
        .route(
            "/console/v1/channels/router/pairings",
            get(console::channels::console_channel_router_pairings_handler),
        )
        .route(
            "/console/v1/channels/router/pairing-codes",
            post(console::channels::console_channel_router_pairing_code_mint_handler),
        )
        .route(
            "/console/v1/channels/discord/onboarding/probe",
            post(console::channels::connectors::discord::console_discord_onboarding_probe_handler),
        )
        .route(
            "/console/v1/channels/discord/onboarding/apply",
            post(console::channels::connectors::discord::console_discord_onboarding_apply_handler),
        )
        .route(
            "/console/v1/channels/discord/accounts/{account_id}/logout",
            post(console::channels::connectors::discord::console_discord_account_logout_handler),
        )
        .route(
            "/console/v1/channels/discord/accounts/{account_id}/remove",
            post(console::channels::connectors::discord::console_discord_account_remove_handler),
        )
        .route("/console/v1/skills", get(console::skills::console_skills_list_handler))
        .route("/console/v1/skills/install", post(console::skills::console_skills_install_handler))
        .route(
            "/console/v1/skills/{skill_id}/verify",
            post(console::skills::console_skills_verify_handler),
        )
        .route(
            "/console/v1/skills/{skill_id}/audit",
            post(console::skills::console_skills_audit_handler),
        )
        .route(
            "/console/v1/skills/{skill_id}/quarantine",
            post(console::skills::console_skill_quarantine_handler),
        )
        .route(
            "/console/v1/skills/{skill_id}/enable",
            post(console::skills::console_skill_enable_handler),
        )
        .route(
            "/console/v1/browser/profiles",
            get(console::browser::console_browser_profiles_list_handler),
        )
        .route(
            "/console/v1/browser/profiles/create",
            post(console::browser::console_browser_profile_create_handler),
        )
        .route(
            "/console/v1/browser/profiles/{profile_id}/rename",
            post(console::browser::console_browser_profile_rename_handler),
        )
        .route(
            "/console/v1/browser/profiles/{profile_id}/delete",
            post(console::browser::console_browser_profile_delete_handler),
        )
        .route(
            "/console/v1/browser/profiles/{profile_id}/activate",
            post(console::browser::console_browser_profile_activate_handler),
        )
        .route(
            "/console/v1/browser/sessions",
            post(console::browser::console_browser_session_create_handler),
        )
        .route(
            "/console/v1/browser/sessions/{session_id}/close",
            post(console::browser::console_browser_session_close_handler),
        )
        .route(
            "/console/v1/browser/sessions/{session_id}/navigate",
            post(console::browser::console_browser_navigate_handler),
        )
        .route(
            "/console/v1/browser/sessions/{session_id}/click",
            post(console::browser::console_browser_click_handler),
        )
        .route(
            "/console/v1/browser/sessions/{session_id}/type",
            post(console::browser::console_browser_type_handler),
        )
        .route(
            "/console/v1/browser/sessions/{session_id}/scroll",
            post(console::browser::console_browser_scroll_handler),
        )
        .route(
            "/console/v1/browser/sessions/{session_id}/wait-for",
            post(console::browser::console_browser_wait_for_handler),
        )
        .route(
            "/console/v1/browser/sessions/{session_id}/title",
            get(console::browser::console_browser_title_handler),
        )
        .route(
            "/console/v1/browser/sessions/{session_id}/screenshot",
            get(console::browser::console_browser_screenshot_handler),
        )
        .route(
            "/console/v1/browser/sessions/{session_id}/observe",
            get(console::browser::console_browser_observe_handler),
        )
        .route(
            "/console/v1/browser/sessions/{session_id}/network-log",
            get(console::browser::console_browser_network_log_handler),
        )
        .route(
            "/console/v1/browser/sessions/{session_id}/reset-state",
            post(console::browser::console_browser_reset_state_handler),
        )
        .route(
            "/console/v1/browser/sessions/{session_id}/tabs",
            get(console::browser::console_browser_tabs_list_handler),
        )
        .route(
            "/console/v1/browser/sessions/{session_id}/tabs/open",
            post(console::browser::console_browser_tab_open_handler),
        )
        .route(
            "/console/v1/browser/sessions/{session_id}/tabs/switch",
            post(console::browser::console_browser_tab_switch_handler),
        )
        .route(
            "/console/v1/browser/sessions/{session_id}/tabs/close",
            post(console::browser::console_browser_tab_close_handler),
        )
        .route(
            "/console/v1/browser/sessions/{session_id}/permissions",
            get(console::browser::console_browser_permissions_get_handler)
                .post(console::browser::console_browser_permissions_set_handler),
        )
        .route(
            "/console/v1/browser/downloads",
            get(console::browser::console_browser_downloads_list_handler),
        )
        .route(
            "/console/v1/browser/relay/tokens",
            post(console::browser::console_browser_relay_token_handler),
        )
        .route(
            "/console/v1/browser/relay/actions",
            post(console::browser::console_browser_relay_action_handler),
        )
        .route(
            "/console/v1/system/heartbeat",
            get(console::system::console_system_heartbeat_handler),
        )
        .route("/console/v1/system/presence", get(console::system::console_system_presence_handler))
        .route(
            "/console/v1/system/events",
            get(console::system::console_system_events_list_handler),
        )
        .route(
            "/console/v1/system/events/emit",
            post(console::system::console_system_event_emit_handler),
        )
        .route("/console/v1/audit/events", get(console::audit::console_audit_events_handler))
        .layer(DefaultBodyLimit::max(HTTP_MAX_REQUEST_BODY_BYTES))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            http_middleware::admin_rate_limit_middleware,
        ))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            http_middleware::console_observability_middleware,
        ))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            http_middleware::console_session_cookie_refresh_middleware,
        ))
        .route_layer(middleware::from_fn(
            http_middleware::admin_console_security_headers_middleware,
        ));
    let canvas_routes = Router::new()
        .route("/canvas/v1/frame/{canvas_id}", get(canvas::canvas_frame_handler))
        .route("/canvas/v1/runtime.js", get(canvas::canvas_runtime_js_handler))
        .route("/canvas/v1/runtime.css", get(canvas::canvas_runtime_css_handler))
        .route(
            "/canvas/v1/bundle/{canvas_id}/{*asset_path}",
            get(canvas::canvas_bundle_asset_handler),
        )
        .route("/canvas/v1/state/{canvas_id}", get(canvas::canvas_state_handler))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            http_middleware::canvas_rate_limit_middleware,
        ))
        .route_layer(middleware::from_fn(http_middleware::canvas_security_headers_middleware));
    Router::new()
        .route("/runtime", get(health::dashboard_handoff_handler))
        .route("/healthz", get(health::health_handler))
        .merge(canvas_routes)
        .merge(admin_routes)
        .merge(console_routes)
        .fallback(get(web_ui::web_ui_entry_handler))
        .with_state(state)
}
