use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use serde_json::Value;

use crate::{transport::urlencoding, *};

#[test]
fn secret_reveal_decodes_base64() {
    let envelope = SecretRevealEnvelope {
        contract: ContractDescriptor {
            contract_version: CONTROL_PLANE_CONTRACT_VERSION.to_owned(),
        },
        scope: "global".to_owned(),
        key: "openai_api_key".to_owned(),
        value_bytes: 3,
        value_base64: BASE64_STANDARD.encode(b"abc"),
        value_utf8: Some("abc".to_owned()),
    };
    assert_eq!(envelope.decode_value().as_deref(), Some(b"abc".as_slice()));
}

#[test]
fn urlencoding_escapes_reserved_bytes() {
    assert_eq!(urlencoding("global/openai key"), "global%2Fopenai%20key");
}

#[test]
fn capability_entry_defaults_optional_dashboard_exposure_when_missing() {
    let entry = serde_json::from_value::<CapabilityEntry>(serde_json::json!({
        "id": "runtime.health",
        "domain": "runtime",
        "dashboard_section": "operations",
        "title": "Runtime health",
        "owner": "palyrad",
        "surfaces": ["dashboard"],
        "execution_mode": "direct_ui",
        "cli_handoff_commands": [],
        "mutation_classes": ["deployment"],
        "test_refs": [],
        "contract_paths": ["/console/v1/diagnostics"]
    }))
    .expect("capability entry should deserialize without dashboard_exposure");

    assert_eq!(entry.dashboard_exposure, None);
}

#[test]
fn capability_entry_serializes_dashboard_exposure_when_present() {
    let entry = CapabilityEntry {
        id: "gateway.access.verify_remote".to_owned(),
        domain: "deployment".to_owned(),
        dashboard_section: "access".to_owned(),
        title: "Remote dashboard URL verification".to_owned(),
        owner: "palyra-cli".to_owned(),
        surfaces: vec!["cli".to_owned(), "dashboard".to_owned()],
        execution_mode: "generated_cli".to_owned(),
        dashboard_exposure: Some(CapabilityDashboardExposure::CliHandoff),
        cli_handoff_commands: vec![
            "cargo run -p palyra-cli -- daemon dashboard-url --verify-remote --json".to_owned(),
        ],
        mutation_classes: vec!["deployment".to_owned()],
        test_refs: Vec::new(),
        contract_paths: Vec::new(),
        notes: None,
    };

    let serialized = serde_json::to_value(entry).expect("capability entry should serialize");
    assert_eq!(serialized.get("dashboard_exposure").and_then(Value::as_str), Some("cli_handoff"));
}

#[test]
fn browser_screenshot_envelope_decodes_base64_image() {
    let envelope = BrowserScreenshotEnvelope {
        contract: ContractDescriptor {
            contract_version: CONTROL_PLANE_CONTRACT_VERSION.to_owned(),
        },
        session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
        success: true,
        mime_type: Some("image/png".to_owned()),
        image_base64: Some(BASE64_STANDARD.encode(b"png-bytes")),
        error: String::new(),
    };

    assert_eq!(envelope.decode_image().as_deref(), Some(b"png-bytes".as_slice()));
}

#[test]
fn browser_permission_setting_serializes_as_snake_case() {
    let value = serde_json::to_value(BrowserPermissionSetting::Allow)
        .expect("permission setting should serialize");
    assert_eq!(value, serde_json::json!("allow"));
}

#[test]
fn mobile_bootstrap_envelope_round_trips() {
    let envelope = MobileBootstrapEnvelope {
        contract: ContractDescriptor {
            contract_version: CONTROL_PLANE_CONTRACT_VERSION.to_owned(),
        },
        release_scope: MobileReleaseScope {
            approvals_inbox: true,
            polling_notifications: true,
            recent_sessions: true,
            safe_url_open: true,
            voice_note: true,
        },
        notifications: MobileNotificationPolicy {
            delivery_mode: "polling".to_owned(),
            quiet_hours_supported: true,
            grouping_supported: true,
            priority_supported: true,
            default_poll_interval_ms: 45_000,
            max_alerts_per_poll: 24,
        },
        pairing: MobilePairingPolicy {
            auth_flow: "login".to_owned(),
            trust_model: "shared".to_owned(),
            revoke_supported: true,
            recovery_supported: true,
            offline_state_visible: true,
        },
        handoff: MobileHandoffPolicy {
            contract: "cross_surface_handoff.v1".to_owned(),
            safe_url_open_requires_mediation: true,
            heavy_surface_handoff_supported: true,
            browser_automation_exposed: false,
        },
        store: MobileLocalStoreContract {
            approvals_cache_key: "a".to_owned(),
            sessions_cache_key: "s".to_owned(),
            inbox_cache_key: "i".to_owned(),
            outbox_queue_key: "o".to_owned(),
            revoke_marker_key: "r".to_owned(),
        },
        rollout: MobileRolloutStatus {
            mobile_companion_enabled: true,
            approvals_enabled: true,
            notifications_enabled: true,
            recent_sessions_enabled: true,
            safe_url_open_enabled: true,
            voice_notes_enabled: true,
        },
        locales: vec!["en".to_owned(), "qps-ploc".to_owned()],
        default_locale: "en".to_owned(),
    };

    let decoded: MobileBootstrapEnvelope =
        serde_json::from_value(serde_json::to_value(envelope).expect("bootstrap should serialize"))
            .expect("bootstrap should deserialize");

    assert_eq!(decoded.default_locale, "en");
    assert!(decoded.release_scope.voice_note);
}

#[test]
fn openai_reconnect_response_shape_is_oauth_bootstrap() {
    let value = serde_json::json!({
        "contract": {
            "contract_version": CONTROL_PLANE_CONTRACT_VERSION
        },
        "provider": "openai",
        "attempt_id": "attempt-1",
        "authorization_url": "https://auth.openai.example/authorize",
        "expires_at_unix_ms": 1_772_000_000_000i64,
        "profile_id": "openai-default",
        "message": "Open the authorization URL to reconnect OpenAI."
    });

    let decoded = serde_json::from_value::<OpenAiOAuthBootstrapEnvelope>(value.clone())
        .expect("reconnect success response should decode as OAuth bootstrap");

    assert_eq!(decoded.attempt_id, "attempt-1");
    assert_eq!(decoded.profile_id.as_deref(), Some("openai-default"));
    assert!(
        serde_json::from_value::<ProviderAuthActionEnvelope>(value).is_err(),
        "reconnect responses intentionally omit provider action state fields"
    );
}
