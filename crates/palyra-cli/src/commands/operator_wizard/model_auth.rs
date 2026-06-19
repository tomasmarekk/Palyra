use crate::commands::wizard::StepChoice;

pub(crate) const DEFAULT_MINIMAX_BASE_URL: &str = "https://api.minimax.io/anthropic";
pub(crate) const DEFAULT_MINIMAX_CN_BASE_URL: &str = "https://api.minimaxi.com/anthropic";
pub(crate) const MINIMAX_AUTH_PROVIDER_KIND: &str = "minimax";
pub(crate) const XAI_AUTH_PROVIDER_KIND: &str = "xai";
pub(crate) const GOOGLE_GEMINI_AUTH_PROVIDER_KIND: &str = "google_gemini";
pub(crate) const GOOGLE_GEMINI_CLI_AUTH_PROVIDER_KIND: &str = "google_gemini_cli";
pub(crate) const OPENROUTER_AUTH_PROVIDER_KIND: &str = "openrouter";
const XAI_BASE_URL: &str = "https://api.x.ai/v1";
const GOOGLE_GEMINI_OPENAI_BASE_URL: &str =
    "https://generativelanguage.googleapis.com/v1beta/openai";
const OPENROUTER_BASE_URL: &str = "https://openrouter.ai/api/v1";
pub(crate) const DEFAULT_XAI_TEXT_MODEL: &str = "grok-4.3";
const DEFAULT_GOOGLE_GEMINI_TEXT_MODEL: &str = "gemini-3.5-flash";
const DEFAULT_OPENROUTER_TEXT_MODEL: &str = "~openai/gpt-latest";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AuthMethodFlow {
    ApiKey,
    DeferredAuthProfile,
    ExistingConfig,
    Skip,
}

#[derive(Debug, Clone, Copy)]
struct AuthMethodDefinition {
    id: &'static str,
    label: &'static str,
    hint: &'static str,
    flow: AuthMethodFlow,
    api_key_label: &'static str,
    api_key_prompt: &'static str,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RegistryProviderDefaults {
    pub(crate) auth_method: &'static str,
    pub(crate) provider_id: &'static str,
    pub(crate) display_name: &'static str,
    pub(crate) auth_provider_kind: &'static str,
    pub(crate) base_url: &'static str,
    pub(crate) chat_model: &'static str,
    pub(crate) secret_key: &'static str,
}

const AUTH_METHOD_DEFINITIONS: &[AuthMethodDefinition] = &[
    AuthMethodDefinition {
        id: "chatgpt_login",
        label: "ChatGPT Login",
        hint: "Sign in with your ChatGPT or Codex subscription",
        flow: AuthMethodFlow::DeferredAuthProfile,
        api_key_label: "OpenAI API Key",
        api_key_prompt: "Enter the OpenAI API key that should be stored in the local vault.",
    },
    AuthMethodDefinition {
        id: "api_key",
        label: "OpenAI API Key",
        hint: "Use your OpenAI API key directly",
        flow: AuthMethodFlow::ApiKey,
        api_key_label: "OpenAI API Key",
        api_key_prompt: "Enter the OpenAI API key that should be stored in the local vault.",
    },
    AuthMethodDefinition {
        id: "anthropic_api_key",
        label: "Anthropic API key",
        hint: "Use your Anthropic API key directly",
        flow: AuthMethodFlow::ApiKey,
        api_key_label: "Anthropic API Key",
        api_key_prompt: "Enter the Anthropic API key that should be stored in the local vault.",
    },
    AuthMethodDefinition {
        id: "anthropic_oauth",
        label: "Anthropic OAuth",
        hint: "Use an Anthropic OAuth auth profile",
        flow: AuthMethodFlow::DeferredAuthProfile,
        api_key_label: "Anthropic API Key",
        api_key_prompt: "Enter the Anthropic API key that should be stored in the local vault.",
    },
    AuthMethodDefinition {
        id: "minimax_api_key_global",
        label: "MiniMax API key (Global)",
        hint: "Global endpoint - api.minimax.io",
        flow: AuthMethodFlow::ApiKey,
        api_key_label: "MiniMax API Key",
        api_key_prompt: "Enter the MiniMax API key that should be stored in the local vault.",
    },
    AuthMethodDefinition {
        id: "minimax_api_key_cn",
        label: "MiniMax API key (CN)",
        hint: "CN endpoint - api.minimaxi.com",
        flow: AuthMethodFlow::ApiKey,
        api_key_label: "MiniMax API Key",
        api_key_prompt: "Enter the MiniMax API key that should be stored in the local vault.",
    },
    AuthMethodDefinition {
        id: "minimax_oauth_global",
        label: "MiniMax OAuth (Global)",
        hint: "Global endpoint - api.minimax.io",
        flow: AuthMethodFlow::DeferredAuthProfile,
        api_key_label: "MiniMax API Key",
        api_key_prompt: "Enter the MiniMax API key that should be stored in the local vault.",
    },
    AuthMethodDefinition {
        id: "minimax_oauth_cn",
        label: "MiniMax OAuth (CN)",
        hint: "CN endpoint - api.minimaxi.com",
        flow: AuthMethodFlow::DeferredAuthProfile,
        api_key_label: "MiniMax API Key",
        api_key_prompt: "Enter the MiniMax API key that should be stored in the local vault.",
    },
    AuthMethodDefinition {
        id: "xai_api_key",
        label: "xAI API key",
        hint: "Use your xAI Grok API key directly",
        flow: AuthMethodFlow::ApiKey,
        api_key_label: "xAI API Key",
        api_key_prompt: "Enter the xAI API key that should be stored in the local vault.",
    },
    AuthMethodDefinition {
        id: "xai_device_code",
        label: "xAI device code",
        hint: "Use an xAI device-code auth profile",
        flow: AuthMethodFlow::DeferredAuthProfile,
        api_key_label: "xAI API Key",
        api_key_prompt: "Enter the xAI API key that should be stored in the local vault.",
    },
    AuthMethodDefinition {
        id: "xai_oauth",
        label: "xAI OAuth",
        hint: "Use an xAI OAuth auth profile",
        flow: AuthMethodFlow::DeferredAuthProfile,
        api_key_label: "xAI API Key",
        api_key_prompt: "Enter the xAI API key that should be stored in the local vault.",
    },
    AuthMethodDefinition {
        id: "gemini_cli_oauth",
        label: "Gemini CLI OAuth",
        hint: "Google OAuth with project-aware token payload",
        flow: AuthMethodFlow::DeferredAuthProfile,
        api_key_label: "Google Gemini API Key",
        api_key_prompt: "Enter the Google Gemini API key that should be stored in the local vault.",
    },
    AuthMethodDefinition {
        id: "google_gemini_api_key",
        label: "Google Gemini API key",
        hint: "Use your Google Gemini API key directly",
        flow: AuthMethodFlow::ApiKey,
        api_key_label: "Google Gemini API Key",
        api_key_prompt: "Enter the Google Gemini API key that should be stored in the local vault.",
    },
    AuthMethodDefinition {
        id: "openrouter_api_key",
        label: "OpenRouter API key",
        hint: "Use your OpenRouter API key directly",
        flow: AuthMethodFlow::ApiKey,
        api_key_label: "OpenRouter API Key",
        api_key_prompt: "Enter the OpenRouter API key that should be stored in the local vault.",
    },
    AuthMethodDefinition {
        id: "openrouter_oauth",
        label: "OpenRouter OAuth",
        hint: "Use an OpenRouter OAuth auth profile",
        flow: AuthMethodFlow::DeferredAuthProfile,
        api_key_label: "OpenRouter API Key",
        api_key_prompt: "Enter the OpenRouter API key that should be stored in the local vault.",
    },
    AuthMethodDefinition {
        id: "existing_config",
        label: "Reuse Current",
        hint: "keep the existing credential source if one is already configured",
        flow: AuthMethodFlow::ExistingConfig,
        api_key_label: "Model Provider API Key",
        api_key_prompt:
            "Enter the model-provider API key that should be stored in the local vault.",
    },
    AuthMethodDefinition {
        id: "skip",
        label: "Skip for Now",
        hint: "leave model auth unset and continue with warnings",
        flow: AuthMethodFlow::Skip,
        api_key_label: "Model Provider API Key",
        api_key_prompt:
            "Enter the model-provider API key that should be stored in the local vault.",
    },
];

const REGISTRY_PROVIDER_DEFAULTS: &[RegistryProviderDefaults] = &[
    RegistryProviderDefaults {
        auth_method: "xai_api_key",
        provider_id: "xai-primary",
        display_name: "xAI (Grok)",
        auth_provider_kind: XAI_AUTH_PROVIDER_KIND,
        base_url: XAI_BASE_URL,
        chat_model: DEFAULT_XAI_TEXT_MODEL,
        secret_key: "xai_api_key",
    },
    RegistryProviderDefaults {
        auth_method: "google_gemini_api_key",
        provider_id: "google-gemini-primary",
        display_name: "Google Gemini",
        auth_provider_kind: GOOGLE_GEMINI_AUTH_PROVIDER_KIND,
        base_url: GOOGLE_GEMINI_OPENAI_BASE_URL,
        chat_model: DEFAULT_GOOGLE_GEMINI_TEXT_MODEL,
        secret_key: "google_gemini_api_key",
    },
    RegistryProviderDefaults {
        auth_method: "openrouter_api_key",
        provider_id: "openrouter-primary",
        display_name: "OpenRouter",
        auth_provider_kind: OPENROUTER_AUTH_PROVIDER_KIND,
        base_url: OPENROUTER_BASE_URL,
        chat_model: DEFAULT_OPENROUTER_TEXT_MODEL,
        secret_key: "openrouter_api_key",
    },
];

pub(crate) fn model_provider_auth_choices() -> Vec<StepChoice> {
    AUTH_METHOD_DEFINITIONS
        .iter()
        .map(|definition| StepChoice {
            value: definition.id.to_owned(),
            label: definition.label.to_owned(),
            hint: Some(definition.hint.to_owned()),
        })
        .collect()
}

pub(crate) fn auth_method_flow(auth_method: &str) -> Option<AuthMethodFlow> {
    if auth_method == "minimax_api_key" {
        return Some(AuthMethodFlow::ApiKey);
    }
    auth_method_definition(auth_method).map(|definition| definition.flow)
}

pub(crate) fn auth_method_label(auth_method: &str) -> &'static str {
    if auth_method == "minimax_api_key" {
        return "MiniMax API key (Global)";
    }
    auth_method_definition(auth_method).map_or("Model provider auth", |definition| definition.label)
}

pub(crate) fn registry_provider_defaults_for_auth_method(
    auth_method: &str,
) -> Option<&'static RegistryProviderDefaults> {
    REGISTRY_PROVIDER_DEFAULTS.iter().find(|defaults| defaults.auth_method == auth_method)
}

pub(crate) fn auth_method_requires_api_key(auth_method: &str) -> bool {
    auth_method_flow(auth_method) == Some(AuthMethodFlow::ApiKey)
}

pub(crate) fn api_key_field_label(auth_method: &str) -> &'static str {
    if auth_method == "minimax_api_key" {
        return "MiniMax API Key";
    }
    auth_method_definition(auth_method)
        .map_or("Model Provider API Key", |definition| definition.api_key_label)
}

pub(crate) fn api_key_prompt_message(auth_method: &str) -> &'static str {
    if auth_method == "minimax_api_key" {
        return "Enter the MiniMax API key that should be stored in the local vault.";
    }
    auth_method_definition(auth_method).map_or(
        "Enter the model-provider API key that should be stored in the local vault.",
        |definition| definition.api_key_prompt,
    )
}

pub(crate) fn provider_display_name(
    provider_kind: &str,
    auth_provider_kind: Option<&str>,
) -> &'static str {
    if provider_kind == "anthropic"
        && auth_provider_kind.is_some_and(|kind| kind.eq_ignore_ascii_case("minimax"))
    {
        return "MiniMax";
    }
    if provider_kind == "openai_compatible" {
        if let Some(auth_provider_kind) = auth_provider_kind {
            if auth_provider_kind.eq_ignore_ascii_case(XAI_AUTH_PROVIDER_KIND) {
                return "xAI (Grok)";
            }
            if auth_provider_kind.eq_ignore_ascii_case(GOOGLE_GEMINI_AUTH_PROVIDER_KIND)
                || auth_provider_kind.eq_ignore_ascii_case(GOOGLE_GEMINI_CLI_AUTH_PROVIDER_KIND)
            {
                return "Google Gemini";
            }
            if auth_provider_kind.eq_ignore_ascii_case(OPENROUTER_AUTH_PROVIDER_KIND) {
                return "OpenRouter";
            }
        }
    }
    match provider_kind {
        "openai_compatible" => "OpenAI-compatible",
        "anthropic" => "Anthropic",
        "deterministic" => "Deterministic",
        "unset" => "unset",
        _ => "Unknown",
    }
}

fn auth_method_definition(auth_method: &str) -> Option<&'static AuthMethodDefinition> {
    AUTH_METHOD_DEFINITIONS.iter().find(|definition| definition.id == auth_method)
}
