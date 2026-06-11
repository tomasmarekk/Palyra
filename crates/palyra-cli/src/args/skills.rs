//! Arguments for `palyra skills` (alias `skill`): package build/verify,
//! procedure capture, and the install/update/verify/audit/quarantine lifecycle
//! with publisher trust controls. Help text is pinned by snapshot tests; see
//! the doc-comment rules in `mod.rs`.

use clap::Subcommand;

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum SkillsCommand {
    Package {
        #[command(subcommand)]
        command: SkillsPackageCommand,
    },
    Procedure {
        #[command(subcommand)]
        command: SkillsProcedureCommand,
    },
    Install {
        #[arg(long, conflicts_with_all = ["registry_dir", "registry_url", "skill_id", "version"])]
        artifact: Option<String>,
        #[arg(long, conflicts_with = "registry_url")]
        registry_dir: Option<String>,
        #[arg(long, conflicts_with = "registry_dir")]
        registry_url: Option<String>,
        #[arg(long)]
        skill_id: Option<String>,
        #[arg(long, requires = "skill_id")]
        version: Option<String>,
        #[arg(long, requires = "registry_url")]
        registry_ca_cert: Option<String>,
        #[arg(long)]
        skills_dir: Option<String>,
        #[arg(long)]
        trust_store: Option<String>,
        #[arg(long = "trusted-publisher")]
        trusted_publishers: Vec<String>,
        #[arg(long, default_value_t = false)]
        allow_untrusted: bool,
        #[arg(long, default_value_t = false)]
        non_interactive: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Remove {
        skill_id: String,
        #[arg(long)]
        version: Option<String>,
        #[arg(long)]
        skills_dir: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    List {
        #[arg(long)]
        skills_dir: Option<String>,
        #[arg(long)]
        publisher: Option<String>,
        #[arg(long, default_value_t = false)]
        current_only: bool,
        #[arg(long, default_value_t = false)]
        quarantined_only: bool,
        #[arg(long, default_value_t = false)]
        eligible_only: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Info {
        skill_id: String,
        #[arg(long)]
        version: Option<String>,
        #[arg(long)]
        skills_dir: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Check {
        skill_id: Option<String>,
        #[arg(long, requires = "skill_id")]
        version: Option<String>,
        #[arg(long)]
        skills_dir: Option<String>,
        #[arg(long)]
        trust_store: Option<String>,
        #[arg(long = "trusted-publisher")]
        trusted_publishers: Vec<String>,
        #[arg(long, default_value_t = false)]
        allow_untrusted: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Update {
        #[arg(long, conflicts_with = "registry_url")]
        registry_dir: Option<String>,
        #[arg(long, conflicts_with = "registry_dir")]
        registry_url: Option<String>,
        #[arg(long)]
        skill_id: String,
        #[arg(long)]
        version: Option<String>,
        #[arg(long, requires = "registry_url")]
        registry_ca_cert: Option<String>,
        #[arg(long)]
        skills_dir: Option<String>,
        #[arg(long)]
        trust_store: Option<String>,
        #[arg(long = "trusted-publisher")]
        trusted_publishers: Vec<String>,
        #[arg(long, default_value_t = false)]
        allow_untrusted: bool,
        #[arg(long, default_value_t = false)]
        non_interactive: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Verify {
        skill_id: String,
        #[arg(long)]
        version: Option<String>,
        #[arg(long)]
        skills_dir: Option<String>,
        #[arg(long)]
        trust_store: Option<String>,
        #[arg(long = "trusted-publisher")]
        trusted_publishers: Vec<String>,
        #[arg(long, default_value_t = false)]
        allow_untrusted: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Audit {
        skill_id: Option<String>,
        #[arg(long, requires = "skill_id")]
        version: Option<String>,
        #[arg(long, conflicts_with = "skill_id")]
        artifact: Option<String>,
        #[arg(long)]
        skills_dir: Option<String>,
        #[arg(long)]
        trust_store: Option<String>,
        #[arg(long = "trusted-publisher")]
        trusted_publishers: Vec<String>,
        #[arg(long, default_value_t = false)]
        allow_untrusted: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Quarantine {
        skill_id: String,
        #[arg(long)]
        version: Option<String>,
        #[arg(long)]
        skills_dir: Option<String>,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Enable {
        skill_id: String,
        #[arg(long)]
        version: Option<String>,
        #[arg(long)]
        skills_dir: Option<String>,
        #[arg(long = "override", default_value_t = false)]
        override_enabled: bool,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum SkillsProcedureCommand {
    Save {
        #[arg(long)]
        path: Option<String>,
        #[arg(long)]
        skills_dir: Option<String>,
        #[arg(long)]
        slug: Option<String>,
        #[arg(long)]
        name: String,
        #[arg(long)]
        summary: Option<String>,
        #[arg(long, conflicts_with = "body_file")]
        body: Option<String>,
        #[arg(long)]
        body_file: Option<String>,
        #[arg(long, default_value_t = false)]
        force: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum SkillsPackageCommand {
    Build {
        #[arg(long)]
        manifest: String,
        #[arg(long)]
        module: Vec<String>,
        #[arg(long)]
        asset: Vec<String>,
        #[arg(long)]
        sbom: String,
        #[arg(long)]
        provenance: String,
        #[arg(long)]
        output: String,
        #[arg(long)]
        signing_key_vault_ref: Option<String>,
        #[arg(long, default_value_t = false, conflicts_with = "signing_key_vault_ref")]
        signing_key_stdin: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Verify {
        #[arg(long)]
        artifact: String,
        #[arg(long)]
        trust_store: Option<String>,
        #[arg(long = "trusted-publisher")]
        trusted_publishers: Vec<String>,
        #[arg(long, default_value_t = false)]
        allow_tofu: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}
