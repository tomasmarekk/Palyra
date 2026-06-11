//! Extension package preflight (`extension doctor`).
//!
//! Verifies a local skill/extension artifact against the signature trust
//! store and security audit policy, failing the command whenever the
//! package is not ready to install.

use crate::*;

/// Dispatches a `palyra extension` subcommand.
///
/// # Errors
/// Propagates doctor failures, including a blocked (non-ready) package.
pub(crate) fn run_extension(command: ExtensionCommand) -> Result<()> {
    match command {
        ExtensionCommand::Doctor {
            artifact,
            trust_store,
            trusted_publishers,
            allow_tofu,
            grants,
            json,
        } => run_extension_doctor(
            artifact,
            trust_store,
            trusted_publishers,
            allow_tofu,
            grants,
            json,
        ),
    }
}

fn run_extension_doctor(
    artifact: String,
    trust_store: Option<String>,
    trusted_publishers: Vec<String>,
    allow_tofu: bool,
    grants: Vec<String>,
    json_output: bool,
) -> Result<()> {
    let artifact_path = Path::new(artifact.as_str());
    let artifact_bytes = fs::read(artifact_path).with_context(|| {
        format!("failed to read extension artifact {}", artifact_path.display())
    })?;
    let trust_store_path = resolve_skills_trust_store_path(trust_store.as_deref())?;
    let mut store = load_trust_store_with_integrity(trust_store_path.as_path())?;
    for trusted in trusted_publishers {
        let (publisher, key) = parse_trusted_publisher_arg(trusted.as_str())?;
        store.add_trusted_key(publisher, key)?;
    }
    let parsed_grants =
        grants.iter().map(|raw| parse_extension_grant(raw.as_str())).collect::<Result<Vec<_>>>()?;
    let report = palyra_skills::extension_doctor_for_skill_artifact(
        artifact_bytes.as_slice(),
        palyra_skills::ExtensionPackageSource::local_artifact(
            artifact_path.to_string_lossy().into_owned(),
        ),
        &mut store,
        allow_tofu,
        parsed_grants.as_slice(),
        &palyra_skills::SkillSecurityAuditPolicy::default(),
    )
    .context("extension doctor preflight failed")?;

    if json_output {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!(
            "extension.doctor package_id={} kind={:?} status={} reasons={} artifact={} trust_store={}",
            report.package_id,
            report.kind,
            report.status,
            if report.reason_codes.is_empty() {
                "none".to_owned()
            } else {
                report.reason_codes.join(",")
            },
            artifact_path.display(),
            trust_store_path.display(),
        );
        for hint in &report.repair_hints {
            println!("extension.doctor.repair_hint {}", hint);
        }
    }
    std::io::stdout().flush().context("stdout flush failed")?;
    if report.status != "ready" {
        anyhow::bail!("extension doctor blocked package; inspect reason codes and repair hints");
    }
    Ok(())
}

// Parses a --grant argument of the form class=value (or class:value);
// secret grants additionally split an optional scope/key form.
fn parse_extension_grant(raw: &str) -> Result<palyra_skills::ExtensionCapabilityGrant> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        anyhow::bail!("--grant value cannot be empty");
    }
    let (kind, value) =
        trimmed.split_once('=').or_else(|| trimmed.split_once(':')).ok_or_else(|| {
            anyhow!(
                "invalid --grant '{}'; expected class=value, for example network=api.example.com",
                trimmed
            )
        })?;
    let class = match kind.trim().to_ascii_lowercase().as_str() {
        "network" | "http" | "http_egress" => palyra_skills::ExtensionCapabilityClass::Network,
        "secret" | "secrets" => palyra_skills::ExtensionCapabilityClass::Secret,
        "storage_read" | "storage-read" | "read" => {
            palyra_skills::ExtensionCapabilityClass::StorageRead
        }
        "storage" | "storage_write" | "storage-write" | "write" => {
            palyra_skills::ExtensionCapabilityClass::StorageWrite
        }
        "channel" | "channels" => palyra_skills::ExtensionCapabilityClass::Channel,
        "device" | "devices" => palyra_skills::ExtensionCapabilityClass::Device,
        other => anyhow::bail!("unsupported extension grant class '{}'", other),
    };
    let value = value.trim();
    if value.is_empty() {
        anyhow::bail!("--grant '{}' is missing a capability value", trimmed);
    }
    let (scope, value) = if class == palyra_skills::ExtensionCapabilityClass::Secret {
        value
            .split_once('/')
            .map(|(scope, key)| (Some(scope.trim().to_owned()), key.trim().to_owned()))
            .unwrap_or((None, value.to_owned()))
    } else {
        (None, value.to_owned())
    };
    Ok(palyra_skills::ExtensionCapabilityGrant::new(class, value, scope))
}

#[cfg(test)]
mod tests {
    use super::parse_extension_grant;

    #[test]
    fn parse_extension_grant_accepts_secret_scope() {
        let grant = parse_extension_grant("secret=skill:acme.echo_http/api_token")
            .expect("secret grant should parse");
        assert_eq!(grant.scope.as_deref(), Some("skill:acme.echo_http"));
        assert_eq!(grant.value, "api_token");
    }
}
