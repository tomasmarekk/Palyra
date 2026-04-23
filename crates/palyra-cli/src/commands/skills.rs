use crate::{client::skills as skills_client, output::skills as skills_output, *};

pub(crate) fn run_skills(command: SkillsCommand) -> Result<()> {
    match command {
        SkillsCommand::Package { command } => match command {
            SkillsPackageCommand::Build {
                manifest,
                module,
                asset,
                sbom,
                provenance,
                output,
                signing_key_vault_ref,
                signing_key_stdin,
                json,
            } => {
                if module.is_empty() {
                    anyhow::bail!("skills package build requires at least one --module");
                }
                let manifest_toml = fs::read_to_string(manifest.as_str()).with_context(|| {
                    format!("failed to read skills manifest {}", Path::new(&manifest).display())
                })?;
                let modules = module
                    .iter()
                    .map(|path| {
                        let bytes = fs::read(path).with_context(|| {
                            format!("failed to read module {}", Path::new(path).display())
                        })?;
                        let entry_path = skill_entry_path_from_cli(path)?;
                        Ok(ArtifactFile { path: entry_path, bytes })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let assets = asset
                    .iter()
                    .map(|path| {
                        let bytes = fs::read(path).with_context(|| {
                            format!("failed to read asset {}", Path::new(path).display())
                        })?;
                        let entry_path = skill_entry_path_from_cli(path)?;
                        Ok(ArtifactFile { path: entry_path, bytes })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let sbom_payload = fs::read(sbom.as_str()).with_context(|| {
                    format!("failed to read SBOM {}", Path::new(&sbom).display())
                })?;
                let provenance_payload = fs::read(provenance.as_str()).with_context(|| {
                    format!(
                        "failed to read provenance payload {}",
                        Path::new(&provenance).display()
                    )
                })?;
                let signing_key_secret = read_skills_signing_key_source(
                    signing_key_vault_ref.as_deref(),
                    signing_key_stdin,
                )?;
                let signing_key = parse_ed25519_signing_key(signing_key_secret.as_slice())
                    .context("invalid signing key bytes (expected raw 32-byte, hex, or base64)")?;

                let build_output = build_signed_skill_artifact(SkillArtifactBuildRequest {
                    manifest_toml,
                    modules,
                    assets,
                    sbom_cyclonedx_json: sbom_payload,
                    provenance_json: provenance_payload,
                    signing_key,
                })
                .context("failed to build signed skill artifact")?;

                let output_path = Path::new(&output);
                if let Some(parent) = output_path.parent() {
                    fs::create_dir_all(parent).with_context(|| {
                        format!("failed to create output directory {}", parent.to_string_lossy())
                    })?;
                }
                fs::write(output_path, build_output.artifact_bytes.as_slice()).with_context(
                    || format!("failed to write skill artifact {}", output_path.display()),
                )?;

                if json {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&json!({
                            "artifact_path": output_path,
                            "payload_sha256": build_output.payload_sha256,
                            "publisher": build_output.manifest.publisher,
                            "skill_id": build_output.manifest.skill_id,
                            "version": build_output.manifest.version,
                            "signature_key_id": build_output.signature.key_id,
                            "artifact_bytes": build_output.artifact_bytes.len(),
                        }))?
                    );
                } else {
                    println!(
                        "skills.package.build artifact={} skill_id={} publisher={} version={} payload_sha256={} key_id={} bytes={}",
                        output_path.display(),
                        build_output.manifest.skill_id,
                        build_output.manifest.publisher,
                        build_output.manifest.version,
                        build_output.payload_sha256,
                        build_output.signature.key_id,
                        build_output.artifact_bytes.len(),
                    );
                }
                std::io::stdout().flush().context("stdout flush failed")
            }
            SkillsPackageCommand::Verify {
                artifact,
                trust_store,
                trusted_publishers,
                allow_tofu,
                json,
            } => {
                let artifact_path = Path::new(artifact.as_str());
                let artifact_bytes = fs::read(artifact_path).with_context(|| {
                    format!("failed to read skill artifact {}", artifact_path.display())
                })?;
                let trust_store_path = resolve_skills_trust_store_path(trust_store.as_deref())
                    .with_context(|| "failed to resolve skills trust store path".to_owned())?;
                let mut store = load_trust_store_with_integrity(trust_store_path.as_path())?;
                for trusted in trusted_publishers {
                    let (publisher, key) = parse_trusted_publisher_arg(trusted.as_str())?;
                    store.add_trusted_key(publisher, key)?;
                }
                let report =
                    verify_skill_artifact(artifact_bytes.as_slice(), &mut store, allow_tofu)
                        .context("failed to verify skill artifact")?;
                save_trust_store_with_integrity(trust_store_path.as_path(), &store)?;

                if json {
                    println!("{}", serde_json::to_string_pretty(&report)?);
                } else {
                    println!(
                        "skills.package.verify artifact={} accepted={} trust={} skill_id={} publisher={} version={} payload_sha256={} trust_store={}",
                        artifact_path.display(),
                        report.accepted,
                        match report.trust_decision {
                            palyra_skills::TrustDecision::Allowlisted => "allowlisted",
                            palyra_skills::TrustDecision::TofuPinned => "tofu_pinned",
                            palyra_skills::TrustDecision::TofuNewlyPinned => "tofu_newly_pinned",
                        },
                        report.manifest.skill_id,
                        report.manifest.publisher,
                        report.manifest.version,
                        report.payload_sha256,
                        trust_store_path.display()
                    );
                }
                std::io::stdout().flush().context("stdout flush failed")
            }
        },
        SkillsCommand::Install {
            artifact,
            registry_dir,
            registry_url,
            skill_id,
            version,
            registry_ca_cert,
            skills_dir,
            trust_store,
            trusted_publishers,
            allow_untrusted,
            non_interactive,
            json,
        } => run_skills_install(SkillsInstallCommand {
            artifact,
            registry_dir,
            registry_url,
            skill_id,
            version,
            registry_ca_cert,
            skills_dir,
            trust_store,
            trusted_publishers,
            allow_untrusted,
            non_interactive,
            json,
        }),
        SkillsCommand::Remove { skill_id, version, skills_dir, json } => {
            run_skills_remove(skill_id, version, skills_dir, json)
        }
        SkillsCommand::List {
            skills_dir,
            publisher,
            current_only,
            quarantined_only,
            eligible_only,
            json,
        } => run_skills_list(
            skills_dir,
            publisher,
            current_only,
            quarantined_only,
            eligible_only,
            json,
        ),
        SkillsCommand::Info { skill_id, version, skills_dir, json } => {
            run_skills_info(skill_id, version, skills_dir, json)
        }
        SkillsCommand::Check {
            skill_id,
            version,
            skills_dir,
            trust_store,
            trusted_publishers,
            allow_untrusted,
            json,
        } => run_skills_check(
            skill_id,
            version,
            skills_dir,
            trust_store,
            trusted_publishers,
            allow_untrusted,
            json,
        ),
        SkillsCommand::Update {
            registry_dir,
            registry_url,
            skill_id,
            version,
            registry_ca_cert,
            skills_dir,
            trust_store,
            trusted_publishers,
            allow_untrusted,
            non_interactive,
            json,
        } => run_skills_update(SkillsUpdateCommand {
            registry_dir,
            registry_url,
            skill_id,
            version,
            registry_ca_cert,
            skills_dir,
            trust_store,
            trusted_publishers,
            allow_untrusted,
            non_interactive,
            json,
        }),
        SkillsCommand::Verify {
            skill_id,
            version,
            skills_dir,
            trust_store,
            trusted_publishers,
            allow_untrusted,
            json,
        } => run_skills_verify(
            skill_id,
            version,
            skills_dir,
            trust_store,
            trusted_publishers,
            allow_untrusted,
            json,
        ),
        SkillsCommand::Audit {
            skill_id,
            version,
            artifact,
            skills_dir,
            trust_store,
            trusted_publishers,
            allow_untrusted,
            json,
        } => run_skills_audit(SkillsAuditCommand {
            skill_id,
            version,
            artifact,
            skills_dir,
            trust_store,
            trusted_publishers,
            allow_untrusted,
            json,
        }),
        SkillsCommand::Quarantine {
            skill_id,
            version,
            skills_dir,
            reason,
            url,
            token,
            principal,
            device_id,
            channel,
            json,
        } => run_skills_quarantine(SkillsQuarantineCommand {
            skill_id,
            version,
            skills_dir,
            reason,
            url,
            token,
            principal,
            device_id,
            channel,
            json,
        }),
        SkillsCommand::Enable {
            skill_id,
            version,
            skills_dir,
            override_enabled,
            reason,
            url,
            token,
            principal,
            device_id,
            channel,
            json,
        } => run_skills_enable(SkillsEnableCommand {
            skill_id,
            version,
            skills_dir,
            override_enabled,
            reason,
            url,
            token,
            principal,
            device_id,
            channel,
            json,
        }),
    }
}

fn run_skills_install(command: SkillsInstallCommand) -> Result<()> {
    let skills_root = resolve_skills_root(command.skills_dir.as_deref())?;
    fs::create_dir_all(skills_root.as_path()).with_context(|| {
        format!("failed to create managed skills directory {}", skills_root.display())
    })?;

    let trust_store_path = resolve_skills_trust_store_path(command.trust_store.as_deref())?;
    let mut trust_store = load_trust_store_with_integrity(trust_store_path.as_path())?;
    let trusted_publishers = command.trusted_publishers.clone();
    for trusted in &trusted_publishers {
        let (publisher, key) = parse_trusted_publisher_arg(trusted.as_str())?;
        trust_store.add_trusted_key(publisher, key)?;
    }

    let resolved = resolve_install_artifact(&command, &mut trust_store, command.allow_untrusted)?;
    let artifact_sha256 = sha256_hex(resolved.artifact_bytes.as_slice());
    if artifact_sha256 != resolved.entry.artifact_sha256 {
        anyhow::bail!(
            "registry hash mismatch for {} {}: expected {} got {}",
            resolved.entry.skill_id,
            resolved.entry.version,
            resolved.entry.artifact_sha256,
            artifact_sha256
        );
    }
    let inspected = inspect_skill_artifact(resolved.artifact_bytes.as_slice())
        .context("skill artifact failed structural verification")?;
    if inspected.manifest.skill_id != resolved.entry.skill_id
        || inspected.manifest.version != resolved.entry.version
        || inspected.manifest.publisher != resolved.entry.publisher
    {
        anyhow::bail!(
            "registry metadata mismatch for artifact {}: expected skill_id={} version={} publisher={}, got skill_id={} version={} publisher={}",
            resolved.source.reference,
            resolved.entry.skill_id,
            resolved.entry.version,
            resolved.entry.publisher,
            inspected.manifest.skill_id,
            inspected.manifest.version,
            inspected.manifest.publisher
        );
    }
    let verification_report = verify_skill_artifact(
        resolved.artifact_bytes.as_slice(),
        &mut trust_store,
        command.allow_untrusted,
    )
    .context("failed to verify skill artifact trust policy")?;
    let security_report = audit_skill_artifact_security(
        resolved.artifact_bytes.as_slice(),
        &mut trust_store,
        command.allow_untrusted,
        &SkillSecurityAuditPolicy::default(),
    )
    .context("failed to evaluate skill security audit policy during install")?;
    save_trust_store_with_integrity(trust_store_path.as_path(), &trust_store)?;
    append_skills_audit_event(
        skills_root.as_path(),
        "skill.audit",
        json!({
            "skill_id": verification_report.manifest.skill_id,
            "version": verification_report.manifest.version,
            "publisher": verification_report.manifest.publisher,
            "source": resolved.source.reference,
            "passed": security_report.passed,
            "should_quarantine": security_report.should_quarantine,
            "quarantine_reasons": security_report.quarantine_reasons,
            "checks": security_report.checks,
        }),
    )?;
    if security_report.should_quarantine {
        append_skills_audit_event(
            skills_root.as_path(),
            "skill.quarantined",
            json!({
                "skill_id": verification_report.manifest.skill_id,
                "version": verification_report.manifest.version,
                "publisher": verification_report.manifest.publisher,
                "reason": "static_security_audit_failed",
                "quarantine_reasons": security_report.quarantine_reasons,
            }),
        )?;
        anyhow::bail!(
            "skill security audit requires quarantine for {} {}: {}",
            verification_report.manifest.skill_id,
            verification_report.manifest.version,
            security_report.quarantine_reasons.join(" | ")
        );
    }

    let missing_secrets = resolve_and_prompt_missing_skill_secrets(
        &verification_report.manifest,
        command.non_interactive,
    )?;
    let mut index = load_installed_skills_index(skills_root.as_path())?;
    let outcome = install_verified_skill_artifact(
        skills_root.as_path(),
        &mut index,
        resolved.artifact_bytes.as_slice(),
        &inspected,
        &verification_report,
        InstallMetadataContext {
            source: resolved.source.clone(),
            artifact_sha256,
            missing_secrets,
        },
    )?;
    save_installed_skills_index(skills_root.as_path(), &index)?;

    let event_kind = if outcome.previous_current_version.is_some() {
        "skill.updated"
    } else {
        "skill.installed"
    };
    append_skills_audit_event(
        skills_root.as_path(),
        event_kind,
        json!({
            "skill_id": outcome.record.skill_id,
            "version": outcome.record.version,
            "publisher": outcome.record.publisher,
            "artifact_sha256": outcome.record.artifact_sha256,
            "payload_sha256": outcome.record.payload_sha256,
            "signature_key_id": outcome.record.signature_key_id,
            "trust_decision": outcome.record.trust_decision,
            "source": outcome.record.source,
            "missing_secrets": outcome.record.missing_secrets,
            "previous_version": outcome.previous_current_version,
        }),
    )?;

    if command.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "event_kind": event_kind,
                "skill_id": outcome.record.skill_id,
                "version": outcome.record.version,
                "publisher": outcome.record.publisher,
                "artifact_sha256": outcome.record.artifact_sha256,
                "payload_sha256": outcome.record.payload_sha256,
                "signature_key_id": outcome.record.signature_key_id,
                "trust_decision": outcome.record.trust_decision,
                "source": outcome.record.source,
                "missing_secrets": outcome.record.missing_secrets,
                "skills_root": skills_root,
                "trust_store": trust_store_path,
            }))?
        );
    } else {
        println!(
            "{} skill_id={} version={} publisher={} trust={} source={} skills_root={} trust_store={}",
            event_kind,
            outcome.record.skill_id,
            outcome.record.version,
            outcome.record.publisher,
            outcome.record.trust_decision,
            outcome.record.source.reference,
            skills_root.display(),
            trust_store_path.display()
        );
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn run_skills_update(command: SkillsUpdateCommand) -> Result<()> {
    if command.registry_dir.is_some() == command.registry_url.is_some() {
        anyhow::bail!(
            "skills update requires exactly one source: --registry-dir or --registry-url"
        );
    }
    let skills_root = resolve_skills_root(command.skills_dir.as_deref())?;
    fs::create_dir_all(skills_root.as_path()).with_context(|| {
        format!("failed to create managed skills directory {}", skills_root.display())
    })?;
    let index = load_installed_skills_index(skills_root.as_path())?;
    let current_version = index
        .entries
        .iter()
        .find(|entry| entry.skill_id == command.skill_id && entry.current)
        .map(|entry| entry.version.clone());

    let trust_store_path = resolve_skills_trust_store_path(command.trust_store.as_deref())?;
    let mut trust_store = load_trust_store_with_integrity(trust_store_path.as_path())?;
    let trusted_publishers = command.trusted_publishers.clone();
    for trusted in &trusted_publishers {
        let (publisher, key) = parse_trusted_publisher_arg(trusted.as_str())?;
        trust_store.add_trusted_key(publisher, key)?;
    }
    let resolved = resolve_registry_artifact_for_skill(
        command.registry_dir.as_deref(),
        command.registry_url.as_deref(),
        command.registry_ca_cert.as_deref(),
        command.skill_id.as_str(),
        command.version.as_deref(),
        &mut trust_store,
        command.allow_untrusted,
    )?;
    if current_version.as_deref() == Some(resolved.entry.version.as_str()) {
        save_trust_store_with_integrity(trust_store_path.as_path(), &trust_store)?;
        if command.json {
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "event_kind": "skill.updated",
                    "updated": false,
                    "reason": "already_current",
                    "skill_id": command.skill_id,
                    "version": resolved.entry.version,
                    "skills_root": skills_root,
                }))?
            );
        } else {
            println!(
                "skill.updated updated=false reason=already_current skill_id={} version={} skills_root={}",
                command.skill_id,
                resolved.entry.version,
                skills_root.display()
            );
        }
        return std::io::stdout().flush().context("stdout flush failed");
    }

    save_trust_store_with_integrity(trust_store_path.as_path(), &trust_store)?;

    let install_command = SkillsInstallCommand {
        artifact: None,
        registry_dir: command.registry_dir,
        registry_url: command.registry_url,
        skill_id: Some(command.skill_id),
        version: command.version,
        registry_ca_cert: command.registry_ca_cert,
        skills_dir: Some(skills_root.to_string_lossy().into_owned()),
        trust_store: Some(trust_store_path.to_string_lossy().into_owned()),
        trusted_publishers,
        allow_untrusted: command.allow_untrusted,
        non_interactive: command.non_interactive,
        json: command.json,
    };
    run_skills_install(install_command)
}

fn run_skills_remove(
    skill_id: String,
    version: Option<String>,
    skills_dir: Option<String>,
    json_output: bool,
) -> Result<()> {
    let skills_root = resolve_skills_root(skills_dir.as_deref())?;
    let mut index = load_installed_skills_index(skills_root.as_path())?;
    let target_positions = if let Some(version) = version.as_deref() {
        let selected = index
            .entries
            .iter()
            .enumerate()
            .filter_map(|(position, entry)| {
                (entry.skill_id == skill_id && entry.version == version).then_some(position)
            })
            .collect::<Vec<_>>();
        if selected.is_empty() {
            anyhow::bail!("skill {} version {} is not installed", skill_id, version);
        }
        selected
    } else {
        let Some(current_position) =
            index.entries.iter().position(|entry| entry.skill_id == skill_id && entry.current)
        else {
            anyhow::bail!("skill {} has no current installed version; pass --version", skill_id);
        };
        vec![current_position]
    };

    let mut removed_versions = target_positions
        .iter()
        .map(|position| index.entries[*position].version.clone())
        .collect::<Vec<_>>();
    removed_versions.sort();
    removed_versions.dedup();

    for version in &removed_versions {
        let path = skills_root.join(skill_id.as_str()).join(version);
        if path.exists() {
            fs::remove_dir_all(path.as_path()).with_context(|| {
                format!("failed to remove installed skill directory {}", path.display())
            })?;
        }
    }
    index.entries.retain(|entry| {
        !(entry.skill_id == skill_id
            && removed_versions.iter().any(|version| version == &entry.version))
    });
    normalize_installed_skills_index(&mut index);
    if let Some(current) = index
        .entries
        .iter()
        .find(|entry| entry.skill_id == skill_id && entry.current)
        .map(|entry| entry.version.clone())
    {
        if let Err(error) = update_skill_current_pointer(
            skills_root.join(skill_id.as_str()).as_path(),
            current.as_str(),
        ) {
            eprintln!(
                "warning: failed to update optional '{}' pointer for skill {}: {}",
                SKILLS_CURRENT_LINK_NAME, skill_id, error
            );
        }
    } else if let Err(error) =
        remove_skill_current_pointer(skills_root.join(skill_id.as_str()).as_path())
    {
        eprintln!(
            "warning: failed to remove optional '{}' pointer for skill {}: {}",
            SKILLS_CURRENT_LINK_NAME, skill_id, error
        );
    }
    save_installed_skills_index(skills_root.as_path(), &index)?;
    append_skills_audit_event(
        skills_root.as_path(),
        "skill.removed",
        json!({
            "skill_id": skill_id,
            "removed_versions": removed_versions,
        }),
    )?;

    if json_output {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "event_kind": "skill.removed",
                "skill_id": skill_id,
                "removed_versions": removed_versions,
                "skills_root": skills_root,
            }))?
        );
    } else {
        println!(
            "skill.removed skill_id={} removed_versions={} skills_root={}",
            skill_id,
            removed_versions.join(","),
            skills_root.display()
        );
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn run_skills_list(
    skills_dir: Option<String>,
    publisher: Option<String>,
    current_only: bool,
    quarantined_only: bool,
    eligible_only: bool,
    json_output: bool,
) -> Result<()> {
    let skills_root = resolve_skills_root(skills_dir.as_deref())?;
    let mut entries = collect_installed_skill_inventory(skills_root.as_path())?;
    if let Some(publisher) = publisher.as_deref() {
        let publisher = publisher.trim().to_ascii_lowercase();
        entries.retain(|entry| entry.record.publisher.to_ascii_lowercase() == publisher);
    }
    if current_only {
        entries.retain(|entry| entry.record.current);
    }
    if quarantined_only {
        entries.retain(|entry| entry.runtime_status.status == "quarantined");
    }
    if eligible_only {
        entries.retain(|entry| entry.eligibility.eligible);
    }

    skills_output::emit_inventory_list(
        skills_root.as_path(),
        entries.as_slice(),
        output::preferred_json(json_output),
    )?;
    std::io::stdout().flush().context("stdout flush failed")
}

fn run_skills_info(
    skill_id: String,
    version: Option<String>,
    skills_dir: Option<String>,
    json_output: bool,
) -> Result<()> {
    let skills_root = resolve_skills_root(skills_dir.as_deref())?;
    let mut index = load_installed_skills_index(skills_root.as_path())?;
    normalize_installed_skills_index(&mut index);
    let record_index = find_installed_skill_record(&index, skill_id.as_str(), version.as_deref())?;
    let record = index.entries[record_index].clone();
    let inventory = build_skill_inventory_entry(skills_root.as_path(), &record)?;
    let artifact_path = artifact_path_for_installed_skill(skills_root.as_path(), &record);
    let artifact_bytes = fs::read(artifact_path.as_path()).with_context(|| {
        format!("failed to read installed artifact {}", artifact_path.display())
    })?;
    let inspection = inspect_skill_artifact(artifact_bytes.as_slice())
        .context("failed to inspect installed skill artifact")?;
    let mut artifact_entries = inspection.entries.keys().cloned().collect::<Vec<_>>();
    artifact_entries.sort();
    let info = SkillInfoOutput {
        inventory,
        manifest: inspection.manifest,
        signature: inspection.signature,
        artifact_entries,
        cached_artifact_path: artifact_path.display().to_string(),
    };

    skills_output::emit_inventory_info(&info, json_output)?;
    std::io::stdout().flush().context("stdout flush failed")
}

fn run_skills_check(
    skill_id: Option<String>,
    version: Option<String>,
    skills_dir: Option<String>,
    trust_store: Option<String>,
    trusted_publishers: Vec<String>,
    allow_untrusted: bool,
    json_output: bool,
) -> Result<()> {
    let skills_root = resolve_skills_root(skills_dir.as_deref())?;
    let mut index = load_installed_skills_index(skills_root.as_path())?;
    normalize_installed_skills_index(&mut index);

    let selected_records = if let Some(skill_id) = skill_id.as_deref() {
        let record_index = find_installed_skill_record(&index, skill_id, version.as_deref())?;
        vec![index.entries[record_index].clone()]
    } else {
        let mut records =
            index.entries.iter().filter(|entry| entry.current).cloned().collect::<Vec<_>>();
        if records.is_empty() {
            records = index.entries.clone();
        }
        records
    };

    if selected_records.is_empty() {
        anyhow::bail!("no installed skills matched the requested check scope");
    }

    let trust_store_path = resolve_skills_trust_store_path(trust_store.as_deref())?;
    let mut store = load_trust_store_with_integrity(trust_store_path.as_path())?;
    for trusted in &trusted_publishers {
        let (publisher, key) = parse_trusted_publisher_arg(trusted.as_str())?;
        store.add_trusted_key(publisher, key)?;
    }

    let mut results = Vec::with_capacity(selected_records.len());
    for record in selected_records {
        let inventory = build_skill_inventory_entry(skills_root.as_path(), &record)?;
        let artifact_path = artifact_path_for_installed_skill(skills_root.as_path(), &record);
        let artifact_bytes = fs::read(artifact_path.as_path()).with_context(|| {
            format!("failed to read installed artifact {}", artifact_path.display())
        })?;

        let verification =
            verify_skill_artifact(artifact_bytes.as_slice(), &mut store, allow_untrusted);
        let audit = audit_skill_artifact_security(
            artifact_bytes.as_slice(),
            &mut store,
            allow_untrusted,
            &SkillSecurityAuditPolicy::default(),
        );

        let mut reasons = inventory.eligibility.reasons.clone();
        let (trust_accepted, trust_error, verification_payload) = match verification {
            Ok(report) => (report.accepted, None, Some(report)),
            Err(error) => {
                reasons.push(format!("trust verification failed: {error}"));
                (false, Some(error.to_string()), None)
            }
        };
        let (audit_passed, quarantine_required, failed_checks, warning_checks, audit_payload) =
            match audit {
                Ok(report) => (
                    report.passed,
                    report.should_quarantine,
                    report
                        .checks
                        .iter()
                        .filter(|check| matches!(check.status, SkillAuditCheckStatus::Fail))
                        .count(),
                    report
                        .checks
                        .iter()
                        .filter(|check| matches!(check.status, SkillAuditCheckStatus::Warn))
                        .count(),
                    Some(report),
                ),
                Err(error) => {
                    reasons.push(format!("security audit failed: {error}"));
                    (false, false, 0, 0, None)
                }
            };
        if quarantine_required {
            reasons.push("security audit requires quarantine".to_owned());
        }

        let check_status = if !trust_accepted || !audit_passed || quarantine_required {
            "blocked".to_owned()
        } else if inventory.eligibility.eligible {
            "ready".to_owned()
        } else {
            inventory.eligibility.status.clone()
        };

        results.push(SkillCheckResult {
            inventory,
            check_status,
            trust_accepted,
            trust_error,
            audit_passed,
            quarantine_required,
            failed_checks,
            warning_checks,
            reasons,
            verification: verification_payload,
            audit: audit_payload,
        });
    }

    skills_output::emit_check_results(skills_root.as_path(), results.as_slice(), json_output)?;
    std::io::stdout().flush().context("stdout flush failed")
}

fn run_skills_verify(
    skill_id: String,
    version: Option<String>,
    skills_dir: Option<String>,
    trust_store: Option<String>,
    trusted_publishers: Vec<String>,
    allow_untrusted: bool,
    json_output: bool,
) -> Result<()> {
    let skills_root = resolve_skills_root(skills_dir.as_deref())?;
    let mut index = load_installed_skills_index(skills_root.as_path())?;
    let record_index = find_installed_skill_record(&index, skill_id.as_str(), version.as_deref())?;
    let record = index.entries[record_index].clone();
    let artifact_path = skills_root
        .join(record.skill_id.as_str())
        .join(record.version.as_str())
        .join(SKILLS_ARTIFACT_FILE_NAME);
    let artifact_bytes = fs::read(artifact_path.as_path()).with_context(|| {
        format!("failed to read installed artifact {}", artifact_path.display())
    })?;

    let trust_store_path = resolve_skills_trust_store_path(trust_store.as_deref())?;
    let mut store = load_trust_store_with_integrity(trust_store_path.as_path())?;
    for trusted in trusted_publishers {
        let (publisher, key) = parse_trusted_publisher_arg(trusted.as_str())?;
        store.add_trusted_key(publisher, key)?;
    }
    let report = verify_skill_artifact(artifact_bytes.as_slice(), &mut store, allow_untrusted)
        .context("failed to verify installed skill artifact")?;
    save_trust_store_with_integrity(trust_store_path.as_path(), &store)?;

    index.entries[record_index].trust_decision =
        trust_decision_label(report.trust_decision).to_owned();
    index.entries[record_index].payload_sha256 = report.payload_sha256.clone();
    save_installed_skills_index(skills_root.as_path(), &index)?;
    append_skills_audit_event(
        skills_root.as_path(),
        "skill.verified",
        json!({
            "skill_id": report.manifest.skill_id,
            "version": report.manifest.version,
            "publisher": report.manifest.publisher,
            "payload_sha256": report.payload_sha256,
            "trust_decision": trust_decision_label(report.trust_decision),
            "accepted": report.accepted,
            "policy_bindings": report.policy_bindings,
        }),
    )?;

    if json_output {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!(
            "skill.verified skill_id={} version={} publisher={} accepted={} trust={} payload_sha256={} trust_store={}",
            report.manifest.skill_id,
            report.manifest.version,
            report.manifest.publisher,
            report.accepted,
            trust_decision_label(report.trust_decision),
            report.payload_sha256,
            trust_store_path.display()
        );
    }
    std::io::stdout().flush().context("stdout flush failed")
}

#[derive(Debug, Clone)]
struct SkillAuditTarget {
    artifact_path: PathBuf,
    source: String,
    skill_id: Option<String>,
    version: Option<String>,
}

fn run_skills_audit(command: SkillsAuditCommand) -> Result<()> {
    let trust_store_path = resolve_skills_trust_store_path(command.trust_store.as_deref())?;
    let mut store = load_trust_store_with_integrity(trust_store_path.as_path())?;
    for trusted in &command.trusted_publishers {
        let (publisher, key) = parse_trusted_publisher_arg(trusted.as_str())?;
        store.add_trusted_key(publisher, key)?;
    }

    let mut targets = Vec::new();
    let mut managed_skills_root: Option<PathBuf> = None;
    if let Some(artifact) = command.artifact.as_deref() {
        let artifact_path = PathBuf::from(artifact);
        targets.push(SkillAuditTarget {
            artifact_path,
            source: "artifact".to_owned(),
            skill_id: command.skill_id.clone(),
            version: command.version.clone(),
        });
    } else {
        let skills_root = resolve_skills_root(command.skills_dir.as_deref())?;
        let index = load_installed_skills_index(skills_root.as_path())?;
        managed_skills_root = Some(skills_root.clone());
        if let Some(skill_id) = command.skill_id.as_deref() {
            let record_index =
                find_installed_skill_record(&index, skill_id, command.version.as_deref())?;
            let record = &index.entries[record_index];
            targets.push(SkillAuditTarget {
                artifact_path: skills_root
                    .join(record.skill_id.as_str())
                    .join(record.version.as_str())
                    .join(SKILLS_ARTIFACT_FILE_NAME),
                source: "installed".to_owned(),
                skill_id: Some(record.skill_id.clone()),
                version: Some(record.version.clone()),
            });
        } else {
            let mut records =
                index.entries.iter().filter(|entry| entry.current).collect::<Vec<_>>();
            if records.is_empty() {
                records = index.entries.iter().collect::<Vec<_>>();
            }
            for record in records {
                targets.push(SkillAuditTarget {
                    artifact_path: skills_root
                        .join(record.skill_id.as_str())
                        .join(record.version.as_str())
                        .join(SKILLS_ARTIFACT_FILE_NAME),
                    source: "installed".to_owned(),
                    skill_id: Some(record.skill_id.clone()),
                    version: Some(record.version.clone()),
                });
            }
        }
    }

    if targets.is_empty() {
        anyhow::bail!(
            "no skill artifacts were selected for audit; pass --artifact or install at least one skill first"
        );
    }

    let mut reports = Vec::new();
    for target in &targets {
        let artifact_bytes = fs::read(target.artifact_path.as_path()).with_context(|| {
            format!("failed to read skill artifact for audit {}", target.artifact_path.display())
        })?;
        let report = audit_skill_artifact_security(
            artifact_bytes.as_slice(),
            &mut store,
            command.allow_untrusted,
            &SkillSecurityAuditPolicy::default(),
        )
        .with_context(|| {
            format!("failed to audit skill artifact security {}", target.artifact_path.display())
        })?;
        reports.push((target.clone(), report));
    }
    save_trust_store_with_integrity(trust_store_path.as_path(), &store)?;

    if let Some(skills_root) = managed_skills_root.as_deref() {
        for (target, report) in &reports {
            append_skills_audit_event(
                skills_root,
                "skill.audit",
                json!({
                    "source": target.source,
                    "artifact": target.artifact_path,
                    "skill_id": target.skill_id,
                    "version": target.version,
                    "should_quarantine": report.should_quarantine,
                    "quarantine_reasons": report.quarantine_reasons,
                    "checks": report.checks,
                }),
            )?;
        }
    }

    let output_payload = json!({
        "trust_store": trust_store_path,
        "audits": reports
            .iter()
            .map(|(target, report)| {
                json!({
                    "source": target.source,
                    "artifact": target.artifact_path,
                    "skill_id": target.skill_id,
                    "version": target.version,
                    "report": report,
                })
            })
            .collect::<Vec<_>>(),
    });
    let quarantine_required = reports.iter().any(|(_, report)| report.should_quarantine);

    if command.json {
        println!("{}", serde_json::to_string_pretty(&output_payload)?);
    } else {
        for (target, report) in &reports {
            let skill_label = target
                .skill_id
                .as_deref()
                .map(|value| value.to_owned())
                .unwrap_or_else(|| "unknown".to_owned());
            let version_label = target
                .version
                .as_deref()
                .map(|value| value.to_owned())
                .unwrap_or_else(|| "unknown".to_owned());
            println!(
                "skill.audit skill_id={} version={} source={} artifact={} passed={} should_quarantine={} failed_checks={} warnings={}",
                skill_label,
                version_label,
                target.source,
                target.artifact_path.display(),
                report.passed,
                report.should_quarantine,
                report
                    .checks
                    .iter()
                    .filter(|check| matches!(check.status, SkillAuditCheckStatus::Fail))
                    .count(),
                report
                    .checks
                    .iter()
                    .filter(|check| matches!(check.status, SkillAuditCheckStatus::Warn))
                    .count()
            );
            if report.should_quarantine && !report.quarantine_reasons.is_empty() {
                println!(
                    "skill.audit.quarantine_reasons {}",
                    report.quarantine_reasons.join(" | ")
                );
            }
        }
    }
    std::io::stdout().flush().context("stdout flush failed")?;
    if quarantine_required {
        anyhow::bail!(
            "one or more audited skills require quarantine; inspect report output for details"
        );
    }
    Ok(())
}

fn run_skills_quarantine(command: SkillsQuarantineCommand) -> Result<()> {
    let skills_root = resolve_skills_root(command.skills_dir.as_deref())?;
    let version = resolve_skills_status_version(
        skills_root.as_path(),
        command.skill_id.as_str(),
        command.version.as_deref(),
    )?;
    let response = skills_client::post_skill_status_action(
        command.skill_id.as_str(),
        "quarantine",
        &SkillStatusRequestBody { version, reason: command.reason, override_enabled: None },
        skills_client::SkillsAdminRequestContext {
            url: command.url,
            token: command.token,
            principal: command.principal,
            device_id: command.device_id,
            channel: command.channel,
        },
        "failed to call daemon skills quarantine endpoint",
    )?;

    append_skills_audit_event(
        skills_root.as_path(),
        "skill.quarantined",
        json!({
            "skill_id": response.skill_id,
            "version": response.version,
            "status": response.status,
            "reason": response.reason,
            "detected_at_ms": response.detected_at_ms,
            "operator_principal": response.operator_principal,
        }),
    )?;

    skills_output::emit_status("skill.quarantined", &response, command.json)?;
    std::io::stdout().flush().context("stdout flush failed")
}

fn run_skills_enable(command: SkillsEnableCommand) -> Result<()> {
    if !command.override_enabled {
        anyhow::bail!("skills enable requires --override");
    }
    let skills_root = resolve_skills_root(command.skills_dir.as_deref())?;
    let version = resolve_skills_status_version(
        skills_root.as_path(),
        command.skill_id.as_str(),
        command.version.as_deref(),
    )?;
    let response = skills_client::post_skill_status_action(
        command.skill_id.as_str(),
        "enable",
        &SkillStatusRequestBody { version, reason: command.reason, override_enabled: Some(true) },
        skills_client::SkillsAdminRequestContext {
            url: command.url,
            token: command.token,
            principal: command.principal,
            device_id: command.device_id,
            channel: command.channel,
        },
        "failed to call daemon skills enable endpoint",
    )?;

    append_skills_audit_event(
        skills_root.as_path(),
        "skill.enabled",
        json!({
            "skill_id": response.skill_id,
            "version": response.version,
            "status": response.status,
            "reason": response.reason,
            "detected_at_ms": response.detected_at_ms,
            "operator_principal": response.operator_principal,
        }),
    )?;

    skills_output::emit_status("skill.enabled", &response, command.json)?;
    std::io::stdout().flush().context("stdout flush failed")
}
