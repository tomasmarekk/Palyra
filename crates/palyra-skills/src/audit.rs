use serde_json::{json, Value};
use wasmtime::{Engine, ExternType, Module};

use crate::artifact::now_unix_ms;
use crate::error::SkillPackagingError;
use crate::models::{
    SkillAuditCheckStatus, SkillAuditSeverity, SkillSecurityAuditCheck, SkillSecurityAuditPolicy,
    SkillSecurityAuditReport, SkillTrustStore,
};
use crate::verify::{inspect_skill_artifact, verify_skill_artifact};

pub fn audit_skill_artifact_security(
    artifact_bytes: &[u8],
    trust_store: &mut SkillTrustStore,
    allow_tofu: bool,
    policy: &SkillSecurityAuditPolicy,
) -> Result<SkillSecurityAuditReport, SkillPackagingError> {
    let verification = verify_skill_artifact(artifact_bytes, trust_store, allow_tofu)?;
    let inspected = inspect_skill_artifact(artifact_bytes)?;
    let mut checks = Vec::new();
    let mut quarantine_reasons = Vec::new();

    checks.push(pass_audit_check(
        "signature_validity",
        "signature chain and payload hash verified",
        None,
    ));
    checks.push(pass_audit_check("sbom_presence", "sbom.cdx.json is present and valid", None));
    checks.push(pass_audit_check(
        "provenance_presence",
        "provenance.json is present and valid",
        None,
    ));
    checks.push(pass_audit_check(
        "manifest_sanity",
        "manifest passed strict schema and wildcard validation",
        None,
    ));
    for warning in &inspected.manifest_warnings {
        checks.push(SkillSecurityAuditCheck {
            check_id: format!("manifest_warning:{}", warning.code),
            status: SkillAuditCheckStatus::Warn,
            severity: SkillAuditSeverity::Warning,
            message: warning.message.clone(),
            details: Some(json!({
                "code": warning.code,
                "severity": warning.severity,
            })),
        });
    }

    let module_paths = inspected
        .entries
        .keys()
        .filter(|path| path.starts_with("modules/") && path.ends_with(".wasm"))
        .cloned()
        .collect::<Vec<_>>();
    if module_paths.is_empty() {
        push_fail_check(
            &mut checks,
            &mut quarantine_reasons,
            "wasm_module_presence",
            "artifact does not contain any modules/*.wasm entries",
            None,
        );
    } else {
        checks.push(pass_audit_check(
            "wasm_module_presence",
            format!("artifact includes {} wasm module(s)", module_paths.len()),
            Some(json!({ "modules": &module_paths })),
        ));
    }

    let filesystem_declared = !inspected.manifest.capabilities.filesystem.read_roots.is_empty()
        || !inspected.manifest.capabilities.filesystem.write_roots.is_empty();
    let engine = Engine::default();
    for module_path in module_paths {
        let Some(module_bytes) = inspected.entries.get(module_path.as_str()) else {
            continue;
        };
        if module_bytes.len() as u64 > policy.max_module_bytes {
            push_fail_check(
                &mut checks,
                &mut quarantine_reasons,
                "wasm_module_size_limit",
                format!(
                    "module '{}' exceeds max_module_bytes ({} > {})",
                    module_path,
                    module_bytes.len(),
                    policy.max_module_bytes
                ),
                Some(json!({
                    "module_path": module_path,
                    "module_bytes": module_bytes.len(),
                    "max_module_bytes": policy.max_module_bytes,
                })),
            );
            continue;
        }

        let module = match Module::new(&engine, module_bytes) {
            Ok(module) => module,
            Err(error) => {
                push_fail_check(
                    &mut checks,
                    &mut quarantine_reasons,
                    "wasm_module_validation",
                    format!("module '{}' failed validation: {error}", module_path),
                    Some(json!({ "module_path": module_path })),
                );
                continue;
            }
        };

        checks.push(pass_audit_check(
            "wasm_module_size_limit",
            format!(
                "module '{}' is within max_module_bytes ({} <= {})",
                module_path,
                module_bytes.len(),
                policy.max_module_bytes
            ),
            Some(json!({
                "module_path": module_path,
                "module_bytes": module_bytes.len(),
                "max_module_bytes": policy.max_module_bytes,
            })),
        ));

        let exported_functions =
            module.exports().filter(|export| matches!(export.ty(), ExternType::Func(_))).count();
        if exported_functions > policy.max_exported_functions {
            push_fail_check(
                &mut checks,
                &mut quarantine_reasons,
                "wasm_exported_function_limit",
                format!(
                    "module '{}' exports too many functions ({} > {})",
                    module_path, exported_functions, policy.max_exported_functions
                ),
                Some(json!({
                    "module_path": module_path,
                    "exported_functions": exported_functions,
                    "max_exported_functions": policy.max_exported_functions,
                })),
            );
        } else {
            checks.push(pass_audit_check(
                "wasm_exported_function_limit",
                format!(
                    "module '{}' export function count is within limit ({} <= {})",
                    module_path, exported_functions, policy.max_exported_functions
                ),
                Some(json!({
                    "module_path": module_path,
                    "exported_functions": exported_functions,
                    "max_exported_functions": policy.max_exported_functions,
                })),
            ));
        }

        if module_imports_wasi_filesystem(&module) && !filesystem_declared {
            push_fail_check(
                &mut checks,
                &mut quarantine_reasons,
                "wasm_wasi_filesystem_imports",
                format!(
                    "module '{}' imports wasi:filesystem without declared filesystem capability",
                    module_path
                ),
                Some(json!({
                    "module_path": module_path,
                    "filesystem_declared": filesystem_declared,
                })),
            );
        } else {
            checks.push(pass_audit_check(
                "wasm_wasi_filesystem_imports",
                format!("module '{}' does not violate wasi:filesystem policy", module_path),
                Some(json!({
                    "module_path": module_path,
                    "filesystem_declared": filesystem_declared,
                })),
            ));
        }
    }

    let vulnerability_scan = match std::env::var("PALYRA_SKILL_AUDIT_VULN_FEED_HOOK") {
        Ok(value) if !value.trim().is_empty() => pass_audit_check(
            "vulnerability_feed_hook",
            "vulnerability feed hook is configured",
            Some(json!({ "hook": value.trim() })),
        ),
        _ => SkillSecurityAuditCheck {
            check_id: "vulnerability_feed_hook".to_owned(),
            status: SkillAuditCheckStatus::Warn,
            severity: SkillAuditSeverity::Warning,
            message:
                "vulnerability feed hook is not configured (set PALYRA_SKILL_AUDIT_VULN_FEED_HOOK)"
                    .to_owned(),
            details: None,
        },
    };
    checks.push(vulnerability_scan.clone());

    let passed = !checks.iter().any(|check| check.status == SkillAuditCheckStatus::Fail);
    let manifest = verification.manifest;
    Ok(SkillSecurityAuditReport {
        skill_id: manifest.skill_id,
        version: manifest.version,
        publisher: manifest.publisher,
        accepted: verification.accepted,
        passed,
        should_quarantine: !passed,
        trust_decision: verification.trust_decision,
        payload_sha256: verification.payload_sha256,
        generated_at_unix_ms: now_unix_ms(),
        policy: policy.clone(),
        manifest_warnings: inspected.manifest_warnings,
        checks,
        quarantine_reasons,
        vulnerability_scan,
    })
}

fn module_imports_wasi_filesystem(module: &Module) -> bool {
    module.imports().any(|import| {
        let namespace = import.module().trim().to_ascii_lowercase();
        namespace.starts_with("wasi:filesystem")
    })
}

fn pass_audit_check(
    check_id: impl Into<String>,
    message: impl Into<String>,
    details: Option<Value>,
) -> SkillSecurityAuditCheck {
    SkillSecurityAuditCheck {
        check_id: check_id.into(),
        status: SkillAuditCheckStatus::Pass,
        severity: SkillAuditSeverity::Info,
        message: message.into(),
        details,
    }
}

fn push_fail_check(
    checks: &mut Vec<SkillSecurityAuditCheck>,
    quarantine_reasons: &mut Vec<String>,
    check_id: impl Into<String>,
    message: impl Into<String>,
    details: Option<Value>,
) {
    let message = message.into();
    checks.push(SkillSecurityAuditCheck {
        check_id: check_id.into(),
        status: SkillAuditCheckStatus::Fail,
        severity: SkillAuditSeverity::Error,
        message: message.clone(),
        details,
    });
    quarantine_reasons.push(message);
}
