use crate::*;
use std::path::Path;

pub(crate) fn emit_status(
    event_kind: &str,
    response: &SkillStatusResponse,
    json_output: bool,
) -> Result<()> {
    if json_output {
        return super::print_json_pretty(response, "failed to encode skill status payload as JSON");
    }

    println!(
        "{event_kind} skill_id={} version={} status={} reason={} detected_at_ms={} operator_principal={}",
        response.skill_id,
        response.version,
        response.status,
        response.reason.clone().unwrap_or_else(|| "none".to_owned()),
        response.detected_at_ms,
        response.operator_principal,
    );
    Ok(())
}

pub(crate) fn emit_inventory_list(
    skills_root: &Path,
    entries: &[SkillInventoryEntry],
    json_output: bool,
) -> Result<()> {
    if json_output {
        return super::print_json_pretty(
            &json!({
                "skills_root": skills_root,
                "count": entries.len(),
                "entries": entries,
            }),
            "failed to encode skills inventory as JSON",
        );
    }

    println!("skills.list root={} count={}", skills_root.display(), entries.len());
    for entry in entries {
        println!(
            "skills.entry skill_id={} version={} publisher={} install_state={} runtime_status={} trust={} eligibility={} tool_count={} source={}",
            entry.record.skill_id,
            entry.record.version,
            entry.record.publisher,
            entry.install_state,
            entry.runtime_status.status,
            entry.record.trust_decision,
            entry.eligibility.status,
            entry.tool_count,
            entry.record.source.reference
        );
    }
    Ok(())
}

pub(crate) fn emit_inventory_info(info: &SkillInfoOutput, json_output: bool) -> Result<()> {
    if json_output {
        return super::print_json_pretty(info, "failed to encode skill info as JSON");
    }

    println!(
        "skills.info skill_id={} version={} publisher={} name={} install_state={} runtime_status={} trust={} eligibility={}",
        info.inventory.record.skill_id,
        info.inventory.record.version,
        info.inventory.record.publisher,
        info.inventory.skill_name,
        info.inventory.install_state,
        info.inventory.runtime_status.status,
        info.inventory.record.trust_decision,
        info.inventory.eligibility.status
    );
    println!(
        "skills.info.requirements protocol_major={} min_palyra_version={} tool_count={} cached_artifact={}",
        info.inventory.requirements.required_protocol_major,
        info.inventory.requirements.min_palyra_version,
        info.inventory.tool_count,
        info.cached_artifact_path
    );
    Ok(())
}

pub(crate) fn emit_check_results(
    skills_root: &Path,
    results: &[SkillCheckResult],
    json_output: bool,
) -> Result<()> {
    if json_output {
        return super::print_json_pretty(
            &json!({
                "skills_root": skills_root,
                "count": results.len(),
                "results": results,
            }),
            "failed to encode skill check results as JSON",
        );
    }

    println!("skills.check root={} count={}", skills_root.display(), results.len());
    for result in results {
        println!(
            "skills.check.entry skill_id={} version={} status={} runtime_status={} trust_accepted={} audit_passed={} quarantine_required={} eligibility={}",
            result.inventory.record.skill_id,
            result.inventory.record.version,
            result.check_status,
            result.inventory.runtime_status.status,
            result.trust_accepted,
            result.audit_passed,
            result.quarantine_required,
            result.inventory.eligibility.status
        );
        if !result.reasons.is_empty() {
            println!("skills.check.reasons {}", result.reasons.join(" | "));
        }
    }
    Ok(())
}
