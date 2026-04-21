use clap::Parser;

use super::*;

#[test]
fn parse_profile_show() {
    let show = Cli::parse_from(["palyra", "profile", "show", "--json"]);
    assert_eq!(
        show.command,
        Command::Profile { command: ProfileCommand::Show { name: None, json: true } }
    );
}

#[test]
fn parse_profile_rename() {
    let rename = Cli::parse_from(["palyra", "profile", "rename", "stage", "staging", "--json"]);
    assert_eq!(
        rename.command,
        Command::Profile {
            command: ProfileCommand::Rename {
                name: "stage".to_owned(),
                new_name: "staging".to_owned(),
                json: true,
            }
        }
    );
}

#[test]
fn parse_profile_delete() {
    let delete = Cli::parse_from([
        "palyra",
        "profile",
        "delete",
        "old-sandbox",
        "--yes",
        "--delete-state-root",
        "--json",
    ]);
    assert_eq!(
        delete.command,
        Command::Profile {
            command: ProfileCommand::Delete {
                name: "old-sandbox".to_owned(),
                yes: true,
                delete_state_root: true,
                json: true,
            }
        }
    );
}

#[test]
fn parse_profile_clone() {
    let cloned = Cli::parse_from([
        "palyra",
        "profile",
        "clone",
        "prod",
        "staging",
        "--label",
        "Staging",
        "--environment",
        "staging",
        "--risk-level",
        "elevated",
        "--strict-mode",
        "--set-default",
        "--force",
        "--json",
    ]);
    assert_eq!(
        cloned.command,
        Command::Profile {
            command: ProfileCommand::Clone {
                name: "prod".to_owned(),
                new_name: "staging".to_owned(),
                label: Some("Staging".to_owned()),
                environment: Some("staging".to_owned()),
                color: None,
                risk_level: Some(ProfileRiskLevelArg::Elevated),
                strict_mode: true,
                set_default: true,
                force: true,
                json: true,
            }
        }
    );
}

#[test]
fn parse_profile_export() {
    let export = Cli::parse_from([
        "palyra",
        "profile",
        "export",
        "prod",
        "--output",
        "artifacts/prod-profile.enc",
        "--mode",
        "encrypted",
        "--password-stdin",
        "--json",
    ]);
    assert_eq!(
        export.command,
        Command::Profile {
            command: ProfileCommand::Export {
                name: Some("prod".to_owned()),
                output: "artifacts/prod-profile.enc".to_owned(),
                mode: ProfileExportModeArg::Encrypted,
                password_stdin: true,
                json: true,
            }
        }
    );
}

#[test]
fn parse_profile_import() {
    let import = Cli::parse_from([
        "palyra",
        "profile",
        "import",
        "--input",
        "artifacts/staging-profile.enc",
        "--name",
        "staging",
        "--password-stdin",
        "--set-default",
        "--force",
        "--json",
    ]);
    assert_eq!(
        import.command,
        Command::Profile {
            command: ProfileCommand::Import {
                input: "artifacts/staging-profile.enc".to_owned(),
                name: Some("staging".to_owned()),
                password_stdin: true,
                set_default: true,
                force: true,
                json: true,
            }
        }
    );
}

fn parse_memory_learning_command<const N: usize>(args: [&str; N]) -> MemoryLearningCommand {
    let parsed = Cli::parse_from(args);
    match parsed.command {
        Command::Memory { command: MemoryCommand::Learning { command } } => command,
        other => panic!("expected memory learning command, got {other:?}"),
    }
}

#[test]
fn parse_memory_learning_list() {
    let list = parse_memory_learning_command([
        "palyra",
        "memory",
        "learning",
        "list",
        "--candidate-kind",
        "procedure",
        "--status",
        "queued",
        "--risk-level",
        "review",
        "--min-confidence",
        "0.85",
        "--max-confidence",
        "0.99",
        "--limit",
        "12",
        "--json",
    ]);
    assert_eq!(
        list,
        MemoryLearningCommand::List {
            candidate_kind: Some("procedure".to_owned()),
            status: Some("queued".to_owned()),
            risk_level: Some("review".to_owned()),
            scope_kind: None,
            scope_id: None,
            session: None,
            min_confidence: Some("0.85".to_owned()),
            max_confidence: Some("0.99".to_owned()),
            limit: Some(12),
            json: true,
        }
    );
}

#[test]
fn parse_memory_learning_review() {
    let review = parse_memory_learning_command([
        "palyra",
        "memory",
        "learning",
        "review",
        "01ARZ3NDEKTSV4RRFFQ69G5FB9",
        "accepted",
        "--summary",
        "promoted",
        "--apply-preference",
    ]);
    assert_eq!(
        review,
        MemoryLearningCommand::Review {
            candidate_id: "01ARZ3NDEKTSV4RRFFQ69G5FB9".to_owned(),
            status: "accepted".to_owned(),
            summary: Some("promoted".to_owned()),
            payload: None,
            apply_preference: true,
            json: false,
        }
    );
}

#[test]
fn parse_memory_learning_apply() {
    let apply = parse_memory_learning_command([
        "palyra",
        "memory",
        "learning",
        "apply",
        "01ARZ3NDEKTSV4RRFFQ69G5FB8",
        "--summary",
        "validated diff and staged it into the workspace",
        "--json",
    ]);
    assert_eq!(
        apply,
        MemoryLearningCommand::Apply {
            candidate_id: "01ARZ3NDEKTSV4RRFFQ69G5FB8".to_owned(),
            summary: Some("validated diff and staged it into the workspace".to_owned()),
            json: true,
        }
    );
}

#[test]
fn parse_memory_learning_promote_procedure() {
    let promote = parse_memory_learning_command([
        "palyra",
        "memory",
        "learning",
        "promote-procedure",
        "01ARZ3NDEKTSV4RRFFQ69G5FBA",
        "--skill-id",
        "palyra.generated.ops.release",
        "--publisher",
        "palyra.generated",
        "--version",
        "0.2.0",
        "--name",
        "Release workflow",
        "--json",
    ]);
    assert_eq!(
        promote,
        MemoryLearningCommand::PromoteProcedure {
            candidate_id: "01ARZ3NDEKTSV4RRFFQ69G5FBA".to_owned(),
            skill_id: Some("palyra.generated.ops.release".to_owned()),
            version: Some("0.2.0".to_owned()),
            publisher: Some("palyra.generated".to_owned()),
            name: Some("Release workflow".to_owned()),
            accept_candidate: true,
            json: true,
        }
    );
}
