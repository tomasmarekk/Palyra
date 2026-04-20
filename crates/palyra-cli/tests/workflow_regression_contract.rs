use anyhow::Result;
use palyra_cli::workflow_regression::{
    load_compat_release_readiness, load_workflow_regression_manifest, repo_root_from_manifest_dir,
    validate_compat_release_readiness, validate_workflow_regression_manifest,
};

#[test]
fn workflow_regression_manifest_and_compat_checklist_remain_consistent() -> Result<()> {
    let repo_root = repo_root_from_manifest_dir()?;
    let manifest_path =
        repo_root.join("infra").join("release").join("workflow-regression-matrix.json");
    let checklist_path =
        repo_root.join("infra").join("release").join("compat-hardening-readiness.json");

    let manifest = load_workflow_regression_manifest(manifest_path.as_path())?;
    validate_workflow_regression_manifest(&manifest)?;

    let checklist = load_compat_release_readiness(checklist_path.as_path())?;
    validate_compat_release_readiness(&checklist, &manifest, repo_root.as_path())?;

    assert_eq!(checklist.matrix_manifest, "infra/release/workflow-regression-matrix.json");
    assert!(manifest.profiles.contains_key("fast"));
    assert!(manifest.profiles.contains_key("full"));
    Ok(())
}
