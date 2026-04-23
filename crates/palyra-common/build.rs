use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

fn main() {
    println!("cargo:rerun-if-env-changed=PALYRA_GIT_HASH");

    if let Some(git_hash) = env::var("PALYRA_GIT_HASH").ok().and_then(normalize_git_hash) {
        println!("cargo:rustc-env=PALYRA_GIT_HASH={git_hash}");
        return;
    }

    let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap_or_default());
    let repo_root = find_repo_root(manifest_dir.as_path());
    if let Some(repo_root) = repo_root.as_deref() {
        emit_git_rerun_paths(repo_root);
    }

    let git_hash =
        repo_root.as_deref().and_then(resolve_git_hash).unwrap_or_else(|| "unknown".to_owned());
    println!("cargo:rustc-env=PALYRA_GIT_HASH={git_hash}");
}

fn normalize_git_hash(value: String) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_owned())
}

fn find_repo_root(start: &Path) -> Option<PathBuf> {
    for ancestor in start.ancestors() {
        if ancestor.join(".git").exists() {
            return Some(ancestor.to_path_buf());
        }
    }
    None
}

fn resolve_git_hash(repo_root: &Path) -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "--short=12", "HEAD"])
        .current_dir(repo_root)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    normalize_git_hash(String::from_utf8_lossy(output.stdout.as_slice()).into_owned())
}

fn emit_git_rerun_paths(repo_root: &Path) {
    let git_path = repo_root.join(".git");
    if git_path.is_dir() {
        emit_git_dir_rerun_paths(git_path.as_path());
        return;
    }

    let Ok(metadata) = fs::read_to_string(git_path.as_path()) else {
        return;
    };
    let Some(raw_git_dir) = metadata.trim().strip_prefix("gitdir:").map(str::trim) else {
        return;
    };
    let git_dir = {
        let path = PathBuf::from(raw_git_dir);
        if path.is_absolute() {
            path
        } else {
            repo_root.join(path)
        }
    };
    emit_git_dir_rerun_paths(git_dir.as_path());
}

fn emit_git_dir_rerun_paths(git_dir: &Path) {
    let head_path = git_dir.join("HEAD");
    println!("cargo:rerun-if-changed={}", head_path.display());

    let Ok(head) = fs::read_to_string(head_path.as_path()) else {
        return;
    };
    let Some(reference) = head.trim().strip_prefix("ref:").map(str::trim) else {
        return;
    };
    println!("cargo:rerun-if-changed={}", git_dir.join(reference).display());
}
