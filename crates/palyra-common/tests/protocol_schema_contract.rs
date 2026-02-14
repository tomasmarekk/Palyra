use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};

fn collect_files(root: &Path, extension: &str) -> Result<Vec<PathBuf>> {
    let mut stack = vec![root.to_path_buf()];
    let mut files = Vec::new();

    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir)
            .with_context(|| format!("failed to read directory {}", dir.display()))?
        {
            let entry =
                entry.with_context(|| format!("failed to read entry in {}", dir.display()))?;
            let path = entry.path();
            let metadata = entry
                .metadata()
                .with_context(|| format!("failed to read metadata for {}", path.display()))?;
            if metadata.is_dir() {
                stack.push(path);
            } else if path.extension().and_then(|ext| ext.to_str()) == Some(extension) {
                files.push(path);
            }
        }
    }

    files.sort();
    Ok(files)
}

#[test]
fn proto_schemas_are_versioned_and_forward_compatible() -> Result<()> {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .canonicalize()
        .context("failed to resolve repository root")?;
    let proto_dir = repo_root.join("schemas").join("proto");
    let proto_files = collect_files(&proto_dir, "proto")?;

    assert!(!proto_files.is_empty(), "expected .proto files in {}", proto_dir.display());

    for file in proto_files {
        let content = fs::read_to_string(&file)
            .with_context(|| format!("failed to read {}", file.display()))?;
        assert!(
            content.contains("package palyra.") && content.contains(".v1;"),
            "proto file must define versioned package: {}",
            file.display()
        );
        assert!(
            content.contains("reserved "),
            "proto file must reserve fields for compatibility: {}",
            file.display()
        );
    }

    let common_proto =
        repo_root.join("schemas").join("proto").join("palyra").join("v1").join("common.proto");
    let common_content = fs::read_to_string(&common_proto)
        .with_context(|| format!("failed to read {}", common_proto.display()))?;
    assert!(common_content.contains("message RunStreamRequest"));
    assert!(common_content.contains("message RunStreamEvent"));
    Ok(())
}

#[test]
fn json_envelope_schemas_require_version_and_limits() -> Result<()> {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .canonicalize()
        .context("failed to resolve repository root")?;
    let envelopes_dir = repo_root.join("schemas").join("json").join("envelopes");
    let envelope_files = collect_files(&envelopes_dir, "json")?;

    assert!(!envelope_files.is_empty(), "expected envelope schemas in {}", envelopes_dir.display());

    for file in envelope_files {
        let content = fs::read_to_string(&file)
            .with_context(|| format!("failed to read {}", file.display()))?;
        assert!(
            content.contains("\"v\""),
            "envelope schema must contain version field: {}",
            file.display()
        );
        assert!(
            content.contains("\"const\": 1"),
            "envelope schema must pin major version: {}",
            file.display()
        );
        assert!(
            content.contains("\"max_payload_bytes\""),
            "envelope schema must define hard payload cap: {}",
            file.display()
        );
    }
    Ok(())
}
