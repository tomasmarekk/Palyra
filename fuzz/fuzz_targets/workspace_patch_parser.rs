//! Fuzzes workspace patch parsing and dry-run application inside a temp root.

#![no_main]

use std::{fs, path::PathBuf, sync::OnceLock};

use libfuzzer_sys::fuzz_target;
use palyra_common::workspace_patch::{
    apply_workspace_patch, WorkspacePatchLimits, WorkspacePatchRedactionPolicy,
    WorkspacePatchRequest,
};

const MAX_FUZZ_PATCH_BYTES: usize = 16 * 1024;

fn fuzz_workspace_root() -> &'static PathBuf {
    static ROOT: OnceLock<PathBuf> = OnceLock::new();
    ROOT.get_or_init(|| {
        let root = std::env::temp_dir().join("palyra-fuzz-workspace-patch");
        let _ = fs::create_dir_all(root.as_path());
        root
    })
}

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_FUZZ_PATCH_BYTES {
        return;
    }
    let Ok(patch) = std::str::from_utf8(data) else {
        return;
    };

    let request = WorkspacePatchRequest {
        patch: patch.to_owned(),
        dry_run: true,
        redaction_policy: WorkspacePatchRedactionPolicy::default(),
    };
    let limits = WorkspacePatchLimits {
        max_patch_bytes: MAX_FUZZ_PATCH_BYTES,
        max_files_touched: 32,
        max_file_bytes: 256 * 1024,
        max_preview_bytes: 8 * 1024,
    };
    let _ = apply_workspace_patch(&[fuzz_workspace_root().clone()], &request, &limits);
});
