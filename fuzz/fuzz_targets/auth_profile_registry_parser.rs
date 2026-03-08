#![no_main]

use std::{fs, path::PathBuf, sync::OnceLock};

use libfuzzer_sys::fuzz_target;

fn fuzz_auth_state_root() -> &'static PathBuf {
    static ROOT: OnceLock<PathBuf> = OnceLock::new();
    ROOT.get_or_init(|| std::env::temp_dir().join("palyra-fuzz-auth-profile-registry"))
}

fuzz_target!(|data: &[u8]| {
    let state_root = fuzz_auth_state_root();
    let identity_root = state_root.join("identity");
    let registry_path = state_root.join("auth_profiles.toml");
    let _ = fs::create_dir_all(identity_root.as_path());
    let _ = fs::write(registry_path.as_path(), data);
    let _ = palyra_auth::AuthProfileRegistry::open(identity_root.as_path());
});
