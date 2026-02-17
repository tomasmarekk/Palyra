//! Bootstrap SDK placeholder for Palyra plugin authors.
//!
//! The initial WIT contract is defined in `wit/palyra-sdk.wit`.

/// WIT package identifier for the bootstrap plugin SDK contract.
pub const WIT_PACKAGE_ID: &str = "palyra:plugins/sdk@0.1.0";
/// WIT world exported by plugin modules.
pub const WIT_WORLD_NAME: &str = "palyra-plugin";
/// Core Wasm import module that exposes Tier A capability handles.
pub const HOST_CAPABILITIES_IMPORT_MODULE: &str = "palyra:plugins/host-capabilities@0.1.0";

/// Host function names defined by the Tier A capability contract.
pub const HOST_CAPABILITY_HTTP_COUNT_FN: &str = "http-count";
pub const HOST_CAPABILITY_HTTP_HANDLE_FN: &str = "http-handle";
pub const HOST_CAPABILITY_SECRET_COUNT_FN: &str = "secret-count";
pub const HOST_CAPABILITY_SECRET_HANDLE_FN: &str = "secret-handle";
pub const HOST_CAPABILITY_STORAGE_COUNT_FN: &str = "storage-count";
pub const HOST_CAPABILITY_STORAGE_HANDLE_FN: &str = "storage-handle";
pub const HOST_CAPABILITY_CHANNEL_COUNT_FN: &str = "channel-count";
pub const HOST_CAPABILITY_CHANNEL_HANDLE_FN: &str = "channel-handle";

/// Default plugin entrypoint exported by the runtime interface.
pub const DEFAULT_RUNTIME_ENTRYPOINT: &str = "run";
/// Source of truth WIT document embedded for tooling/tests.
pub const WIT_SOURCE: &str = include_str!("../wit/palyra-sdk.wit");

/// Returns the WIT package identifier.
#[must_use]
pub fn wit_package_id() -> &'static str {
    WIT_PACKAGE_ID
}

/// Returns embedded WIT source text.
#[must_use]
pub fn wit_source() -> &'static str {
    WIT_SOURCE
}

#[cfg(test)]
mod tests {
    use super::{
        wit_package_id, wit_source, HOST_CAPABILITIES_IMPORT_MODULE,
        HOST_CAPABILITY_CHANNEL_COUNT_FN, HOST_CAPABILITY_CHANNEL_HANDLE_FN,
        HOST_CAPABILITY_HTTP_COUNT_FN, HOST_CAPABILITY_HTTP_HANDLE_FN,
        HOST_CAPABILITY_SECRET_COUNT_FN, HOST_CAPABILITY_SECRET_HANDLE_FN,
        HOST_CAPABILITY_STORAGE_COUNT_FN, HOST_CAPABILITY_STORAGE_HANDLE_FN, WIT_WORLD_NAME,
    };

    #[test]
    fn wit_package_id_is_stable() {
        assert_eq!(wit_package_id(), "palyra:plugins/sdk@0.1.0");
    }

    #[test]
    fn wit_source_contains_expected_world_and_imports() {
        let source = wit_source();
        assert!(source.contains("world palyra-plugin"));
        assert!(source.contains("import host-capabilities"));
        assert!(source.contains("run: func() -> s32"));
        assert!(source.contains("plugin-hello: func() -> string"));
        assert!(source.contains(HOST_CAPABILITY_HTTP_COUNT_FN));
        assert!(source.contains(HOST_CAPABILITY_HTTP_HANDLE_FN));
        assert!(source.contains(HOST_CAPABILITY_SECRET_COUNT_FN));
        assert!(source.contains(HOST_CAPABILITY_SECRET_HANDLE_FN));
        assert!(source.contains(HOST_CAPABILITY_STORAGE_COUNT_FN));
        assert!(source.contains(HOST_CAPABILITY_STORAGE_HANDLE_FN));
        assert!(source.contains(HOST_CAPABILITY_CHANNEL_COUNT_FN));
        assert!(source.contains(HOST_CAPABILITY_CHANNEL_HANDLE_FN));
    }

    #[test]
    fn exported_wit_symbol_names_are_stable() {
        assert_eq!(WIT_WORLD_NAME, "palyra-plugin");
        assert_eq!(HOST_CAPABILITIES_IMPORT_MODULE, "palyra:plugins/host-capabilities@0.1.0");
    }
}
