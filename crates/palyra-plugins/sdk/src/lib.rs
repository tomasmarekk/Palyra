//! Bootstrap SDK placeholder for Palyra plugin authors.
//!
//! The initial WIT contract is defined in `wit/palyra-sdk.wit`.

/// WIT package identifier for the bootstrap plugin SDK contract.
pub const WIT_PACKAGE_ID: &str = "palyra:plugins/sdk@0.1.0";

/// Returns the WIT package identifier.
#[must_use]
pub fn wit_package_id() -> &'static str {
    WIT_PACKAGE_ID
}

#[cfg(test)]
mod tests {
    use super::wit_package_id;

    #[test]
    fn wit_package_id_is_stable() {
        assert_eq!(wit_package_id(), "palyra:plugins/sdk@0.1.0");
    }
}
