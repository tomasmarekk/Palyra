//! Feature-off preflight coverage and feature-on fault-runtime test routing.

use super::*;

#[cfg(feature = "qa-fault-injection")]
mod feature;

#[cfg(not(feature = "qa-fault-injection"))]
#[test]
fn feature_off_rejects_activation_environment() {
    let _guard = crate::test_env::lock();
    std::env::set_var(QA_FAULT_LAUNCH_PATH_ENV, "qa-fault/launch.json");

    let error = load_fault_injection(Path::new("unused"))
        .expect_err("feature-off build must reject activation");

    std::env::remove_var(QA_FAULT_LAUNCH_PATH_ENV);
    assert!(error.to_string().starts_with(FEATURE_DISABLED_REASON_CODE));
}
