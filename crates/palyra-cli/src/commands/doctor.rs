//! Entry point for the `palyra doctor` command.
//!
//! Thin facade over [`recovery`], which owns diagnostics rendering, repair
//! planning, apply/rollback execution, and recovery-run manifests.

mod recovery;

use crate::*;

pub(crate) use recovery::build_doctor_support_bundle_value;
pub(crate) use recovery::DoctorCommandRequest;

/// Runs doctor diagnostics and the optional repair or rollback flow.
///
/// # Errors
/// Returns an error when diagnostics cannot be built, a repair or rollback
/// step fails, or `--strict` is set and a blocking check is failing.
pub(crate) fn run_doctor(request: DoctorCommandRequest) -> Result<()> {
    recovery::run_doctor(request)
}
