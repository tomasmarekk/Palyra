mod recovery;

use crate::*;

pub(crate) use recovery::build_doctor_support_bundle_value;

pub(crate) fn run_doctor(
    strict: bool,
    json: bool,
    repair: bool,
    dry_run: bool,
    force: bool,
    only: Vec<String>,
    skip: Vec<String>,
    rollback_run: Option<String>,
) -> Result<()> {
    recovery::run_doctor(
        strict,
        json,
        repair,
        dry_run,
        force,
        only,
        skip,
        rollback_run,
    )
}
