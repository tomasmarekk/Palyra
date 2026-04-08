mod recovery;

use crate::*;

pub(crate) use recovery::build_doctor_support_bundle_value;
pub(crate) use recovery::DoctorCommandRequest;

pub(crate) fn run_doctor(request: DoctorCommandRequest) -> Result<()> {
    recovery::run_doctor(request)
}
