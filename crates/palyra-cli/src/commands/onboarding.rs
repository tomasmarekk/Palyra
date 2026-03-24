use crate::*;

pub(crate) fn run_onboarding(command: OnboardingCommand) -> Result<()> {
    match command {
        OnboardingCommand::Wizard { path, force, options } => {
            commands::operator_wizard::run_onboarding_wizard(
                commands::operator_wizard::OnboardingWizardRequest {
                    path,
                    force,
                    setup_mode: None,
                    setup_tls_scaffold: None,
                    options,
                },
            )
        }
    }
}
