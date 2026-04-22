use clap::ValueEnum;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ConfigureSectionArg {
    DeploymentProfile,
    Workspace,
    AuthModel,
    Gateway,
    RuntimeControls,
    DaemonService,
    Channels,
    Skills,
    HealthSecurity,
}
