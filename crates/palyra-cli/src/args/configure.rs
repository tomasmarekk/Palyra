use clap::ValueEnum;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ConfigureSectionArg {
    Workspace,
    AuthModel,
    Gateway,
    DaemonService,
    Channels,
    Skills,
    HealthSecurity,
}
