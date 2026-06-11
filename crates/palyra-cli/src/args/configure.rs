//! Section selector for `palyra configure`; each variant maps to one
//! reconfigurable area of the onboarding wizard engine. Help text is pinned by
//! snapshot tests; see the doc-comment rules in `mod.rs`.

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
