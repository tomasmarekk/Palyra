//! Mode and TLS scaffold selectors shared by `palyra setup` (alias `init`) and
//! the configure/onboarding wizards. Help text is pinned by snapshot tests; see
//! the doc-comment rules in `mod.rs`.

use clap::ValueEnum;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum InitModeArg {
    Local,
    Remote,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum InitTlsScaffoldArg {
    None,
    BringYourOwn,
    SelfSigned,
}
