//! Shell targets for `palyra completion` script generation. Help text is
//! pinned by snapshot tests; see the doc-comment rules in `mod.rs`.

use clap::ValueEnum;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum CompletionShell {
    Bash,
    Zsh,
    Fish,
    Powershell,
    Elvish,
}
