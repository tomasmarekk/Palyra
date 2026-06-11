//! Shared identifier argument shapes for command families that historically
//! required `--id` but also need to accept positional ids for CLI consistency.

use clap::Args;

#[derive(Debug, Clone, PartialEq, Eq, Args)]
pub struct RequiredCommandIdArg {
    #[arg(
        value_name = "ID",
        required_unless_present = "id_flag",
        conflicts_with = "id_flag",
        help = "Command identifier; positional alternative to --id"
    )]
    positional_id: Option<String>,
    #[arg(
        long = "id",
        value_name = "ID",
        required_unless_present = "positional_id",
        conflicts_with = "positional_id"
    )]
    id_flag: Option<String>,
}

impl RequiredCommandIdArg {
    #[must_use]
    pub fn value(&self) -> &str {
        self.id_flag
            .as_deref()
            .or(self.positional_id.as_deref())
            .expect("clap requires either positional ID or --id")
    }

    #[cfg(test)]
    pub(crate) fn from_flag(value: impl Into<String>) -> Self {
        Self { positional_id: None, id_flag: Some(value.into()) }
    }

    #[cfg(test)]
    pub(crate) fn from_positional(value: impl Into<String>) -> Self {
        Self { positional_id: Some(value.into()), id_flag: None }
    }
}
