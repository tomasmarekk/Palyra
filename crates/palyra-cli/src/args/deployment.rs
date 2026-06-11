//! Arguments for `palyra deployment`: profile listing, manifests, preflights,
//! deploy recipes, upgrade smoke checks, promotion checks, and rollback plans.
//! Help text is pinned by snapshot tests; see the doc-comment rules in `mod.rs`.

use clap::{Subcommand, ValueEnum};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum DeploymentProfileArg {
    Local,
    SingleVm,
    WorkerEnabled,
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum DeploymentCommand {
    Profiles {
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Manifest {
        #[arg(long = "deployment-profile", value_enum)]
        deployment_profile: DeploymentProfileArg,
        #[arg(long)]
        output: Option<String>,
    },
    Preflight {
        #[arg(long = "deployment-profile", value_enum)]
        deployment_profile: Option<DeploymentProfileArg>,
        #[arg(long)]
        path: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Recipe {
        #[arg(long = "deployment-profile", value_enum)]
        deployment_profile: DeploymentProfileArg,
        #[arg(long)]
        output_dir: String,
    },
    UpgradeSmoke {
        #[arg(long = "deployment-profile", value_enum)]
        deployment_profile: DeploymentProfileArg,
        #[arg(long)]
        path: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    PromotionCheck {
        #[arg(long = "deployment-profile", value_enum)]
        deployment_profile: DeploymentProfileArg,
        #[arg(long)]
        gates: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    RollbackPlan {
        #[arg(long = "deployment-profile", value_enum)]
        deployment_profile: DeploymentProfileArg,
        #[arg(long)]
        output: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}
