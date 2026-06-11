//! Binary entry point for the `palyra` operator CLI; all argument parsing,
//! dispatch, and error rendering live in [`palyra_cli::run`].

fn main() -> std::process::ExitCode {
    palyra_cli::run()
}
