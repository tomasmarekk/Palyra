//! Deterministic native-terminal fixture for cross-platform PTY conformance.

use std::env;
use std::io::{self, BufRead as _, IsTerminal as _, Write as _};
use std::process;

fn main() -> io::Result<()> {
    let mode = env::args().nth(1).unwrap_or_else(|| "probe".to_owned());
    match mode.as_str() {
        "probe" => {
            if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
                eprintln!("PTY_INACTIVE");
                process::exit(23);
            }
            println!("PTY_RUNTIME_OK");
        }
        "repl" => {
            if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
                process::exit(23);
            }
            print!("input> ");
            io::stdout().flush()?;
            let mut line = String::new();
            io::stdin().lock().read_line(&mut line)?;
            println!("got:{}", line.trim_end_matches(['\r', '\n']));
        }
        "osc" => {
            print!("\x1b]52;c;blocked\x07safe\x1b[31m-red-\x1b[0m");
            io::stdout().flush()?;
        }
        _ => process::exit(64),
    }
    Ok(())
}
