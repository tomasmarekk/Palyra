//! Reference canonical worker process for transport conformance and local deployment.
//! `--stdio` executes the full remote request envelope from a scoped bundle.

use std::{
    env,
    io::{self, Read, Write},
};

use palyra_workerd::{network_runtime::ReferenceNetworkWorker, WorkerRemoteToolRequestEnvelope};

fn main() {
    if let Err(error) = run() {
        eprintln!("palyra-workerd failed: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = env::args_os().skip(1);
    if args.next().as_deref() != Some(std::ffi::OsStr::new("--stdio")) || args.next().is_some() {
        return Err("usage: palyra-workerd --stdio".into());
    }
    run_canonical_stdio()
}

fn run_canonical_stdio() -> Result<(), Box<dyn std::error::Error>> {
    let mut input = Vec::new();
    io::stdin().take(1024 * 1024 + 1).read_to_end(&mut input)?;
    if input.len() > 1024 * 1024 {
        return Err("palyra-workerd request exceeds the stdio transport limit".into());
    }
    let request: WorkerRemoteToolRequestEnvelope = serde_json::from_slice(input.as_slice())?;
    let response = ReferenceNetworkWorker::execute_remote_request(&request, unix_time_ms()?)?;
    let output = serde_json::to_vec(&response)?;
    if output.len() > 1024 * 1024 {
        return Err("palyra-workerd response exceeds the stdio transport limit".into());
    }
    io::stdout().write_all(output.as_slice())?;
    Ok(())
}

fn unix_time_ms() -> Result<i64, std::time::SystemTimeError> {
    let millis = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis();
    Ok(i64::try_from(millis).unwrap_or(i64::MAX))
}
