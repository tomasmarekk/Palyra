//! Reference canonical worker process for transport conformance and local deployment.
//! `--stdio` executes the full remote request envelope from a scoped bundle;
//! positional arguments retain the lower-level canonical-task conformance mode.

use std::{
    env,
    io::{self, Read, Write},
    path::PathBuf,
};

use palyra_workerd::{
    network_runtime::ReferenceNetworkWorker, remote_protocol::RemoteWorkerProtocolV1,
    WorkerRemoteToolRequestEnvelope,
};

fn main() {
    if let Err(error) = run() {
        eprintln!("palyra-workerd failed: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = env::args_os().skip(1);
    let first =
        args.next().ok_or("usage: palyra-workerd --stdio | <workspace-root> <worker-id>")?;
    if first == "--stdio" {
        if args.next().is_some() {
            return Err("palyra-workerd --stdio does not accept additional arguments".into());
        }
        return run_canonical_stdio();
    }
    let workspace_root = PathBuf::from(first);
    let worker_id = args
        .next()
        .and_then(|value| value.into_string().ok())
        .ok_or("usage: palyra-workerd --stdio | <workspace-root> <worker-id>")?;
    if args.next().is_some() {
        return Err("unexpected palyra-workerd arguments".into());
    }
    let mut input = Vec::new();
    io::stdin().take(1024 * 1024).read_to_end(&mut input)?;
    let protocol: RemoteWorkerProtocolV1 = serde_json::from_slice(input.as_slice())?;
    let worker = ReferenceNetworkWorker::new(worker_id, workspace_root)?;
    let observed_at_unix_ms = unix_time_ms()?;
    let response = worker.execute(&protocol, observed_at_unix_ms)?;
    let output = serde_json::to_vec(&response)?;
    io::stdout().write_all(output.as_slice())?;
    Ok(())
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
