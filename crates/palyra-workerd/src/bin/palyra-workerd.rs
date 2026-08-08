//! Reference canonical worker process for transport conformance and local deployment.
//! It accepts one `RemoteWorkerProtocolV1` JSON document on stdin and emits one
//! bounded `ReferenceWorkerResponse` JSON document on stdout.

use std::{
    env,
    io::{self, Read, Write},
    path::PathBuf,
};

use palyra_workerd::{
    network_runtime::ReferenceNetworkWorker, remote_protocol::RemoteWorkerProtocolV1,
};

fn main() {
    if let Err(error) = run() {
        eprintln!("palyra-workerd failed: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = env::args_os().skip(1);
    let workspace_root = args
        .next()
        .map(PathBuf::from)
        .ok_or("usage: palyra-workerd <workspace-root> <worker-id>")?;
    let worker_id = args
        .next()
        .and_then(|value| value.into_string().ok())
        .ok_or("usage: palyra-workerd <workspace-root> <worker-id>")?;
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

fn unix_time_ms() -> Result<i64, std::time::SystemTimeError> {
    let millis = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis();
    Ok(i64::try_from(millis).unwrap_or(i64::MAX))
}
