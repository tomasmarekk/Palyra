use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);

    let proto_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../schemas/proto");
    let gateway_proto = proto_root.join("palyra/v1/gateway.proto");
    let cron_proto = proto_root.join("palyra/v1/cron.proto");
    let memory_proto = proto_root.join("palyra/v1/memory.proto");
    let auth_proto = proto_root.join("palyra/v1/auth.proto");
    let common_proto = proto_root.join("palyra/v1/common.proto");
    let node_proto = proto_root.join("palyra/v1/node.proto");
    let browser_proto = proto_root.join("palyra/v1/browser.proto");

    println!("cargo:rerun-if-changed={}", gateway_proto.display());
    println!("cargo:rerun-if-changed={}", cron_proto.display());
    println!("cargo:rerun-if-changed={}", memory_proto.display());
    println!("cargo:rerun-if-changed={}", auth_proto.display());
    println!("cargo:rerun-if-changed={}", common_proto.display());
    println!("cargo:rerun-if-changed={}", node_proto.display());
    println!("cargo:rerun-if-changed={}", browser_proto.display());

    tonic_build::configure().build_server(true).build_client(true).compile_protos(
        &[
            gateway_proto,
            cron_proto,
            memory_proto,
            auth_proto,
            common_proto,
            node_proto,
            browser_proto,
        ],
        &[proto_root],
    )?;

    Ok(())
}
