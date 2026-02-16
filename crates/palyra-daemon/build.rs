use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);

    let proto_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../schemas/proto");
    let gateway_proto = proto_root.join("palyra/v1/gateway.proto");
    let common_proto = proto_root.join("palyra/v1/common.proto");

    println!("cargo:rerun-if-changed={}", gateway_proto.display());
    println!("cargo:rerun-if-changed={}", common_proto.display());

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&[gateway_proto, common_proto], &[proto_root])?;

    Ok(())
}
