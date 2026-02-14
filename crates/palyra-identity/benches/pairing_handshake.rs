use criterion::{criterion_group, criterion_main, Criterion};
use palyra_identity::{DeviceIdentity, IdentityManager, PairingClientKind, PairingMethod};
use std::time::SystemTime;

fn pairing_roundtrip_benchmark(c: &mut Criterion) {
    c.bench_function("pairing_roundtrip_node_pin", |b| {
        b.iter(|| {
            let mut manager =
                IdentityManager::with_memory_store().expect("manager should initialize");
            let device_id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
            let device = DeviceIdentity::generate(device_id).expect("device should generate");
            let session = manager
                .start_pairing(
                    PairingClientKind::Node,
                    PairingMethod::Pin { code: "123456".to_owned() },
                    SystemTime::now(),
                )
                .expect("session should start");
            let hello = manager
                .build_device_hello(&session, &device, "123456")
                .expect("hello should build");
            manager.complete_pairing(hello, SystemTime::now()).expect("pairing should complete");
        });
    });
}

criterion_group!(benches, pairing_roundtrip_benchmark);
criterion_main!(benches);
