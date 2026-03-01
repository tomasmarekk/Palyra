use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion};
use palyra_policy::{evaluate, PolicyRequest};

fn bench_policy_evaluate(c: &mut Criterion) {
    let request = PolicyRequest {
        principal: "user:bench".to_owned(),
        action: "tool.execute.shell".to_owned(),
        resource: "tool:shell".to_owned(),
    };

    c.bench_function("evaluate_deny_by_default", |b| b.iter(|| evaluate(black_box(&request))));
}

criterion_group!(benches, bench_policy_evaluate);
criterion_main!(benches);
