//! DES 引擎吞吐基准测试
//!
//! 运行：`cargo bench`

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use strack_sim::core::{Event, EventKind, Simulator};

fn bench_push_pop(c: &mut Criterion) {
    c.bench_function("schedule_run_100k_independent", |b| {
        b.iter(|| {
            let mut sim = Simulator::new();
            for t in 0..100_000u64 {
                sim.schedule(Event::new(
                    black_box(t),
                    EventKind::Custom("x".into()),
                    0,
                ));
            }
            sim.run();
            black_box(sim.processed());
        });
    });
}

criterion_group!(benches, bench_push_pop);
criterion_main!(benches);
