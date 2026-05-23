//! 第一阶段集成测试：百万级事件正确性 + 吞吐
use strack_sim::core::{Event, EventKind, Simulator};

#[test]
fn process_one_million_events_in_order() {
    let mut sim = Simulator::new();
    for t in (0..1_000_000u64).rev() {
        // 逆序插入，考验堆的排序能力
        sim.schedule(Event::new(t, EventKind::Custom("x".into()), 0));
    }
    let t0 = std::time::Instant::now();
    sim.run();
    let dt = t0.elapsed();

    assert_eq!(sim.processed(), 1_000_000);
    assert_eq!(sim.now(), 999_999);

    let throughput = sim.processed() as f64 / dt.as_secs_f64() / 1e6;
    println!("1M events processed in {:?} ({:.2} M ev/s)", dt, throughput);
    // release 模式下应远超此阈值；debug 模式留足余量
    assert!(
        throughput > 0.5,
        "throughput too low: {:.2} M ev/s",
        throughput
    );
}

#[test]
fn stop_event_stops_pop_event_driver() {
    let mut sim = Simulator::new();
    sim.schedule(Event::new(100, EventKind::Stop, 0));
    sim.schedule(Event::new(200, EventKind::Custom("late".into()), 0));

    while let Some(ev) = sim.pop_event() {
        if matches!(ev.kind, EventKind::Stop) {
            sim.stop();
        }
    }

    assert_eq!(sim.processed(), 1);
    assert_eq!(sim.pending(), 1);
    assert_eq!(sim.now(), 100);
}
