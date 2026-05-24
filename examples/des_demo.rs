//! 第一阶段演示：纯 DES 引擎调度，无任何网络概念
//!
//! 运行：
//! ```bash
//! cargo run --release --example des_demo
//! ```
//!
//! 输出：
//! - 标准输出：场景描述与统计结果
//! - 文件 logs/des_demo.log：每个事件的 tracing 日志

use std::cell::RefCell;
use std::fs::OpenOptions;
use std::rc::Rc;
use std::sync::Mutex;
use fabric_sim::core::{Event, EventKind, Simulator};
use tracing_subscriber::fmt::writer::MakeWriterExt;

fn init_logger() {
    let _ = std::fs::create_dir_all("logs");
    let log_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open("logs/des_demo.log")
        .expect("无法打开日志文件");
    let writer = Mutex::new(log_file);
    tracing_subscriber::fmt()
        .with_writer(writer.with_max_level(tracing::Level::INFO))
        .with_ansi(false)
        .with_target(false)
        .init();
}

fn main() {
    init_logger();
    println!("=== STrack-Sim DES 引擎演示 ===\n");

    // ---------------- 场景 1：10 个乱序事件 ----------------
    println!("场景 1：10 个独立事件按时间排序");
    let mut sim = Simulator::new();
    let log: Rc<RefCell<Vec<u64>>> = Rc::new(RefCell::new(Vec::new()));
    let log_clone = Rc::clone(&log);
    sim.register_handler(
        1,
        Box::new(move |ev, _now| {
            log_clone.borrow_mut().push(ev.time);
            tracing::info!(time = ev.time, kind = ?ev.kind, "事件触发");
            Vec::new()
        }),
    );
    for t in [500u64, 100, 900, 300, 700, 200, 800, 400, 600, 1000] {
        sim.schedule(Event::new(t, EventKind::Custom(format!("t={t}")), 1));
    }
    sim.run();
    println!("  处理顺序：{:?}", *log.borrow());
    println!("  当前时钟：{} ns", sim.now());
    println!("  已处理事件：{}\n", sim.processed());

    // ---------------- 场景 2：链式 10 万事件 ----------------
    println!("场景 2：链式 10 万事件压力测试");
    let mut sim2 = Simulator::new();
    let counter: Rc<RefCell<u64>> = Rc::new(RefCell::new(0));
    let counter_clone = Rc::clone(&counter);
    sim2.register_handler(
        2,
        Box::new(move |ev, now| {
            *counter_clone.borrow_mut() += 1;
            if *counter_clone.borrow() < 100_000 {
                vec![Event::new(now + 1, ev.kind.clone(), ev.target)]
            } else {
                Vec::new()
            }
        }),
    );
    sim2.schedule(Event::new(0, EventKind::Custom("chain".into()), 2));
    let t0 = std::time::Instant::now();
    sim2.run();
    let dt = t0.elapsed();
    println!("  最终时钟：{} ns", sim2.now());
    println!("  已处理事件：{}", sim2.processed());
    println!("  墙钟耗时：{:?}", dt);
    println!(
        "  吞吐：{:.2} M events/sec",
        sim2.processed() as f64 / dt.as_secs_f64() / 1e6
    );

    // ---------------- 场景 3：100 万独立事件（逆序插入） ----------------
    println!("\n场景 3：100 万独立事件（逆序插入，考验堆排序）");
    let mut sim3 = Simulator::new();
    sim3.register_handler(3, Box::new(|_ev, _now| Vec::new()));
    for t in (0..1_000_000u64).rev() {
        sim3.schedule(Event::new(t, EventKind::Custom("x".into()), 3));
    }
    let t0 = std::time::Instant::now();
    sim3.run();
    let dt = t0.elapsed();
    println!("  最终时钟：{} ns", sim3.now());
    println!("  已处理事件：{}", sim3.processed());
    println!("  墙钟耗时：{:?}", dt);
    println!(
        "  吞吐：{:.2} M events/sec",
        sim3.processed() as f64 / dt.as_secs_f64() / 1e6
    );

    println!("\n详细事件日志已写入 logs/des_demo.log");
}
