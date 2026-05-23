//! 优化前后热点路径对比基准
//!
//! 运行：
//! ```bash
//! cargo bench --bench optimization_compare
//! ```
//!
//! 这个 benchmark 不切换 Git 版本，而是在同一个二进制里同时放入：
//! - 优化前：std::collections::BinaryHeap 事件队列
//! - 优化后：当前 crate 的 4-ary `EventQueue`
//! - 优化前：HashMap packet buffer
//! - 优化后：Vec<Option<Packet>> + free-list slab

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::collections::{BinaryHeap, HashMap};
use strack_sim::core::{Event, EventKind, EventQueue};
use strack_sim::network::Packet;

struct OldBinaryHeapQueue {
    heap: BinaryHeap<Event>,
}

impl OldBinaryHeapQueue {
    fn with_capacity(cap: usize) -> Self {
        Self {
            heap: BinaryHeap::with_capacity(cap),
        }
    }

    #[inline]
    fn push(&mut self, ev: Event) {
        self.heap.push(ev);
    }

    #[inline]
    fn pop(&mut self) -> Option<Event> {
        self.heap.pop()
    }
}

struct BenchPacketSlab {
    slots: Vec<Option<Packet>>,
    free: Vec<u64>,
}

impl BenchPacketSlab {
    fn with_capacity(cap: usize) -> Self {
        Self {
            slots: Vec::with_capacity(cap),
            free: Vec::new(),
        }
    }

    #[inline]
    fn insert(&mut self, pkt: Packet) -> u64 {
        let id = if let Some(id) = self.free.pop() {
            self.slots[id as usize] = Some(pkt);
            id
        } else {
            let id = self.slots.len() as u64;
            self.slots.push(Some(pkt));
            id
        };
        if let Some(Some(ref mut p)) = self.slots.get_mut(id as usize) {
            p.id = id;
        }
        id
    }

    #[inline]
    fn remove(&mut self, id: u64) -> Option<Packet> {
        let idx = id as usize;
        if idx >= self.slots.len() {
            return None;
        }
        let pkt = self.slots[idx].take();
        if pkt.is_some() {
            self.free.push(id);
        }
        pkt
    }
}

fn make_event(i: u64) -> Event {
    let time = i.wrapping_mul(1_103_515_245).wrapping_add(12_345) % 1_000_000;
    Event::new(
        time,
        EventKind::PacketArrive {
            packet_id: i,
            src: (i % 1024) as u32,
        },
        (i % 1024) as u32,
    )
}

fn make_packet(i: u64) -> Packet {
    Packet::data(i, 0, (i % 4096) as u32, (i % 65_536) as u32, 1, 2, i)
}

fn bench_event_queue(c: &mut Criterion) {
    let mut group = c.benchmark_group("event_queue_old_binaryheap_vs_new_4ary");
    for &n in &[100_000usize, 1_000_000] {
        group.throughput(Throughput::Elements(n as u64));

        group.bench_with_input(BenchmarkId::new("old_binaryheap", n), &n, |b, &n| {
            b.iter(|| {
                let mut q = OldBinaryHeapQueue::with_capacity(n);
                for i in 0..n as u64 {
                    q.push(make_event(black_box(i)));
                }
                let mut processed = 0u64;
                while q.pop().is_some() {
                    processed += 1;
                }
                black_box(processed);
            });
        });

        group.bench_with_input(BenchmarkId::new("new_4ary_heap", n), &n, |b, &n| {
            b.iter(|| {
                let mut q = EventQueue::with_capacity(n);
                for i in 0..n as u64 {
                    q.push(make_event(black_box(i)));
                }
                let mut processed = 0u64;
                while q.pop().is_some() {
                    processed += 1;
                }
                black_box(processed);
            });
        });
    }
    group.finish();
}

fn bench_packet_buffer(c: &mut Criterion) {
    let mut group = c.benchmark_group("packet_buffer_old_hashmap_vs_new_slab");
    for &n in &[100_000usize, 1_000_000] {
        group.throughput(Throughput::Elements(n as u64));

        group.bench_with_input(BenchmarkId::new("old_hashmap", n), &n, |b, &n| {
            b.iter(|| {
                let mut buf: HashMap<u64, Packet> = HashMap::with_capacity(n);
                for i in 0..n as u64 {
                    buf.insert(i, make_packet(black_box(i)));
                }
                let mut removed = 0u64;
                for i in 0..n as u64 {
                    if buf.remove(&i).is_some() {
                        removed += 1;
                    }
                }
                black_box(removed);
            });
        });

        group.bench_with_input(BenchmarkId::new("new_slab", n), &n, |b, &n| {
            b.iter(|| {
                let mut buf = BenchPacketSlab::with_capacity(n);
                for i in 0..n as u64 {
                    let id = buf.insert(make_packet(black_box(i)));
                    black_box(id);
                }
                let mut removed = 0u64;
                for i in 0..n as u64 {
                    if buf.remove(i).is_some() {
                        removed += 1;
                    }
                }
                black_box(removed);
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_event_queue, bench_packet_buffer);
criterion_main!(benches);
