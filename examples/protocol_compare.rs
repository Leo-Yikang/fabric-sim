//! P3 端到端示例：协议 baseline 对比
//!
//! 对比 ECMP / STrack / DCQCN / HPCC / Swift 在 Incast 场景下的表现

use strack_sim::nic::{
    DcqcnProtocol, HpccProtocol, SimpleTcp, STrackMode, STrackProtocol, SwiftProtocol,
};
use strack_sim::sim_runner::SimRunner;
use strack_sim::topology::LeafSpine;
use strack_sim::traffic::Incast;

fn main() {
    println!("=== STrack-Sim P3 示例：协议 baseline 对比 ===");
    println!("拓扑：4 Leaf × 8 Spine × 4 host/leaf = 16 hosts");
    println!("流量：15 个发送端同时向 host 0 发 512 KB（Incast）\n");

    // SimpleTcp
    {
        let summary = {
            let topo = LeafSpine {
                n_leaf: 4,
                n_spine: 8,
                hosts_per_leaf: 4,
                host_link_bps: 100_000_000_000,
                fabric_link_bps: 400_000_000_000,
                prop_delay_ns: 500,
                ecn_threshold_bytes: 20_000,
                buffer_bytes: 200_000,
            }
            .build();
            let n_hosts = topo.num_hosts();
            let mut runner = SimRunner::new(topo, "TCP".to_string(), |h, _topo| {
                Box::new(SimpleTcp::new(h))
            })
            .expect("SimRunner 初始化失败");
            let senders: Vec<u32> = (1..n_hosts as u32).collect();
            let flows = Incast {
                senders,
                receiver: 0,
                bytes_per_sender: 512 * 1024,
                start_time_ns: 1000,
            }
            .generate();
            runner.inject_flows(flows);
            runner.run(50_000_000);
            runner.summarize()
        };
        summary.pretty_print();
    }

    // ECMP
    {
        let summary = {
            let topo = LeafSpine {
                n_leaf: 4,
                n_spine: 8,
                hosts_per_leaf: 4,
                host_link_bps: 100_000_000_000,
                fabric_link_bps: 400_000_000_000,
                prop_delay_ns: 500,
                ecn_threshold_bytes: 20_000,
                buffer_bytes: 200_000,
            }
            .build();
            let n_hosts = topo.num_hosts();
            let mut runner = SimRunner::new(topo, "ECMP".to_string(), |h, topo| {
                Box::new(STrackProtocol::new(h, STrackMode::Ecmp, topo))
            })
            .expect("SimRunner 初始化失败");
            let senders: Vec<u32> = (1..n_hosts as u32).collect();
            let flows = Incast {
                senders,
                receiver: 0,
                bytes_per_sender: 512 * 1024,
                start_time_ns: 1000,
            }
            .generate();
            runner.inject_flows(flows);
            runner.run(50_000_000);
            runner.summarize()
        };
        summary.pretty_print();
    }

    // STrack
    {
        let summary = {
            let topo = LeafSpine {
                n_leaf: 4,
                n_spine: 8,
                hosts_per_leaf: 4,
                host_link_bps: 100_000_000_000,
                fabric_link_bps: 400_000_000_000,
                prop_delay_ns: 500,
                ecn_threshold_bytes: 20_000,
                buffer_bytes: 200_000,
            }
            .build();
            let n_hosts = topo.num_hosts();
            let mut runner = SimRunner::new(topo, "STrack".to_string(), |h, topo| {
                Box::new(STrackProtocol::new(h, STrackMode::Strack, topo))
            })
            .expect("SimRunner 初始化失败");
            let senders: Vec<u32> = (1..n_hosts as u32).collect();
            let flows = Incast {
                senders,
                receiver: 0,
                bytes_per_sender: 512 * 1024,
                start_time_ns: 1000,
            }
            .generate();
            runner.inject_flows(flows);
            runner.run(50_000_000);
            runner.summarize()
        };
        summary.pretty_print();
    }

    // DCQCN
    {
        let summary = {
            let topo = LeafSpine {
                n_leaf: 4,
                n_spine: 8,
                hosts_per_leaf: 4,
                host_link_bps: 100_000_000_000,
                fabric_link_bps: 400_000_000_000,
                prop_delay_ns: 500,
                ecn_threshold_bytes: 20_000,
                buffer_bytes: 200_000,
            }
            .build();
            let n_hosts = topo.num_hosts();
            let mut runner = SimRunner::new(topo, "DCQCN".to_string(), |h, _topo| {
                Box::new(DcqcnProtocol::new(h))
            })
            .expect("SimRunner 初始化失败");
            let senders: Vec<u32> = (1..n_hosts as u32).collect();
            let flows = Incast {
                senders,
                receiver: 0,
                bytes_per_sender: 512 * 1024,
                start_time_ns: 1000,
            }
            .generate();
            runner.inject_flows(flows);
            runner.run(50_000_000);
            runner.summarize()
        };
        summary.pretty_print();
    }

    // HPCC
    {
        let summary = {
            let topo = LeafSpine {
                n_leaf: 4,
                n_spine: 8,
                hosts_per_leaf: 4,
                host_link_bps: 100_000_000_000,
                fabric_link_bps: 400_000_000_000,
                prop_delay_ns: 500,
                ecn_threshold_bytes: 20_000,
                buffer_bytes: 200_000,
            }
            .build();
            let n_hosts = topo.num_hosts();
            let mut runner = SimRunner::new(topo, "HPCC".to_string(), |h, _topo| {
                Box::new(HpccProtocol::new(h))
            })
            .expect("SimRunner 初始化失败");
            let senders: Vec<u32> = (1..n_hosts as u32).collect();
            let flows = Incast {
                senders,
                receiver: 0,
                bytes_per_sender: 512 * 1024,
                start_time_ns: 1000,
            }
            .generate();
            runner.inject_flows(flows);
            runner.run(50_000_000);
            runner.summarize()
        };
        summary.pretty_print();
    }

    // Swift
    {
        let summary = {
            let topo = LeafSpine {
                n_leaf: 4,
                n_spine: 8,
                hosts_per_leaf: 4,
                host_link_bps: 100_000_000_000,
                fabric_link_bps: 400_000_000_000,
                prop_delay_ns: 500,
                ecn_threshold_bytes: 20_000,
                buffer_bytes: 200_000,
            }
            .build();
            let n_hosts = topo.num_hosts();
            let mut runner = SimRunner::new(topo, "Swift".to_string(), |h, _topo| {
                Box::new(SwiftProtocol::new(h))
            })
            .expect("SimRunner 初始化失败");
            let senders: Vec<u32> = (1..n_hosts as u32).collect();
            let flows = Incast {
                senders,
                receiver: 0,
                bytes_per_sender: 512 * 1024,
                start_time_ns: 1000,
            }
            .generate();
            runner.inject_flows(flows);
            runner.run(50_000_000);
            runner.summarize()
        };
        summary.pretty_print();
    }
}
