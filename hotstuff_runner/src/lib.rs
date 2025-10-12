// hotstuff_runner/src/lib.rs
pub mod app;
pub mod event;
pub mod kv_store;
pub mod pompe;
pub mod pompe_adversary;
pub mod pompe_network;
pub mod utils;
pub mod stats;
pub mod tcp_node;
pub mod tokio_network;

pub mod smrol;
pub use smrol::{PnfifoBc, SmrolMessage, SmrolThresholdSig};

// 重新导出常用类型
pub use app::TestApp;
pub use kv_store::MemoryKVStore;
pub use stats::PerformanceStats;

use std::io::Write;

pub fn log_node(node_id: usize, level: log::Level, message: &str) {
    let file_path = format!("log/consensus-node{}.log", node_id);
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(file_path)
    {
        let timestamp = chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f");
        writeln!(file, "[{}][{}] {}", timestamp, level, message).ok();
    }
}

use std::collections::HashMap;
use std::sync::OnceLock;

static HOST_MAP: OnceLock<HashMap<String, String>> = OnceLock::new();

pub fn resolve_target(target_id: usize, port: u16) -> String {
    let target_name = format!("node{}", target_id);

    // 初始化 HOST_MAP（只执行一次）
    let map = HOST_MAP.get_or_init(|| {
        let hosts_str = std::env::var("NODE_HOSTS")
            .unwrap_or_else(|_| panic!("NODE_HOSTS environment variable not set"));
        hosts_str
            .split(',')
            .filter_map(|entry| {
                let mut parts = entry.split(':');
                let name = parts.next()?.trim().to_string();
                let ip = parts.next()?.trim().to_string();
                Some((name, ip))
            })
            .collect::<HashMap<_, _>>()
    });

    let host_ip = map
        .get(&target_name)
        .unwrap_or_else(|| panic!("Target {} not found in NODE_HOSTS", target_name));

    format!("{}:{}", host_ip, port)
}