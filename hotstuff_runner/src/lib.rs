// hotstuff_runner/src/lib.rs
pub mod app;
pub mod network;
pub mod kv_store;
pub mod tcp_network;
pub mod tcp_node;

// 重新导出常用类型
pub use app::TestApp;
pub use network::{TestNetwork, NodeNetwork};
pub use kv_store::MemoryKVStore;


use std::io::Write;

pub fn log_node(node_id: usize, level: log::Level, message: &str) {
    let file_path = format!("log/node{}.log", node_id);
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(file_path)
    {
        let timestamp = chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f");
        writeln!(file, "[{}][{}] {}", timestamp, level, message).ok();
    }
}