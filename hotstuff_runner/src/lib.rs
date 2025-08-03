// hotstuff_runner/src/lib.rs
pub mod app;
pub mod network;
pub mod kv_store;

// 重新导出常用类型
pub use app::TestApp;
pub use network::{TestNetwork, NodeNetwork};
pub use kv_store::MemoryKVStore;
pub mod node;  