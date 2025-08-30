//! 简化的系统事件定义 - 支持模块间通信

use std::time::Instant;
use serde::{Serialize, Deserialize};

// 客户端交易结构
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TestTransaction {
    pub id: u64,
    pub from: String,
    pub to: String,
    pub amount: u64,
    pub timestamp: u64,
    pub nonce: u64,
}
/// 🔥 系统事件 - 用于 docker_node, tcp_node, pompe 之间的通信
#[derive(Debug, Clone)]
pub enum SystemEvent {
    TransactionReceived {
        transaction: TestTransaction,
        is_pompe: bool,
    },
    /// Pompe Ordering1 阶段完成
    PompeOrdering1Completed {
        tx_id: u64,
        // timestamp_us: u64,
    },
    
    /// HotStuff 区块提交完成
    HotStuffCommitted {
        block_height: u64,
        tx_ids: Vec<u64>,
        // commit_time: Instant,
    },
    TransactionProcessed {
        count: usize,
    },
    PompeOutputReady {
        transactions: Vec<String>,
    },
    HotStuffConsumed {
        count: usize,
    },
    NetworkStatsUpdate {
        connections: usize,
        messages: usize,
    },
    PerformanceUpdate {
        submission_tps: f64,
        consensus_tps: f64,
        pompe_tps: f64,
    },
}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ResponseCommand {
    Ordering1Response { tx_ids: Vec<u64> },
    HotStuffCommitted { tx_ids: Vec<u64> },
    Error { tx_ids: Vec<u64>, error_msg: String },
}

