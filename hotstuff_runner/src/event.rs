//! Simplified system event definitions for inter-module communication

use serde::{Deserialize, Serialize};
use std::time::Instant;

// Client transaction structure
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TestTransaction {
    pub id: u64,
    pub from: String,
    pub to: String,
    pub amount: u64,
    pub timestamp: u64,
    pub nonce: u64,
}
/// System events used for communication among docker_node, tcp_node, and pompe
#[derive(Debug, Clone)]
pub enum SystemEvent {
    TransactionReceived {
        transaction: TestTransaction,
        is_pompe: bool,
    },
    // HotStuff starts a new view (allows Pompe to compute the current leader)
    StartView {
        view: u64,
    },
    /// Pompe ordering1 phase completed
    PompeOrdering1Completed {
        tx_id: u64,
        // timestamp_us: u64,
    },

    /// SMROL ordering completed before invoking HotStuff consensus
    SmrolOrderingCompleted {
        tx_ids: Vec<u64>,
    },

    /// Pompe pushed transactions into the HotStuff queue
    PushedToHotStuff {
        tx_ids: Vec<u64>,
    },

    /// HotStuff block commit completed
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
    SmrolOrderingResponse { tx_ids: Vec<u64> },
    HotStuffCommitted { tx_ids: Vec<u64> },
    Error { tx_ids: Vec<u64>, error_msg: String },
}
