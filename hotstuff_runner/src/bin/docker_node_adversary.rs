// hotstuff_runner/src/bin/docker_node.rs
//! Completely lock-free event-driven Docker node implementation

use crossbeam::queue::SegQueue;
use ed25519_dalek::SigningKey;
use hotstuff_rs::types::{
    crypto_primitives::VerifyingKey,
    data_types::Power,
    update_sets::{AppStateUpdates, ValidatorSetUpdates},
};
use hotstuff_runner::{
    app::TestApp,
    event::{self, ResponseCommand, SystemEvent, TestTransaction},
    pompe::{self, load_pompe_config, LockFreeHotStuffAdapter},
    pompe_adversary::PompeManager,
    smrol::manager::SmrolManager,
    stats::PerformanceStats,
    tcp_node::Node,
    tokio_network::{TokioNetwork, TokioNetworkConfig},
};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::fs::{create_dir_all, File};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Notify};
use tracing::{debug, error, info, warn};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

pub struct TransactionGenerator {
    current_tx_id: u64,
    current_nonce: u64,
    accounts: Vec<String>,
}

impl TransactionGenerator {
    pub fn new() -> Self {
        let accounts = vec![
            "alice".to_string(),
            "bob".to_string(),
            "charlie".to_string(),
            "david".to_string(),
            "eve".to_string(),
        ];

        Self {
            current_tx_id: 0,
            current_nonce: 0,
            accounts,
        }
    }

    pub fn generate_transaction(&mut self) -> TestTransaction {
        let mut rng = rand::thread_rng();

        let from_idx = rng.gen_range(0, self.accounts.len());
        let mut to_idx = rng.gen_range(0, self.accounts.len());
        while to_idx == from_idx {
            to_idx = rng.gen_range(0, self.accounts.len());
        }

        let from = self.accounts[from_idx].clone();
        let to = self.accounts[to_idx].clone();
        let amount = rng.gen_range(1, 100000);

        self.current_tx_id += 1;
        self.current_nonce += 1;

        TestTransaction {
            id: 999999999999999,
            from,
            to,
            amount,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            nonce: self.current_nonce,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ClientMessage {
    pub message_type: String,
    pub transaction: Option<TestTransaction>,
    pub client_id: String,
}

// Completely lock-free statistical counters
#[derive(Debug)]
pub struct LockFreeStats {
    tx_received: AtomicUsize,
    pompe_tx_received: AtomicUsize,
    hotstuff_queue_size: AtomicUsize,
    pompe_queue_size: AtomicUsize,
    hotstuff_consumed: AtomicUsize,
    last_report_time: AtomicU64,
}

impl LockFreeStats {
    fn new() -> Self {
        Self {
            tx_received: AtomicUsize::new(0),
            pompe_tx_received: AtomicUsize::new(0),
            hotstuff_queue_size: AtomicUsize::new(0),
            pompe_queue_size: AtomicUsize::new(0),
            hotstuff_consumed: AtomicUsize::new(0),
            last_report_time: AtomicU64::new(0),
        }
    }

    fn increment_tx_received(&self) -> usize {
        self.tx_received.fetch_add(1, Ordering::Relaxed)
    }

    fn increment_pompe_tx(&self) -> usize {
        self.pompe_tx_received.fetch_add(1, Ordering::Relaxed)
    }

    fn update_hotstuff_queue_size(&self, size: usize) {
        self.hotstuff_queue_size.store(size, Ordering::Relaxed);
    }

    fn update_pompe_queue_size(&self, size: usize) {
        self.pompe_queue_size.store(size, Ordering::Relaxed);
    }

    fn increment_hotstuff_consumed(&self, count: usize) -> usize {
        self.hotstuff_consumed.fetch_add(count, Ordering::Relaxed)
    }

    fn get_stats(&self) -> (usize, usize, usize, usize, usize) {
        (
            self.tx_received.load(Ordering::Relaxed),
            self.pompe_tx_received.load(Ordering::Relaxed),
            self.hotstuff_queue_size.load(Ordering::Relaxed),
            self.pompe_queue_size.load(Ordering::Relaxed),
            self.hotstuff_consumed.load(Ordering::Relaxed),
        )
    }
}

// Regular transaction processor that directly feeds HotStuff queue
#[derive(Debug)]
pub struct RegularTransactionProcessor {
    direct_queue: Arc<SegQueue<String>>,
    size_counter: AtomicUsize,
}

impl RegularTransactionProcessor {
    fn new(hotstuff_queue: Arc<SegQueue<String>>) -> Self {
        Self {
            direct_queue: hotstuff_queue,
            size_counter: AtomicUsize::new(0),
        }
    }

    fn push_transaction(&self, transaction: String) {
        let tx_clone = transaction.clone();
        self.direct_queue.push(transaction);
        self.size_counter.fetch_add(1, Ordering::Relaxed);
        info!("Tx {} pushed to HotStuff queue.", tx_clone);
        // Limit queue size to prevent memory issues
        if self.get_queue_size() > 100000 {
            let _ = self.direct_queue.pop(); // Remove oldest transaction
        }
    }

    fn get_queue_size(&self) -> usize {
        // Approximate queue size - not perfect but sufficient for monitoring
        self.direct_queue.len()
    }
}

fn setup_tracing_logger(node_id: usize) {
    create_dir_all("logs").expect("Cannot create logs directory");
    let _ = fs::remove_file(format!("logs/node{}.log", node_id));

    let node_log_file = File::options()
        .create(true)
        .append(true)
        .open(format!("logs/node{}.log", node_id))
        .expect("Cannot open node log file");

    let result = tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")))
        .with(
            fmt::layer()
                .with_writer(std::io::stdout)
                .with_target(true)
                .with_thread_ids(true)
                .with_ansi(true),
        )
        .with(
            fmt::layer()
                .with_writer(node_log_file)
                .with_target(true)
                .with_thread_ids(true)
                .with_ansi(false),
        )
        .try_init();

    match result {
        Ok(_) => info!("Logging system initialized successfully"),
        Err(_) => warn!("Logging system already initialized, skipping"),
    }
}

fn create_peer_address(i: usize) -> Result<SocketAddr, String> {
    let hostname = format!("node{}", i);
    let port = 10000 + i as u16;
    let addr_str = format!("{}:{}", hostname, port);

    info!("Trying to resolve address: {}", addr_str);

    match std::net::ToSocketAddrs::to_socket_addrs(&addr_str) {
        Ok(mut addrs) => {
            if let Some(addr) = addrs.next() {
                info!("Successfully resolved address: {} -> {}", addr_str, addr);
                Ok(addr)
            } else {
                Err(format!("No address found: {}", addr_str))
            }
        }
        Err(e) => {
            warn!("DNS resolution failed {}: {}", addr_str, e);
            let fallback_addr = format!("127.0.0.1:{}", port);
            info!("Trying fallback address: {}", fallback_addr);
            fallback_addr
                .parse::<SocketAddr>()
                .map_err(|e| format!("Fallback address parsing failed: {}", e))
        }
    }
}

// Completely lock-free client listener
async fn start_lockfree_client_listener(
    node_id: usize,
    port: u16,
    event_tx: broadcast::Sender<SystemEvent>,
    regular_tx_processor: Arc<RegularTransactionProcessor>,
    lockfree_stats: Arc<LockFreeStats>,
    node_stats: Arc<PerformanceStats>,
    event_for_response_tx: broadcast::Sender<SystemEvent>,
    pompe_manager: Option<Arc<PompeManager>>,
    smrol_manager: Option<Arc<SmrolManager>>,
) -> Result<(), String> {
    let addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&addr)
        .await
        .map_err(|e| format!("Bind failed: {}", e))?;

    info!(
        "[Lock-free] Node {} listening for client connections: {}",
        node_id, addr
    );

    loop {
        match listener.accept().await {
            Ok((socket, _client_addr)) => {
                // do nothing
            }
            Err(e) => {
                error!("Node {} accepting client connection failed: {}", node_id, e);
            }
        }
    }
    Ok(())
}

// // Lock-free connection handling
// async fn handle_lockfree_client_connection(
//     node_id: usize,
//     socket:  TcpStream,
//     event_tx: broadcast::Sender<SystemEvent>,
//     regular_tx_processor: Arc<RegularTransactionProcessor>,
//     lockfree_stats: Arc<LockFreeStats>,
//     node_stats: Arc<PerformanceStats>,
//     mut event_for_response_rx: broadcast::Receiver<SystemEvent>,
//     pompe_manager: Option<Arc<PompeManager>>,
// ) -> Result<(), String> {
//     let mut length_buf = [0u8; 4];
//     let mut connection_tx_count = 0;

//     info!("[Lock-free] Node {} new client connection established", node_id);
//     let (mut read_half, mut write_half) = socket.into_split();

//     // 响应发送任务
//     let write_task = tokio::spawn(async move {
//         let mut response_count = 0;

//         while let Ok(response_cmd) = event_for_response_rx.recv().await {
//             let response_json = match response_cmd {
//                 SystemEvent::PompeOrdering1Completed { tx_id } => {
//                     let tx_ids = vec![tx_id];
//                     response_count += tx_ids.len();
//                     serde_json::json!({
//                         "message_type": "pompe_ordering1_response",
//                         "tx_ids": tx_ids,
//                         "node_id": node_id
//                     })
//                 }
//                 SystemEvent::HotStuffCommitted { block_height, tx_ids } => {
//                     info!("[Event received] Node {} HotStuffCommitted: block_height={}, tx_ids={:?}", node_id, block_height, tx_ids);
//                     response_count += tx_ids.len();
//                     serde_json::json!({
//                         "message_type": "consensus_response",
//                         "tx_ids": tx_ids,
//                         "node_id": node_id
//                     })
//                 }
//                 // SystemEvent::Error { tx_ids, error_msg } => {
//                 //     serde_json::json!({
//                 //         "message_type": "error_response",
//                 //         "tx_ids": tx_ids,
//                 //         "error_msg": error_msg,
//                 //         "node_id": node_id
//                 //     })
//                 // }
//                 _ => {
//                     // For all other variants, skip sending a response and continue the loop
//                     continue;
//                 }
//             };

//             let serialized = serde_json::to_vec(&response_json).unwrap();
//             let message_length = serialized.len() as u32;

//             if write_half.write_all(&message_length.to_be_bytes()).await.is_err() ||
//                write_half.write_all(&serialized).await.is_err() ||
//                write_half.flush().await.is_err() {
//                 error!("Node {} 响应发送失败", node_id);
//                 break;
//             }
//             debug!("***** Node {} 向客户端发送响应: {:?} tx_id:{:?}", node_id, response_json.get("message_type"), response_json.get("tx_ids"));
//             // 🔥 减少日志频率
//             if response_count % 50 == 0 {
//                 info!("Node {} 已发送 {} 个响应", node_id, response_count);
//             }
//         }
//     });

//     loop {
//         match read_half.read_exact(&mut length_buf).await {
//             Ok(_) => {
//                 let message_length = u32::from_be_bytes(length_buf) as usize;

//                 if message_length > 1024 * 1024 {
//                     warn!("Node {} message too large: {}, disconnecting", node_id, message_length);
//                     break;
//                 }

//                 let mut message_buf = vec![0u8; message_length];
//                 read_half.read_exact(&mut message_buf).await.map_err(|e| format!("Reading message failed: {}", e))?;

//                 if let Ok(client_message) = serde_json::from_slice::<ClientMessage>(&message_buf) {
//                     if let Some(transaction) = client_message.transaction {
//                         connection_tx_count += 1;

//                         // Identify tx types
//                         // let is_pompe = client_message.message_type == "pompe_transaction";
//                         let is_smrol = client_message.message_type == "smrol_transaction";
//                         let is_pompe=true;

//                         // Lock-free statistics update
//                         let total_received = lockfree_stats.increment_tx_received();
//                         if is_pompe {
//                             lockfree_stats.increment_pompe_tx();
//                         }

//                         // info!("Node {} received {} ID={}, {}->{}:{}",
//                         //       node_id,
//                         //       if is_pompe { "Pompe transaction" } else if is_smrol { "SMROL transaction" } else { "standard transaction" },
//                         //       transaction.id, transaction.from, transaction.to, transaction.amount);

//                         // Process transaction with lock-free processor
//                         let tx_string = format!("{}:{}->{}:{}", transaction.id, transaction.from, transaction.to, transaction.amount);

//                         if is_pompe {
//                             if let Some(ref pompe) = pompe_manager {
//                                 match pompe.process_raw_transaction(&tx_string).await {
//                                     Ok(_) => {
//                                         info!("[Lock-free] Node {} Pompe transaction processed directly: {}", node_id, tx_string);
//                                     }
//                                     Err(e) => {
//                                         error!("Pompe 交易处理失败: {}, 错误: {}", tx_string, e);
//                                     }
//                                 }
//                             } else {
//                                 warn!("Pompe 管理器未启用，跳过交易: {}", tx_string);
//                             }
//                             // Pompe transactions go through Pompe processor
//                             // info!("[Lock-free] Node {} Pompe transaction queued: {}", node_id, tx_string);
//                         } else if is_smrol {
//                             // TODO: Add SMROL transaction processing logic here
//                         } else {
//                             // Regular transactions go directly to HotStuff queue (following second code logic)
//                             regular_tx_processor.push_transaction(tx_string.clone());

//                             // Update performance statistics for regular transactions
//                             node_stats.record_submitted();

//                             // Log regular transaction processing with queue status
//                             let queue_size = regular_tx_processor.get_queue_size();
//                             info!("[Lock-free] Node {} Regular transaction queued: {}, queue size: {}",
//                                   node_id, tx_string, queue_size);

//                             // Queue size management (following second code logic)
//                             if queue_size > 100000 {
//                                 warn!("Node {} Regular transaction queue overflow: {} transactions",
//                                       node_id, queue_size);
//                             }

//                             // Every 100 regular transactions, show statistics
//                             if connection_tx_count % 100 == 0 && !is_pompe {
//                                 let current_tps = node_stats.get_submission_tps();
//                                 info!("Node {} Regular tx stats: {} received, Queue: {}, TPS: {:.1}",
//                                       node_id, connection_tx_count, queue_size, current_tps);
//                             }
//                         }

//                         // Send event notification (non-blocking)
//                         let _ = event_tx.send(SystemEvent::TransactionReceived {
//                             transaction: transaction.clone(),
//                             is_pompe,
//                         });

//                         // Periodic reporting (lock-free check)
//                         if total_received % 10 == 0 {
//                             let (total_rx, pompe_rx, hotstuff_size, pompe_size, hotstuff_consumed) = lockfree_stats.get_stats();
//                             let regular_queue_size = regular_tx_processor.get_queue_size();

//                             info!("[Lock-free] Node {} reception stats: {} transactions (Pompe: {}, Regular: {}), queue status: Pompe={}, HotStuff={}, Regular={}, consumed={}",
//                                   node_id, total_rx, pompe_rx, total_rx - pompe_rx, pompe_size, hotstuff_size, regular_queue_size, hotstuff_consumed);
//                         }
//                     }
//                 }
//                 else {
//                     error!("Node {} JSON parsing failed, message length: {}", node_id, message_length);
//                 }
//             }
//             Err(e) => {
//                 if connection_tx_count > 0 {
//                     let final_regular_queue_size = regular_tx_processor.get_queue_size();
//                     info!("[Lock-free] Node {} client disconnected ({}), received {} transactions this session, final regular queue: {}",
//                           node_id, e, connection_tx_count, final_regular_queue_size);
//                 } else {
//                     info!("[Lock-free] Node {} client disconnected ({}), no transactions received", node_id, e);
//                 }
//                 break;
//             }
//         }
//     }

//     Ok(())
// }

// Lock-free performance monitor
async fn start_lockfree_performance_monitor(
    node_id: usize,
    mut event_rx: broadcast::Receiver<SystemEvent>,
    node_stats: Arc<PerformanceStats>,
    pompe_manager: Option<Arc<PompeManager>>,
    lockfree_stats: Arc<LockFreeStats>,
) {
    info!("[Lock-free] Node {} performance monitor started", node_id);

    // Periodic reporting task
    tokio::spawn({
        let node_stats_clone = Arc::clone(&node_stats);
        let lockfree_stats_clone = Arc::clone(&lockfree_stats);
        let pompe_manager_clone = pompe_manager.clone();

        async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));

            loop {
                interval.tick().await;

                // Get statistics
                let submission_tps = node_stats_clone.get_submission_tps();
                let end_to_end_tps = node_stats_clone.get_end_to_end_tps();
                let pure_consensus_tps = node_stats_clone.get_pure_consensus_tps();
                let (total_rx, pompe_rx, hotstuff_size, pompe_size, hotstuff_consumed) =
                    lockfree_stats_clone.get_stats();

                info!("[Lock-free performance monitor] =========================");
                info!("  Submission TPS: {:.2}", submission_tps);
                info!("  End-to-end TPS: {:.2}", end_to_end_tps);
                info!("  Pure consensus TPS: {:.2}", pure_consensus_tps);
                info!("  Total received: {} (Pompe: {})", total_rx, pompe_rx);
                info!("  HotStuff queue: {}", hotstuff_size);
                info!("  Pompe queue: {}", pompe_size);
                info!("  HotStuff consumed: {}", hotstuff_consumed);

                // Bottleneck analysis and performance diagnosis
                if pompe_size > 10 && hotstuff_size == 0 {
                    warn!("Pompe processing bottleneck detected:");
                    warn!("   - Pompe queue backlog: {} transactions", pompe_size);
                    warn!("   - HotStuff queue idle: {} transactions", hotstuff_size);
                    warn!("   - Possible cause: Pompe BFT consensus slow, need to optimize network or increase batch size");

                    if pompe_size > 50 {
                        warn!("   - Suggestion: Consider increasing Pompe batch size or reducing processing interval");
                    }
                } else if hotstuff_size > 30 && pure_consensus_tps < submission_tps * 0.3 {
                    warn!("HotStuff processing bottleneck detected:");
                    warn!(
                        "   - HotStuff queue backlog: {} transactions",
                        hotstuff_size
                    );
                    warn!(
                        "   - Confirmation TPS ({:.1}) much lower than submission TPS ({:.1})",
                        pure_consensus_tps, submission_tps
                    );
                    warn!("   - Possible cause: HotStuff consensus network delay or block size limitation");
                } else if pompe_size < 5
                    && hotstuff_size < 5
                    && submission_tps > 10.0
                    && pure_consensus_tps < 5.0
                {
                    warn!("Overall processing delay detected:");
                    warn!("   - Both queues are relatively empty but TPS is low: submission({:.1}) vs confirmation({:.1})", submission_tps, pure_consensus_tps);
                    warn!("   - Possible cause: Network delay, long block packing intervals, or high validation overhead");
                } else if total_rx > 100 {
                    let processing_efficiency =
                        (hotstuff_consumed as f64 / total_rx as f64) * 100.0;
                    if processing_efficiency > 90.0 {
                        info!(
                            "System running well - processing efficiency: {:.1}%",
                            processing_efficiency
                        );
                    } else if processing_efficiency < 70.0 {
                        warn!(
                            "Low processing efficiency: {:.1}% - system may need tuning",
                            processing_efficiency
                        );
                    } else {
                        info!(
                            "Normal processing efficiency: {:.1}%",
                            processing_efficiency
                        );
                    }
                }

                // Connection pool monitoring
                if let Some(ref pompe) = pompe_manager_clone {
                    match pompe.get_network_stats().await {
                        Some(stats) => {
                            let (connections, messages) = stats;
                            info!(
                                "  Active connections: {}, total messages: {}",
                                connections, messages
                            );

                            if connections > 0 {
                                let avg = messages as f64 / connections as f64;
                                if avg > 50.0 {
                                    info!("  Connection reuse effective ({:.1} msg/conn)", avg);
                                } else {
                                    warn!("  Connection reuse ineffective ({:.1} msg/conn)", avg);
                                }
                            }
                        }
                        None => {
                            debug!("  Network stats not available");
                        }
                    }

                    // Pompe status monitoring
                    let (
                        batch_count,
                        ordering1_count,
                        commit_count,
                        consensus_ready,
                        _,
                        tx_store_count,
                        initiator_count,
                    ) = pompe.get_detailed_stats();

                    if batch_count > 0 || ordering1_count > 0 || commit_count > 0 {
                        info!("  Pompe status: batch={}, o1={}, commit={}, ready={}, store={}, init={}", 
                            batch_count, ordering1_count, commit_count, consensus_ready, tx_store_count, initiator_count);
                    }
                }

                info!("[Lock-free performance monitor] =========================");
            }
        }
    });

    // Event response loop
    loop {
        match event_rx.recv().await {
            Ok(event) => {
                match event {
                    SystemEvent::TransactionReceived {
                        transaction: _,
                        is_pompe,
                    } => {
                        if is_pompe {
                            // Record submission in performance stats
                            node_stats.record_submitted();
                        }
                    }

                    SystemEvent::HotStuffConsumed { count } => {
                        info!(
                            "[Lock-free event response] HotStuff consumed: {} transactions",
                            count
                        );
                    }

                    SystemEvent::NetworkStatsUpdate {
                        connections,
                        messages,
                    } => {
                        info!("[Lock-free event response] Network status: {} connections, {} messages", connections, messages);
                    }

                    SystemEvent::PerformanceUpdate {
                        submission_tps,
                        consensus_tps,
                        pompe_tps,
                    } => {
                        info!("[Lock-free event response] Performance update: submission={:.2}, confirmation={:.2}, Pompe={:.2}", 
                              submission_tps, consensus_tps, pompe_tps);
                    }

                    _ => {
                        // Handle other event types
                    }
                }
            }
            Err(e) => {
                error!("[Lock-free event response] Event reception failed: {}", e);
            }
        }
    }
}

// Adversarial transaction generator, calls Ordering1 continuously
async fn adversary(pompe_manager: Option<Arc<PompeManager>>) {
    if let Some(pompe) = pompe_manager {
        let mut tx_generator = TransactionGenerator::new();
        loop {
            let transaction = tx_generator.generate_transaction();
            let tx_string = format!(
                "{}:{}->{}:{}",
                transaction.id, transaction.from, transaction.to, transaction.amount
            );
            match pompe.process_raw_transaction(&tx_string).await {
                Ok(_) => {
                    warn!("😈 [Adversary] flooding transactions: {}", tx_string);
                }
                Err(e) => {
                    error!("Pompe 交易处理失败: {}, 错误: {}", tx_string, e);
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let node_id: usize = env::var("NODE_ID")
        .unwrap_or_else(|_| "9".to_string())
        .parse()
        .expect("NODE_ID must be a number");

    let my_port: u16 = env::var("NODE_PORT")
        .unwrap_or_else(|_| (10000 + node_id).to_string())
        .parse()
        .expect("NODE_PORT must be a number");

    let node_least_id: usize = env::var("NODE_LEAST_ID")
        .unwrap_or_else(|_| "1".to_string())
        .parse()
        .expect("NODE_LEAST_ID must be a number");
    let node_num: usize = env::var("NODE_NUM")
        .unwrap_or_else(|_| "4".to_string())
        .parse()
        .expect("NODE_NUM must be a number");

    setup_tracing_logger(node_id);
    info!(
        "[Completely lock-free] Starting Docker node {} (port: {})",
        node_id, my_port
    );

    let secret_bytes: [u8; 32] = [(node_id + 1) as u8; 32];
    let signing_key = SigningKey::from_bytes(&secret_bytes);
    let my_verifying_key = VerifyingKey::from(signing_key.verifying_key());

    info!("Node key: {:?}", my_verifying_key.to_bytes()[0..8].to_vec());

    // Create peer node configuration
    let mut peer_addrs = HashMap::new();
    let mut all_verifying_keys = Vec::new();

    for i in node_least_id..=(node_least_id + node_num - 1) {
        let peer_secret: [u8; 32] = [(i + 1) as u8; 32];
        let peer_signing_key = SigningKey::from_bytes(&peer_secret);
        let peer_verifying_key = VerifyingKey::from(peer_signing_key.verifying_key());

        let addr =
            create_peer_address(i).map_err(|e| format!("Cannot create peer address: {}", e))?;

        peer_addrs.insert(peer_verifying_key, addr);
        all_verifying_keys.push(peer_verifying_key);

        info!(
            "Node {} peer_verifying_key: {:?} -> {}",
            i,
            peer_verifying_key.to_bytes()[0..4].to_vec(),
            addr
        );
    }

    if !peer_addrs.contains_key(&my_verifying_key) {
        error!("Current node {} not in peer_addrs!", node_id);
        return Err("Node configuration error".to_string());
    }

    info!("Validator set: {} validators", all_verifying_keys.len());

    let init_app_state_updates = AppStateUpdates::new();
    let mut init_validator_set_updates = ValidatorSetUpdates::new();
    for key in &all_verifying_keys {
        init_validator_set_updates.insert(*key, Power::new(1));
    }

    let my_addr: SocketAddr = format!("0.0.0.0:{}", my_port)
        .parse()
        .expect("Invalid local address");

    let tcp_config = TokioNetworkConfig {
        my_addr,
        peer_addrs,
        my_key: my_verifying_key,
    };

    info!("Creating Tokio TCP network...");
    let tcp_network = match TokioNetwork::new(tcp_config) {
        Ok(network) => network,
        Err(e) => {
            error!("Tokio network creation failed: {}", e);
            return Err(format!("Tokio network creation failed: {}", e));
        }
    };

    info!("Tokio network created successfully");

    // Completely lock-free event-driven architecture
    let (event_tx, event_rx) = broadcast::channel::<SystemEvent>(1000);
    let (event_for_response_tx, event_for_response_rx) = broadcast::channel::<SystemEvent>(1000);

    let event_for_response_rx_clone = event_for_response_rx;

    // Use the lock-free transaction queue from Node for regular transactions
    let shared_tx_queue = Arc::new(SegQueue::new());
    let regular_tx_processor = Arc::new(RegularTransactionProcessor::new(shared_tx_queue.clone()));

    // Performance stats
    let node_stats = Arc::new(PerformanceStats::new());
    let lockfree_stats = Arc::new(LockFreeStats::new());

    // Start completely lock-free client listener with dual processors
    let client_listener_node_id = node_id;
    let client_listener_port = my_port - 1000;
    let event_tx_clone = event_tx.clone();

    let regular_tx_processor_clone = Arc::clone(&regular_tx_processor);
    let lockfree_stats_clone = Arc::clone(&lockfree_stats);
    let node_stats_clone = Arc::clone(&node_stats);
    let event_for_response_tx_clone = event_for_response_tx.clone();

    info!("Waiting for other nodes to start...");
    tokio::time::sleep(Duration::from_secs(15)).await;

    info!("Creating HotStuff node...");

    // Create Node with lock-free queue integration
    // replica
    let node = Node::new(
        node_id,
        signing_key.clone(),
        tcp_network.clone(),
        init_app_state_updates.clone(),
        init_validator_set_updates.clone(),
        shared_tx_queue.clone(),
        node_stats.clone(),
        event_for_response_tx.clone(), /* 🎯 */
    );

    // Add monitoring for regular transaction queue size (similar to second code)
    let regular_tx_monitor = Arc::clone(&regular_tx_processor);
    let stats_for_monitor = Arc::clone(&node_stats);
    let lockfree_stats_for_monitor = Arc::clone(&lockfree_stats);

    // Create Pompe manager
    // pompe manager
    let pompe_config = load_pompe_config();
    let pompe_manager = if pompe_config.enable {
        info!(
            "Enabling completely lock-free Pompe BFT, batch size: {}",
            pompe_config.batch_size
        );

        let all_node_ids: Vec<usize> = (node_least_id..=(node_least_id + node_num - 1)).collect();
        info!("Pompe network node list: {:?}", all_node_ids);

        let mut pompe = PompeManager::new_with_complete_network(
            node_id,
            all_node_ids,
            pompe_config,
            tcp_network.clone(),
            event_for_response_tx.clone(), /* pompe event sender 🎯 */
        );

        // Use connected mode lock-free adapter
        let mut lockfree_adapter = LockFreeHotStuffAdapter::new();
        lockfree_adapter.connect_to_queue(shared_tx_queue.clone());
        let lockfree_adapter = Arc::new(lockfree_adapter);
        pompe.set_lockfree_adapter(Arc::clone(&lockfree_adapter));

        let pompe_arc = Arc::new(pompe);
        let pompe_clone = Arc::clone(&pompe_arc);

        // Start Pompe network loop
        tokio::spawn(async move {
            if let Err(e) = pompe_clone.start_network_message_loop().await {
                error!(
                    "Completely lock-free Pompe network loop startup failed: {}",
                    e
                );
            }
        });

        Some(pompe_arc)
    } else {
        info!("Pompe BFT disabled");
        None
    };

    // TODO: create smrol manager here
    // TODO: pass smrol manager to client listener

    // Pass Pompe manager to client listener
    let pompe_manager_clone = pompe_manager.clone();
    let pompe_manager_clone2 = pompe_manager.clone();

    tokio::spawn(async move {
        if let Err(e) = start_lockfree_client_listener(
            client_listener_node_id,
            client_listener_port,
            event_tx_clone,
            regular_tx_processor_clone,
            lockfree_stats_clone,
            node_stats_clone,
            event_for_response_tx_clone,
            pompe_manager_clone,
            None,
        )
        .await
        {
            error!("Completely lock-free client listener failed: {}", e);
        }
    });

    //   **** Run adversary function continuously rather than listening to the client
    // tokio::spawn(async move {
    adversary(pompe_manager_clone2).await;
    //    });

    let lockfree_stats_clone = Arc::clone(&lockfree_stats);
    tokio::spawn(start_lockfree_performance_monitor(
        node_id,
        event_rx,
        node_stats.clone(),
        pompe_manager.clone(),
        lockfree_stats_clone,
    ));

    tokio::time::sleep(Duration::from_secs(5)).await;
    info!("网络连通性测试:");
    let connectivity_timeout = Duration::from_secs(2);
    for target_id in node_least_id..(node_least_id + node_num) {
        let addr = format!("node{}:{}", target_id, 20000 + target_id);
        match tokio::time::timeout(connectivity_timeout, tokio::net::TcpStream::connect(&addr))
            .await
        {
            Ok(Ok(_)) => info!("节点 {} 端口 {} 可达", target_id, 20000 + target_id),
            Ok(Err(e)) => warn!(
                "节点 {} 端口 {} 暂不可达: {}",
                target_id,
                20000 + target_id,
                e
            ),
            Err(_) => warn!(
                "节点 {} 端口 {} 连接超时 (>{:?})",
                target_id,
                20000 + target_id,
                connectivity_timeout
            ),
        }
    }

    tokio::spawn(async move {
        info!("[Regular tx monitor] Regular transaction monitor started");
        let mut monitor_interval = tokio::time::interval(Duration::from_secs(5));
        let mut last_queue_size = 0;
        let mut loop_counter = 0;

        loop {
            monitor_interval.tick().await;
            loop_counter += 1;

            let current_queue_size = regular_tx_monitor.get_queue_size();
            let submission_tps = stats_for_monitor.get_submission_tps();
            let consensus_tps = stats_for_monitor.get_end_to_end_tps();
            let total_confirmed_txs = stats_for_monitor.get_confirmed_transactions();
            let total_confirmed_blocks = stats_for_monitor.get_confirmed_blocks();

            // Update lockfree stats with regular transaction queue size
            lockfree_stats_for_monitor.update_hotstuff_queue_size(current_queue_size);

            // Queue size change notification (following second code logic)
            if current_queue_size != last_queue_size {
                if current_queue_size > last_queue_size {
                    info!(
                        "Node {} Regular queue increased: {} -> {} (+{})",
                        node_id,
                        last_queue_size,
                        current_queue_size,
                        current_queue_size - last_queue_size
                    );
                } else {
                    info!(
                        "Node {} Regular queue decreased: {} -> {} (-{})",
                        node_id,
                        last_queue_size,
                        current_queue_size,
                        last_queue_size - current_queue_size
                    );
                }
                last_queue_size = current_queue_size;
            }

            // Check for queue backlog (following second code logic)
            if current_queue_size > 1000 {
                warn!(
                    "Node {} Regular transaction queue backlog: {} transactions",
                    node_id, current_queue_size
                );
            }

            // Periodic detailed monitoring (every 10 cycles = 50 seconds)
            if loop_counter % 10 == 0 {
                info!("[Regular tx monitor] Node {} status report:", node_id);
                info!("  Regular queue size: {}", current_queue_size);
                info!("  Submission TPS: {:.2}", submission_tps);
                info!("  Consensus TPS: {:.2}", consensus_tps);
                info!("  Total confirmed transactions: {}", total_confirmed_txs);
                info!("  Total confirmed blocks: {}", total_confirmed_blocks);

                // Performance analysis
                if submission_tps > 0.0 && consensus_tps > 0.0 {
                    let efficiency = (consensus_tps / submission_tps) * 100.0;
                    if efficiency > 90.0 {
                        info!("  Processing efficiency: {:.1}% - Excellent", efficiency);
                    } else if efficiency > 70.0 {
                        info!("  Processing efficiency: {:.1}% - Good", efficiency);
                    } else {
                        warn!(
                            "  Processing efficiency: {:.1}% - Needs attention",
                            efficiency
                        );
                    }
                }
            }
        }
    });

    info!(
        "[Dual-path lock-free] Node {} all components started, entering dual-path event loop",
        node_id
    );

    info!("Node {} 所有组件启动完成", node_id);
    tokio::signal::ctrl_c()
        .await
        .map_err(|e| format!("信号处理失败: {}", e))?;
    info!("Node {} 收到退出信号，正常关闭", node_id);

    Ok(())
}
