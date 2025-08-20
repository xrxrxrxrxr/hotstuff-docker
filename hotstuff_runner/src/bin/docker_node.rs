// hotstuff_runner/src/bin/docker_node.rs
//! Completely lock-free event-driven Docker node implementation

use hotstuff_rs::{
    types::{
        crypto_primitives::VerifyingKey,
        data_types::Power,
        update_sets::{AppStateUpdates, ValidatorSetUpdates},
    },
    replica::ReplicaSpec,
};
use hotstuff_runner::{
    tcp_node::Node,
    tcp_network::{TcpNetworkConfig, TcpNetwork},
    app::TestApp,
    stats::PerformanceStats,
    pompe::{PompeManager, load_pompe_config, LockFreeHotStuffAdapter},
    lockfree_types::LockFreePerformanceStats
};
use std::sync::Arc;
use tokio::sync::{mpsc, broadcast, Notify};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::env;
use std::fs;
use std::fs::{File, create_dir_all};
use tracing::{info, error, warn, debug};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt};
use std::time::Duration;
use ed25519_dalek::SigningKey;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use serde::{Serialize, Deserialize};
use std::sync::atomic::{AtomicUsize, AtomicU64, Ordering};
use crossbeam::queue::SegQueue;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TestTransaction {
    pub id: u64,
    pub from: String,
    pub to: String,
    pub amount: u64,
    pub timestamp: u64,
    pub nonce: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ClientMessage {
    pub message_type: String,
    pub transaction: Option<TestTransaction>,
    pub client_id: String,
}

// Completely lock-free event system
#[derive(Debug, Clone)]
pub enum SystemEvent {
    TransactionReceived {
        transaction: TestTransaction,
        is_pompe: bool,
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

// Lock-free HotStuff queue
#[derive(Debug)]
pub struct LockFreeHotStuffQueue {
    pub queue: Arc<SegQueue<String>>,
    size_counter: AtomicUsize,
    notify: Arc<Notify>,
}

impl LockFreeHotStuffQueue {
    fn new() -> Self {
        Self {
            queue: Arc::new(SegQueue::new()),
            size_counter: AtomicUsize::new(0),
            notify: Arc::new(Notify::new()),
        }
    }
    
    fn push(&self, item: String) {
        self.queue.push(item);
        let new_size = self.size_counter.fetch_add(1, Ordering::Relaxed) + 1;
        self.notify.notify_one();
        
        debug!("Lock-free queue: added transaction, new size: {}", new_size);
    }
    
    fn pop(&self) -> Option<String> {
        if let Some(item) = self.queue.pop() {
            let new_size = self.size_counter.fetch_sub(1, Ordering::Relaxed) - 1;
            debug!("Lock-free queue: consumed transaction, new size: {}", new_size);
            Some(item)
        } else {
            None
        }
    }
    
    fn len(&self) -> usize {
        self.size_counter.load(Ordering::Relaxed)
    }
    
    fn get_notify(&self) -> Arc<Notify> {
        Arc::clone(&self.notify)
    }
    
    fn drain_batch(&self, max_size: usize) -> Vec<String> {
        let mut batch = Vec::new();
        
        for _ in 0..max_size {
            if let Some(item) = self.pop() {
                batch.push(item);
            } else {
                break;
            }
        }
        
        batch
    }
}

// Lock-free adapter for compatibility
pub struct CompatibleLockFreeQueue;

impl CompatibleLockFreeQueue {
    fn create_empty_mutex() -> Arc<std::sync::Mutex<Vec<String>>> {
        // Return empty mutex wrapper - actual operations use lock-free queues
        Arc::new(std::sync::Mutex::new(Vec::new()))
    }
}

// Lock-free transaction processing using crossbeam queues
pub struct LockFreeTransactionProcessor {
    input_queue: Arc<SegQueue<String>>,
    processing_queue: Arc<SegQueue<String>>,
    size_counter: AtomicUsize,
}

impl LockFreeTransactionProcessor {
    fn new() -> Self {
        Self {
            input_queue: Arc::new(SegQueue::new()),
            processing_queue: Arc::new(SegQueue::new()),
            size_counter: AtomicUsize::new(0),
        }
    }
    
    fn push_input(&self, transaction: String) {
        self.input_queue.push(transaction);
        self.size_counter.fetch_add(1, Ordering::Relaxed);
    }
    
    fn process_batch(&self, batch_size: usize) -> Vec<String> {
        let mut batch = Vec::with_capacity(batch_size);
        
        for _ in 0..batch_size {
            if let Some(tx) = self.input_queue.pop() {
                self.size_counter.fetch_sub(1, Ordering::Relaxed);
                batch.push(tx);
            } else {
                break;
            }
        }
        
        batch
    }
    
    fn get_queue_size(&self) -> usize {
        self.size_counter.load(Ordering::Relaxed)
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
        .with(
            fmt::layer()
                .with_writer(std::io::stdout)
                .with_target(true)
                .with_thread_ids(true)
                .with_ansi(true)
        )
        .with(
            fmt::layer()
                .with_writer(node_log_file)
                .with_target(true)
                .with_thread_ids(true)
                .with_ansi(false)
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
            fallback_addr.parse::<SocketAddr>()
                .map_err(|e| format!("Fallback address parsing failed: {}", e))
        }
    }
}

// Completely lock-free client listener
async fn start_lockfree_client_listener(
    node_id: usize, 
    port: u16, 
    event_tx: broadcast::Sender<SystemEvent>,
    tx_processor: Arc<LockFreeTransactionProcessor>,
    lockfree_stats: Arc<LockFreeStats>,
) -> Result<(), String> {
    let addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&addr).await.map_err(|e| format!("Bind failed: {}", e))?;
    
    info!("[Lock-free] Node {} listening for client connections: {}", node_id, addr);
    
    loop {
        match listener.accept().await {
            Ok((mut socket, _client_addr)) => {
                let event_tx_clone = event_tx.clone();
                let tx_processor_clone = Arc::clone(&tx_processor);
                let lockfree_stats_clone = Arc::clone(&lockfree_stats);
                
                tokio::spawn(async move {
                    if let Err(e) = handle_lockfree_client_connection(
                        node_id, 
                        &mut socket, 
                        event_tx_clone,
                        tx_processor_clone,
                        lockfree_stats_clone,
                    ).await {
                        error!("Node {} client connection handling failed: {}", node_id, e);
                    }
                });
            }
            Err(e) => {
                error!("Node {} accepting client connection failed: {}", node_id, e);
            }
        }
    }
}

// Lock-free connection handling
async fn handle_lockfree_client_connection(
    node_id: usize, 
    socket: &mut TcpStream,
    event_tx: broadcast::Sender<SystemEvent>,
    tx_processor: Arc<LockFreeTransactionProcessor>,
    lockfree_stats: Arc<LockFreeStats>,
) -> Result<(), String> {
    let mut length_buf = [0u8; 4];
    let mut connection_tx_count = 0;

    info!("[Lock-free] Node {} new client connection established", node_id);
    
    loop {
        match socket.read_exact(&mut length_buf).await {
            Ok(_) => {
                let message_length = u32::from_be_bytes(length_buf) as usize;
                
                if message_length > 1024 * 1024 {
                    warn!("Node {} message too large: {}, disconnecting", node_id, message_length);
                    break;
                }
                
                let mut message_buf = vec![0u8; message_length];
                socket.read_exact(&mut message_buf).await.map_err(|e| format!("Reading message failed: {}", e))?;
                
                if let Ok(client_message) = serde_json::from_slice::<ClientMessage>(&message_buf) {
                    if let Some(transaction) = client_message.transaction {
                        connection_tx_count += 1;
                        let is_pompe = client_message.message_type == "pompe_transaction";
                        
                        // Lock-free statistics update
                        let total_received = lockfree_stats.increment_tx_received();
                        if is_pompe {
                            lockfree_stats.increment_pompe_tx();
                        }

                        info!("Node {} received {} ID={}, {}->{}:{}", 
                              node_id, 
                              if is_pompe { "Pompe transaction" } else { "standard transaction" },
                              transaction.id, transaction.from, transaction.to, transaction.amount);

                        // Process transaction with lock-free processor
                        if is_pompe {
                            let tx_string = format!("{}:{}->{}:{}", transaction.id, transaction.from, transaction.to, transaction.amount);
                            
                            // Immediately push to lock-free processor
                            tx_processor.push_input(tx_string.clone());
                            info!("[Lock-free] Node {} Pompe transaction queued: {}", node_id, tx_string);
                        }
                        
                        // Send event notification (non-blocking)
                        let _ = event_tx.send(SystemEvent::TransactionReceived {
                            transaction: transaction.clone(),
                            is_pompe,
                        });

                        // Periodic reporting (lock-free check)
                        if total_received % 10 == 0 {
                            let (total_rx, pompe_rx, hotstuff_size, pompe_size, hotstuff_consumed) = lockfree_stats.get_stats();
                            info!("[Lock-free] Node {} reception stats: {} transactions (Pompe: {}), queue status: Pompe={}, HotStuff={}, consumed={}", 
                                  node_id, total_rx, pompe_rx, pompe_size, hotstuff_size, hotstuff_consumed);
                        }
                    }
                }
                else {
                    error!("Node {} JSON parsing failed, message length: {}", node_id, message_length);
                }
            }
            Err(e) => {
                if connection_tx_count > 0 {
                    info!("[Lock-free] Node {} client disconnected ({}), received {} transactions this session", 
                          node_id, e, connection_tx_count);
                } else {
                    info!("[Lock-free] Node {} client disconnected ({}), no transactions received", node_id, e);
                }
                break;
            }
        }
    }
    
    Ok(())
}

// Lock-free Pompe processor using crossbeam queues
async fn start_lockfree_pompe_processor(
    node_id: usize,
    tx_processor: Arc<LockFreeTransactionProcessor>,
    pompe_manager: Arc<PompeManager>,
    event_tx: broadcast::Sender<SystemEvent>,
    lockfree_stats: Arc<LockFreeStats>,
) {
    info!("[Lock-free] Node {} Pompe processor started", node_id);
    
    // Main processing loop: batch processing
    let batch_size = 10;
    let mut batch_interval = tokio::time::interval(Duration::from_millis(20));
    
    loop {
        batch_interval.tick().await;
        
        // Lock-free batch extraction
        let batch = tx_processor.process_batch(batch_size);
        
        if !batch.is_empty() {
            let queue_size = tx_processor.get_queue_size();
            lockfree_stats.update_pompe_queue_size(queue_size);
            
            let processed_count = process_lockfree_transaction_batch(&pompe_manager, batch).await;
            
            if processed_count > 0 {
                info!("[Lock-free] Node {} Pompe processed {} transactions", node_id, processed_count);
                
                let _ = event_tx.send(SystemEvent::TransactionProcessed {
                    count: processed_count,
                });
            }
        }
    }
}

// Lock-free batch processing function
async fn process_lockfree_transaction_batch(
    pompe_manager: &Arc<PompeManager>,
    transactions: Vec<String>,
) -> usize {
    let mut processed_count = 0;
    
    for transaction in transactions {
        if let Err(e) = pompe_manager.process_raw_transaction(&transaction).await {
            error!("[Lock-free] Pompe transaction processing failed: {}, error: {}", transaction, e);
        } else {
            processed_count += 1;
        }
    }
    
    processed_count
}

// Lock-free HotStuff queue monitor
async fn start_lockfree_hotstuff_monitor(
    node_id: usize,
    hotstuff_queue: Arc<LockFreeHotStuffQueue>,
    event_tx: broadcast::Sender<SystemEvent>,
    lockfree_stats: Arc<LockFreeStats>,
) {
    info!("[Lock-free] Node {} HotStuff queue monitor started", node_id);
    
    let notify = hotstuff_queue.get_notify();
    let mut last_size = 0;
    
    loop {
        // Wait for queue change notification
        notify.notified().await;
        
        let current_size = hotstuff_queue.len();
        lockfree_stats.update_hotstuff_queue_size(current_size);
        
        let change = current_size as i32 - last_size as i32;
        
        if change != 0 {
            if change > 0 {
                info!("[Lock-free] HotStuff queue increased: +{} (current: {}) - Pompe outputting", 
                    change, current_size);
            } else {
                let consumed = (-change) as usize;
                lockfree_stats.increment_hotstuff_consumed(consumed);
                
                info!("[Lock-free] HotStuff queue decreased: {} (current: {}) - HotStuff consuming!", 
                    change, current_size);
                
                let _ = event_tx.send(SystemEvent::HotStuffConsumed {
                    count: consumed,
                });
            }
        }
        
        last_size = current_size;
    }
}

// Lock-free performance monitor
async fn start_lockfree_performance_monitor(
    node_id: usize,
    mut event_rx: broadcast::Receiver<SystemEvent>,
    lockfree_node_stats: Arc<LockFreePerformanceStats>, // Use our lock-free stats
    pompe_manager: Option<Arc<PompeManager>>,
    lockfree_stats: Arc<LockFreeStats>,
) {
    info!("[Lock-free] Node {} performance monitor started", node_id);
    
    // Periodic reporting task
    tokio::spawn({
        let lockfree_node_stats_clone = Arc::clone(&lockfree_node_stats);
        let lockfree_stats_clone = Arc::clone(&lockfree_stats);
        let pompe_manager_clone = pompe_manager.clone();
        
        async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            
            loop {
                interval.tick().await;
                
                // Get lock-free statistics
                let submission_tps = lockfree_node_stats_clone.get_submission_tps();
                let consensus_tps = lockfree_node_stats_clone.get_confirmation_tps();
                let (total_rx, pompe_rx, hotstuff_size, pompe_size, hotstuff_consumed) = lockfree_stats_clone.get_stats();
                
                info!("[Lock-free performance monitor] =========================");
                info!("  Submission speed: {:.2} TPS", submission_tps);
                info!("  Confirmation speed: {:.2} TPS", consensus_tps);
                info!("  Total received: {} (Pompe: {})", total_rx, pompe_rx);
                info!("  HotStuff queue: {}", hotstuff_size);
                info!("  Pompe queue: {}", pompe_size);
                info!("  HotStuff consumed: {}", hotstuff_consumed);
                
                // Bottleneck analysis
                if hotstuff_size > 50 && consensus_tps < submission_tps * 0.5 {
                    warn!("[Bottleneck analysis] HotStuff consumption slow, severe queue backlog");
                } else if hotstuff_size < 10 && consensus_tps < 5.0 {
                    warn!("[Bottleneck analysis] Pompe output slow, HotStuff lacks transactions to process");
                } else {
                    info!("[Bottleneck analysis] System running normally");
                }
                
                // Connection pool monitoring
                if let Some(ref pompe) = pompe_manager_clone {
                    // Fix: Explicitly handle the network stats method
                    match pompe.get_network_stats().await {
                        Some(stats) => {
                            let (connections, messages) = stats;
                            info!("  Active connections: {}, total messages: {}", connections, messages);
                            
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
                            // Handle case where network stats are not available
                            debug!("  Network stats not available");
                        }
                    }
                    
                    // Pompe status monitoring
                    let (batch_count, ordering1_count, commit_count, consensus_ready, _, tx_store_count, initiator_count) = pompe.get_detailed_stats();
                    
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
                    SystemEvent::TransactionReceived { transaction: _, is_pompe } => {
                        if is_pompe {
                            // Record submission in lock-free stats
                            lockfree_node_stats.record_submitted();
                        }
                    }
                    
                    SystemEvent::HotStuffConsumed { count } => {
                        info!("[Lock-free event response] HotStuff consumed: {} transactions", count);
                    }
                    
                    SystemEvent::NetworkStatsUpdate { connections, messages } => {
                        info!("[Lock-free event response] Network status: {} connections, {} messages", connections, messages);
                    }
                    
                    SystemEvent::PerformanceUpdate { submission_tps, consensus_tps, pompe_tps } => {
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
    info!("[Completely lock-free] Starting Docker node {} (port: {})", node_id, my_port);
    
    let secret_bytes: [u8; 32] = [(node_id + 1) as u8; 32];
    let signing_key = SigningKey::from_bytes(&secret_bytes);
    let my_verifying_key = VerifyingKey::from(signing_key.verifying_key());
    
    info!("Node key: {:?}", my_verifying_key.to_bytes()[0..8].to_vec());

    // Create peer node configuration
    let mut peer_addrs = HashMap::new();
    let mut all_verifying_keys = Vec::new();

    for i in node_least_id..=(node_least_id+node_num-1) {
        let peer_secret: [u8; 32] = [(i + 1) as u8; 32];
        let peer_signing_key = SigningKey::from_bytes(&peer_secret);
        let peer_verifying_key = VerifyingKey::from(peer_signing_key.verifying_key());
       
       let addr = create_peer_address(i).map_err(|e| format!("Cannot create peer address: {}", e))?;
       
       peer_addrs.insert(peer_verifying_key, addr);
       all_verifying_keys.push(peer_verifying_key);
       
       info!("Node {}: {:?} -> {}", 
             i, 
             peer_verifying_key.to_bytes()[0..4].to_vec(), 
             addr);
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
   
   let tcp_config = TcpNetworkConfig {
       my_addr,
       peer_addrs,
       my_key: my_verifying_key,
   };
   
   info!("Creating TCP network...");
   let tcp_network = match TcpNetwork::new(tcp_config) {
       Ok(network) => network,
       Err(e) => {
           error!("TCP network creation failed: {}", e);
           return Err(format!("TCP network creation failed: {}", e));
       }
   };
   
   info!("TCP network created successfully");
   
   // Completely lock-free event-driven architecture
   let (event_tx, event_rx) = broadcast::channel::<SystemEvent>(1000);
   
   // Core innovation: completely lock-free HotStuff queue
   let lockfree_hotstuff_queue = Arc::new(LockFreeHotStuffQueue::new());
   
   // Lock-free transaction processor
   let tx_processor = Arc::new(LockFreeTransactionProcessor::new());
   
   // For compatibility with Node::new, create empty mutex wrapper (not actually used)
   let compatible_queue = CompatibleLockFreeQueue::create_empty_mutex();
   
   // Original performance stats (keep for compatibility)
   let original_performance_stats = Arc::new(std::sync::Mutex::new(PerformanceStats::new()));
   let lockfree_stats = Arc::new(LockFreeStats::new());
   
   // Start completely lock-free client listener
   let client_listener_node_id = node_id;
   let client_listener_port = my_port - 1000;
   let event_tx_clone = event_tx.clone();
   let tx_processor_clone = Arc::clone(&tx_processor);
   let lockfree_stats_clone = Arc::clone(&lockfree_stats);
   
   tokio::spawn(async move {
       if let Err(e) = start_lockfree_client_listener(
           client_listener_node_id,
           client_listener_port,
           event_tx_clone,
           tx_processor_clone,
           lockfree_stats_clone,
       ).await {
           error!("Lock-free client listener failed: {}", e);
       }
   });
   
   info!("Waiting for other nodes to start...");
   tokio::time::sleep(Duration::from_secs(10)).await;

   info!("Creating HotStuff node...");
   
   // Create Node with lock-free queue integration
   let node = Node::new(
       node_id,
       signing_key.clone(),
       tcp_network.clone(),
       init_app_state_updates.clone(),
       init_validator_set_updates.clone(),
       compatible_queue,
       original_performance_stats.clone(),
   );

   // Get lock-free references from the node
   let node_lockfree_queue = node.get_transaction_queue();
   let node_lockfree_stats = node.get_lockfree_stats();
   
   // Bridge transactions from external processor to Node's internal queue
   let bridge_queue = Arc::clone(&node_lockfree_queue);
   let bridge_tx_processor = Arc::clone(&tx_processor);
   
   tokio::spawn(async move {
       info!("[Lock-free bridge] Transaction bridge started");
       let mut bridge_interval = tokio::time::interval(Duration::from_millis(50));
       
       loop {
           bridge_interval.tick().await;
           
           // Move transactions from external processor to node's internal queue
           let transactions = bridge_tx_processor.process_batch(20);
           
           if !transactions.is_empty() {
               let tx_count = transactions.len();
               for tx in transactions {
                   bridge_queue.push(tx);
               }
               info!("[Lock-free bridge] Bridged {} transactions to Node", tx_count);
           }
       }
   });

   // Create completely lock-free HotStuff consumer
   let lockfree_hotstuff_queue_clone = Arc::clone(&lockfree_hotstuff_queue);
   let lockfree_stats_clone = Arc::clone(&lockfree_stats);
   
   tokio::spawn(async move {
       info!("[Lock-free consumer] HotStuff transaction consumer started");
       
       let mut consumption_interval = tokio::time::interval(Duration::from_millis(100));
       
       loop {
           consumption_interval.tick().await;
           
           // Lock-free batch consumption
           let batch = lockfree_hotstuff_queue_clone.drain_batch(20);
           
           if !batch.is_empty() {
               let consumed_count = batch.len();
               lockfree_stats_clone.increment_hotstuff_consumed(consumed_count);
               
               info!("[Lock-free consumer] HotStuff consumed {} transactions", consumed_count);
               
               // Add actual HotStuff processing logic here
               for (i, tx) in batch.iter().enumerate() {
                   if i < 3 { // Only show first 3
                       info!("  [{}] {}", i+1, 
                           if tx.len() > 60 { &tx[0..60] } else { tx });
                   }
               }
               
               if batch.len() > 3 {
                   info!("  ... and {} other transactions", batch.len() - 3);
               }
           }
       }
   });

   // Create completely lock-free Pompe manager
   let pompe_config = load_pompe_config();
   let pompe_manager = if pompe_config.enable {
       info!("Enabling completely lock-free Pompe BFT, batch size: {}", pompe_config.batch_size);

       let all_node_ids: Vec<usize> = (node_least_id..=(node_least_id+node_num-1)).collect();
       info!("Pompe network node list: {:?}", all_node_ids);

       let mut pompe = PompeManager::new_with_complete_network(
           node_id,
           all_node_ids,
           pompe_config,
           tcp_network.clone(),
       );

       // Use connected mode lock-free adapter
       let mut lockfree_adapter = LockFreeHotStuffAdapter::new();
       lockfree_adapter.connect_to_queue(lockfree_hotstuff_queue.queue.clone());
       let lockfree_adapter = Arc::new(lockfree_adapter);
       pompe.set_lockfree_adapter(Arc::clone(&lockfree_adapter));
       
       let pompe_arc = Arc::new(pompe);
       let pompe_clone = Arc::clone(&pompe_arc);
       
       // Start Pompe network loop
       tokio::spawn(async move {
           if let Err(e) = pompe_clone.start_network_message_loop().await {
               error!("Completely lock-free Pompe network loop startup failed: {}", e);
           }
       });
       
       // Periodic cleanup
       let pompe_for_cleanup = Arc::clone(&pompe_arc);
       tokio::spawn(async move {
           let mut cleanup_interval = tokio::time::interval(Duration::from_secs(30));
           
           loop {
               cleanup_interval.tick().await;
               
               pompe_for_cleanup.cleanup_expired_states();
               info!("[Periodic cleanup] Node {} executed Pompe state cleanup", node_id);
           }
       });
   
       Some(pompe_arc)
   } else {
       info!("Pompe BFT disabled");
       None
   };

   // Start completely lock-free Pompe processor
   if let Some(ref pompe) = pompe_manager {
       let pompe_clone = Arc::clone(pompe);
       let event_tx_clone = event_tx.clone();
       let lockfree_stats_clone = Arc::clone(&lockfree_stats);
       
       tokio::spawn(start_lockfree_pompe_processor(
           node_id,
           tx_processor,
           pompe_clone,
           event_tx_clone,
           lockfree_stats_clone,
       ));
   }

   // Start lock-free HotStuff queue monitor
   let lockfree_hotstuff_queue_clone = Arc::clone(&lockfree_hotstuff_queue);
   let event_tx_clone = event_tx.clone();
   let lockfree_stats_clone = Arc::clone(&lockfree_stats);
   tokio::spawn(start_lockfree_hotstuff_monitor(
       node_id,
       lockfree_hotstuff_queue_clone,
       event_tx_clone,
       lockfree_stats_clone,
   ));

   // Start lock-free performance monitor
   let pompe_manager_clone = pompe_manager.clone();
   let lockfree_stats_clone = Arc::clone(&lockfree_stats);
   let node_lockfree_stats_for_monitor = Arc::clone(&node_lockfree_stats);
   tokio::spawn(start_lockfree_performance_monitor(
       node_id,
       event_rx,
       node_lockfree_stats_for_monitor,
       pompe_manager_clone,
       lockfree_stats_clone,
   ));

   info!("[Completely lock-free] Node {} all components started, entering lock-free event loop", node_id);
   
   // Main loop: completely event-driven, no polling, lock-free
   let mut heartbeat_interval = tokio::time::interval(Duration::from_secs(30));
   
   loop {
       tokio::select! {
           // Heartbeat detection: ensure system is alive
           _ = heartbeat_interval.tick() => {
               let (total_rx, pompe_rx, hotstuff_size, pompe_size, hotstuff_consumed) = lockfree_stats.get_stats();
               
               info!("[Lock-free heartbeat] Node {} system running normally", node_id);
               info!("   Total received: {}, Pompe: {}", total_rx, pompe_rx);
               info!("   Queue status: HotStuff={}, Pompe={}, consumed={}", hotstuff_size, pompe_size, hotstuff_consumed);
               
               // Health check
               if let Some(ref pompe) = pompe_manager {
                   match pompe.get_network_stats().await {
                       Some(stats) => {
                           let (connections, messages) = stats;
                           info!("   Network status: {} connections, {} messages", connections, messages);
                           
                           if connections == 0 && messages == 0 {
                               warn!("[Health check] Network connection abnormal, may need restart");
                           }
                       }
                       None => {
                           debug!("   Network stats not available for health check");
                       }
                   }
                   
                   let (batch_count, ordering1_count, commit_count, consensus_ready, _, tx_store_count, _) = pompe.get_detailed_stats();
                   
                   if tx_store_count > 1000 || ordering1_count > 100 {
                       warn!("[Health check] Pompe status abnormal: store={}, ordering1={}", tx_store_count, ordering1_count);
                       
                       // Trigger cleanup
                       pompe.cleanup_expired_states();
                       info!("[Health check] Emergency cleanup executed");
                   }
                   
                   if commit_count > 500 {
                       warn!("[Health check] Commit set backlog: {}", commit_count);
                   }
               }
               
               // Lock-free queue health check
               if hotstuff_size > 100 {
                   warn!("[Health check] HotStuff queue severe backlog: {}", hotstuff_size);
               } else if pompe_size > 1000 {
                   warn!("[Health check] Pompe input queue severe backlog: {}", pompe_size);
               }
               
               // Throughput analysis
               if total_rx > 0 {
                   let processing_rate = (hotstuff_consumed as f64 / total_rx as f64) * 100.0;
                   info!("   Processing rate: {:.1}%", processing_rate);
                   
                   if processing_rate < 80.0 {
                       warn!("[Performance warning] Processing rate low: {:.1}%", processing_rate);
                   }
               }
           }
           
           // Graceful shutdown signal handling
           _ = tokio::signal::ctrl_c() => {
               info!("[Graceful shutdown] Received Ctrl+C signal, starting shutdown process");
               
               // Final statistics report
               let (total_rx, pompe_rx, hotstuff_size, pompe_size, hotstuff_consumed) = lockfree_stats.get_stats();
               
               info!("[Final statistics] =========================");
               info!("  Total received transactions: {}", total_rx);
               info!("  Pompe transactions: {}", pompe_rx);
               info!("  Final queue status: HotStuff={}, Pompe={}", hotstuff_size, pompe_size);
               info!("  HotStuff consumed: {}", hotstuff_consumed);
               
               if total_rx > 0 {
                   let processing_rate = (hotstuff_consumed as f64 / total_rx as f64) * 100.0;
                   info!("  Overall processing rate: {:.1}%", processing_rate);
               }
               
               if let Some(ref pompe) = pompe_manager {
                   let (batch_count, ordering1_count, commit_count, consensus_ready, _, tx_store_count, initiator_count) = pompe.get_detailed_stats();
                   
                   info!("  Pompe final status:");
                   info!("     Batch records: {}", batch_count);
                   info!("     ordering1 in progress: {}", ordering1_count);
                   info!("     Commit set size: {}", commit_count);
                   info!("     consensus ready: {}", consensus_ready);
                   info!("     Transaction storage: {}", tx_store_count);
                   info!("     Initiator records: {}", initiator_count);
                   
                   if let Some((connections, messages)) = pompe.get_network_stats().await {
                       info!("     Network connections: {}", connections);
                       info!("     Network messages: {}", messages);
                   }
               }
               
               // Access performance statistics
               info!("  Performance statistics:");
               let submission_tps = node_lockfree_stats.get_submission_tps();
               let confirmation_tps = node_lockfree_stats.get_confirmation_tps();
               let confirmed_txs = node_lockfree_stats.get_confirmed_transactions();
               let confirmed_blocks = node_lockfree_stats.get_confirmed_blocks();
               
               info!("     Submission TPS: {:.2}", submission_tps);
               info!("     Confirmation TPS: {:.2}", confirmation_tps);
               info!("     Confirmed transactions: {}", confirmed_txs);
               info!("     Confirmed blocks: {}", confirmed_blocks);
               
               info!("✅ [Graceful shutdown] Node {} completely lock-free architecture shutdown complete", node_id);
               break;
           }
       }
   }
   
   Ok(())
}