// Optimized client node with separated state architecture
// hotstuff_runner/src/bin/client.rs

use ed25519_dalek::SigningKey;
use rand::Rng;
use serde::{Deserialize, Serialize};
use sha2::digest::consts::U328;
use std::collections::HashMap;
use std::collections::HashSet;
use std::env;
use std::fs;
use std::fs::{create_dir_all, File};
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tracing::{error, info, warn};
use tracing_subscriber::field::debug;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

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

// Separated state: core business logic (no sharing required)
pub struct ClientNode {
    client_id: String,
    connections: HashMap<usize, PersistentConnection>,
    tx_generator: TransactionGenerator,
    stats: ClientStats,
    response_tx: Option<tokio::sync::mpsc::UnboundedSender<ResponseCommand>>,
    is_pompe: bool,
    is_smrol: bool,
}

// Separated state: latency tracker (standalone)
pub struct LatencyTracker {
    send_timestamps: HashMap<u64, Instant>,
    pushed_timestamps: HashMap<u64, Instant>,
    ordering_latencies: Vec<u128>,
    consensus_latencies: Vec<u128>,
    pushed_to_consensus_latencies: Vec<u128>,
    ordering_recorded: HashSet<u64>,
    consensus_recorded: HashSet<u64>,
}

// Separated state: statistics reporter (standalone)
pub struct StatsReporter {
    total_responses: usize,
}

// Response command enum: latency tracker handles responses
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ResponseCommand {
    Ordering1Response { tx_ids: Vec<u64> },
    SmrolOrderingResponse { tx_ids: Vec<u64> },
    PushedToHotStuff { tx_ids: Vec<u64> },
    HotStuffCommitted { tx_ids: Vec<u64> },
    Error { tx_ids: Vec<u64>, error_msg: String },
}

use std::sync::OnceLock;

static HOST_MAP: OnceLock<HashMap<String, String>> = OnceLock::new();

pub fn resolve_target(target_id: usize, port: u16) -> String {
    let target_name = format!("node{}", target_id);

    // Initialize HOST_MAP (run once)
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

impl LatencyTracker {
    pub fn new() -> Self {
        Self {
            send_timestamps: HashMap::new(),
            pushed_timestamps: HashMap::new(),
            ordering_latencies: Vec::new(),
            consensus_latencies: Vec::new(),
            pushed_to_consensus_latencies: Vec::new(),
            ordering_recorded: HashSet::new(),
            consensus_recorded: HashSet::new(),
        }
    }

    pub fn record_send_time(&mut self, tx_id: u64) {
        self.send_timestamps.insert(tx_id, Instant::now());
    }

    // Support batch processing of ordering responses
    pub fn handle_ordering_response(&mut self, tx_ids: Vec<u64>) {
        for tx_id in tx_ids {
            // Record only the first occurrence
            if self.ordering_recorded.contains(&tx_id) {
                continue;
            }
            if let Some(send_time) = self.send_timestamps.get(&tx_id) {
                let latency = send_time.elapsed().as_micros();
                let latency_ms = latency as f64 / 1000.0;
                self.ordering_latencies.push(latency);
                self.ordering_recorded.insert(tx_id);
                // if tx_id % 1000 == 0 {
                info!("[latency] tx {} ordering delay: {} ms", tx_id, latency_ms);
                // }
            }
        }
    }

    pub fn handle_pushed_to_hotstuff(&mut self, tx_ids: Vec<u64>) {
        let now = Instant::now();
        for tx_id in tx_ids {
            self.pushed_timestamps.entry(tx_id).or_insert(now);
        }
    }

    // Support batch processing of consensus responses
    pub fn handle_consensus_response(&mut self, tx_ids: Vec<u64>) {
        for tx_id in tx_ids {
            if self.consensus_recorded.contains(&tx_id) {
                continue;
            }
            if let Some(send_time) = self.send_timestamps.remove(&tx_id) {
                let latency = send_time.elapsed().as_micros();
                let latency_ms = latency as f64 / 1000.0;
                self.consensus_latencies.push(latency);
                self.consensus_recorded.insert(tx_id);
                if tx_id % 1000 == 0 {
                    info!("[latency] tx {} consensus delay: {} ms", tx_id, latency_ms);
                }
            }
            if let Some(pushed_time) = self.pushed_timestamps.remove(&tx_id) {
                let latency = pushed_time.elapsed().as_micros();
                self.pushed_to_consensus_latencies.push(latency);
                let latency_ms = latency as f64 / 1000.0;
                if tx_id % 1000 == 0 {
                    info!(
                        "[latency] tx {} pushed2hotstuff->consensus delay: {} ms",
                        tx_id, latency_ms
                    );
                }
            }
        }
    }

    pub fn get_stats(&self) -> (usize, usize) {
        (
            self.ordering_latencies.len(),
            self.consensus_latencies.len(),
        )
    }

    pub fn print_ordering_stats(&self) {
        if self.ordering_latencies.is_empty() {
            return;
        }

        let mut sorted = self.ordering_latencies.clone();
        sorted.sort();

        let avg = sorted.iter().sum::<u128>() as f64 / sorted.len() as f64;
        let p50 = sorted[sorted.len() / 2];
        let p95 = sorted[sorted.len() * 95 / 100];
        let p99 = sorted[sorted.len() * 99 / 100];

        info!("[ordering] latency statistics (samples: {}):", sorted.len());
        info!("  Average: {:.2} ms", avg as f64 / 1000.0);
        info!("  P50: {} ms", p50 as f64 / 1000.0);
        info!("  P95: {} ms", p95 as f64 / 1000.0);
        info!("  P99: {} ms", p99 as f64 / 1000.0);
    }

    pub fn print_consensus_stats(&self) {
        if self.consensus_latencies.is_empty() {
            return;
        }

        let mut sorted = self.consensus_latencies.clone();
        sorted.sort();

        let avg = sorted.iter().sum::<u128>() as f64 / sorted.len() as f64;
        let p50 = sorted[sorted.len() / 2];
        let p95 = sorted[sorted.len() * 95 / 100];
        let p99 = sorted[sorted.len() * 99 / 100];

        info!(
            "[consensus] latency statistics (samples: {}):",
            sorted.len()
        );
        info!("  Average: {:.2} ms", avg as f64 / 1000.0);
        info!("  P50: {} ms", p50 as f64 / 1000.0);
        info!("  P95: {} ms", p95 as f64 / 1000.0);
        info!("  P99: {} ms", p99 as f64 / 1000.0);
    }

    pub fn print_pushed_to_consensus_stats(&self) {
        if self.pushed_to_consensus_latencies.is_empty() {
            return;
        }

        let mut sorted = self.pushed_to_consensus_latencies.clone();
        sorted.sort();

        let avg = sorted.iter().sum::<u128>() as f64 / sorted.len() as f64;
        let p50 = sorted[sorted.len() / 2];
        let p95 = sorted[sorted.len() * 95 / 100];
        let p99 = sorted[sorted.len() * 99 / 100];

        info!(
            "[pushed2hotstuff->consensus] latency statistics (samples: {}):",
            sorted.len()
        );
        info!("  Average: {:.2} ms", avg as f64 / 1000.0);
        info!("  P50: {} ms", p50 as f64 / 1000.0);
        info!("  P95: {} ms", p95 as f64 / 1000.0);
        info!("  P99: {} ms", p99 as f64 / 1000.0);
    }

    pub fn print_comprehensive_stats(&self) {
        info!("[latency] ============= aggregate latency report =============");
        self.print_ordering_stats();
        self.print_consensus_stats();
        self.print_pushed_to_consensus_stats();

        if !self.ordering_latencies.is_empty() && !self.consensus_latencies.is_empty() {
            let avg_ordering_ms = self.ordering_latencies.iter().sum::<u128>() as f64
                / self.ordering_latencies.len() as f64
                / 1000.0;
            let avg_consensus_ms = self.consensus_latencies.iter().sum::<u128>() as f64
                / self.consensus_latencies.len() as f64
                / 1000.0;

            info!("[latency] comparison analysis:");
            info!("  Ordering average: {:.2} ms", avg_ordering_ms);
            info!("  Consensus average: {:.2} ms", avg_consensus_ms);
            info!(
                "  Consensus/Ordering ratio: {:.2}x",
                avg_consensus_ms / avg_ordering_ms
            );
            if !self.pushed_to_consensus_latencies.is_empty() {
                let avg_pushed_consensus_ms =
                    self.pushed_to_consensus_latencies.iter().sum::<u128>() as f64
                        / self.pushed_to_consensus_latencies.len() as f64
                        / 1000.0;
                info!(
                    "  Pushed2HotStuff->Consensus average: {:.2} ms",
                    avg_pushed_consensus_ms
                );
            }
        }
        info!("📊 ==========================================");
    }

    pub fn save_latency_data(
        &self,
        ordering_file: &str,
        consensus_file: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use std::io::Write;

        if !self.ordering_latencies.is_empty() {
            let mut file = File::create(ordering_file)?;
            writeln!(file, "latency_us")?;
            for latency in &self.ordering_latencies {
                writeln!(file, "{}", latency)?;
            }
            info!(
                "[latency] ordering latency data saved to: {}",
                ordering_file
            );
        }

        if !self.consensus_latencies.is_empty() {
            let mut file = File::create(consensus_file)?;
            writeln!(file, "latency_us")?;
            for latency in &self.consensus_latencies {
                writeln!(file, "{}", latency)?;
            }
            info!(
                "[latency] consensus latency data saved to: {}",
                consensus_file
            );
        }

        Ok(())
    }
}

impl StatsReporter {
    pub fn new() -> Self {
        Self { total_responses: 0 }
    }

    pub fn record_response(&mut self) {
        self.total_responses += 1;
    }

    pub fn should_print_stats(&self) -> bool {
        self.total_responses > 0 && self.total_responses % 100 == 0
    }
}

// Command enum: bridges business logic and latency tracker
#[derive(Debug)]
pub enum ClientCommand {
    SendBatch {
        node_id: usize,
        transactions: Vec<TestTransaction>,
        reply_tx: tokio::sync::oneshot::Sender<Result<usize, Box<dyn std::error::Error + Send>>>,
    },
    PrintStats,
    GetConnectionCount {
        reply_tx: tokio::sync::oneshot::Sender<usize>,
    },
}

impl ClientNode {
    pub fn new(client_id: String, is_pompe: bool, is_smrol: bool) -> Self {
        info!("[client] initializing core: {}", client_id);

        let tx_generator = TransactionGenerator::new(client_id.clone());

        Self {
            client_id,
            connections: HashMap::new(),
            tx_generator,
            stats: ClientStats::default(),
            response_tx: None,
            is_pompe,
            is_smrol,
        }
    }
    pub fn set_response_sender(
        &mut self,
        response_tx: tokio::sync::mpsc::UnboundedSender<ResponseCommand>,
    ) {
        self.response_tx = Some(response_tx);
    }

    pub async fn establish_connections(
        &mut self,
        node_least_id: usize,
        node_num: usize,
        response_tx: tokio::sync::mpsc::UnboundedSender<ResponseCommand>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        info!("[client] establishing persistent connections to all nodes...");
        self.response_tx = Some(response_tx.clone());

        for node_id in node_least_id..(node_least_id + node_num) {
            match PersistentConnection::new(
                node_id,
                response_tx.clone(),
                self.is_pompe,
                self.is_smrol,
            )
            .await
            {
                Ok(conn) => {
                    self.connections.insert(node_id, conn);
                    info!("[client] connected to node {}", node_id);
                }
                Err(e) => {
                    error!("[client] failed to connect to node {}: {}", node_id, e);
                }
            }
        }

        info!(
            "[client] established {} persistent connections",
            self.connections.len()
        );
        Ok(())
    }

    pub async fn send_batch_to_node(
        &mut self,
        node_id: usize,
        transactions: Vec<TestTransaction>,
    ) -> Result<usize, Box<dyn std::error::Error>> {
        if let Some(connection) = self.connections.get_mut(&node_id) {
            match connection.send_batch(&transactions, &self.client_id).await {
                Ok(sent_count) => {
                    self.stats.record_sent(sent_count as u64);
                    self.stats.record_confirmed(sent_count as u64);
                    Ok(sent_count)
                }
                Err(e) => {
                    error!("[client] failed to send batch to node {}: {}", node_id, e);
                    self.stats.record_failed(transactions.len() as u64);

                    // Attempt to reconnect
                    if let Some(response_tx) = &self.response_tx {
                        info!("[client] attempting reconnection to node {}", node_id);
                        match PersistentConnection::new(
                            node_id,
                            response_tx.clone(),
                            self.is_pompe,
                            self.is_smrol,
                        )
                        .await
                        {
                            Ok(new_conn) => {
                                self.connections.insert(node_id, new_conn);
                                info!("[client] reconnected to node {}", node_id);
                            }
                            Err(reconnect_err) => {
                                error!(
                                    "[client] failed to reconnect to node {}: {}",
                                    node_id, reconnect_err
                                );
                            }
                        }
                    }
                    Err(e)
                }
            }
        } else {
            error!("[client] no connection to node {}", node_id);
            Err("connection not available".into())
        }
    }

    pub fn get_connection_count(&self) -> usize {
        self.connections.len()
    }

    pub async fn run_load_test(
        &mut self,
        config: LoadTestConfig,
        node_least_id: usize,
        node_num: usize,
        latency_tracker: Arc<Mutex<LatencyTracker>>,
    ) {
        info!(
            "[load-test] starting load test - target TPS: {}, duration: {} s",
            config.target_tps, config.duration_secs
        );

        // Tuning hook: reduce send rate to probe latency
        let is_latency = false;

        // let mut batch_size = std::cmp::max(100, config.target_tps / 5);
        let mut batch_size = config.target_tps / (5 * node_num as u32);
        if batch_size == 0 {
            batch_size = 1;
        }
        //  if batch_size>100 { batch_size=100; } // cap batch size at 100
        let mut batch_interval = Duration::from_millis(200);

        if is_latency {
            batch_size = 1;
            batch_interval = Duration::from_millis(1000);
        }
        // let batch_size = 1;
        // let batch_interval = Duration::from_millis(1000);
        let end_time = Instant::now() + Duration::from_secs(config.duration_secs);

        let mut total_sent = 0;
        let mut batch_counter = 0;
        let mut rng = rand::thread_rng();

        while Instant::now() < end_time {
            for node_offset in 0..node_num {
                // Debug hook
                // for node_offset in 0..2 {
                let node_id = node_least_id + node_offset;
                // let node_id = node_least_id; // Debug: send only to the first node
                let transactions = self.tx_generator.generate_batch(batch_size as usize);

                // Notify latency tracker before sending
                let tx_ids: Vec<u64> = transactions.iter().map(|tx| tx.id).collect();
                {
                    let mut tracker = latency_tracker.lock().await;
                    for tx_id in &tx_ids {
                        tracker.record_send_time(*tx_id);
                    }
                }

                match self.send_batch_to_node(node_id, transactions).await {
                    Ok(sent_count) => {
                        total_sent += sent_count;
                        info!(
                            "[load-test] batch {} sent {} transactions to node {}",
                            batch_counter + 1,
                            sent_count,
                            node_id
                        );
                    }
                    Err(e) => {
                        warn!(
                            "[load-test] batch {} failed to send to node {}: {}",
                            batch_counter + 1,
                            node_id,
                            e
                        );
                    }
                }
            }

            batch_counter += 1;

            if total_sent >= 5000 && total_sent % 5000 == 0 {
                self.stats.log_summary();
            }

            // Apply light symmetric jitter to reduce phase alignment across clients
            let base_ms = std::cmp::max(1, batch_interval.as_millis() as i64);
            let jitter_window_ms = std::cmp::max(1, base_ms / 5);
            let jitter_offset_ms = rng.gen_range(-jitter_window_ms, jitter_window_ms + 1);
            let sleep_ms = std::cmp::max(1, base_ms + jitter_offset_ms) as u64;
            tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
            // Debug hook: send one transaction every 120 seconds
            // tokio::time::sleep(Duration::from_millis(100)).await;
        }

        info!(
            "[load-test] completed; total transactions sent: {}",
            total_sent
        );
        self.stats.log_summary();
    }

    pub async fn run_interactive_mode(
        &mut self,
        node_least_id: usize,
        node_num: usize,
        latency_tracker: Arc<Mutex<LatencyTracker>>,
    ) {
        info!("[client] entering interactive mode");

        let mut tx_counter = 0;
        let mut rng = rand::thread_rng();

        loop {
            let batch_size = 5;
            let transactions = self.tx_generator.generate_batch(batch_size);
            let target_node = (tx_counter / batch_size) % node_num + node_least_id;

            // Notify latency tracker before sending
            let tx_ids: Vec<u64> = transactions.iter().map(|tx| tx.id).collect();
            {
                let mut tracker = latency_tracker.lock().await;
                for tx_id in &tx_ids {
                    tracker.record_send_time(*tx_id);
                }
            }

            match self.send_batch_to_node(target_node, transactions).await {
                Ok(sent_count) => {
                    tx_counter += sent_count;
                    info!(
                        "[client] sent {} transactions to node {}, cumulative total: {}",
                        sent_count, target_node, tx_counter
                    );
                }
                Err(e) => {
                    error!(
                        "[client] failed to send batch to node {}: {}",
                        target_node, e
                    );
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            }

            if tx_counter >= 100 && tx_counter % 100 == 0 {
                self.stats.log_summary();
            }

            // Interactive mode jitter: keep average pacing but break strict periodicity
            let base_ms = 1000i64;
            let jitter_window_ms = std::cmp::max(1, base_ms / 5);
            let jitter_offset_ms = rng.gen_range(-jitter_window_ms, jitter_window_ms + 1);
            let sleep_ms = std::cmp::max(1, base_ms + jitter_offset_ms) as u64;
            tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
        }
    }
}

// Other structs remain unchanged...
pub struct TransactionGenerator {
    current_tx_id: u64,
    current_nonce: u64,
    client_id: String,
    accounts: Vec<String>,
}

impl TransactionGenerator {
    pub fn new(client_id: String) -> Self {
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
            client_id,
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
            id: self.current_tx_id,
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

    pub fn generate_batch(&mut self, count: usize) -> Vec<TestTransaction> {
        (0..count).map(|_| self.generate_transaction()).collect()
    }
}

#[derive(Default)]
pub struct ClientStats {
    pub total_sent: u64,
    pub total_confirmed: u64,
    pub total_failed: u64,
    pub start_time: Option<Instant>,
}

impl ClientStats {
    pub fn record_sent(&mut self, count: u64) {
        if self.start_time.is_none() {
            self.start_time = Some(Instant::now());
        }
        self.total_sent += count;
    }

    pub fn record_confirmed(&mut self, count: u64) {
        self.total_confirmed += count;
    }

    pub fn record_failed(&mut self, count: u64) {
        self.total_failed += count;
    }

    pub fn calculate_tps(&self) -> f64 {
        if let Some(start_time) = self.start_time {
            let elapsed = start_time.elapsed().as_secs_f64();
            if elapsed > 0.0 {
                return self.total_sent as f64 / elapsed;
            }
        }
        0.0
    }

    pub fn log_summary(&self) {
        let tps = self.calculate_tps();
        let success_rate = if self.total_sent > 0 {
            (self.total_confirmed as f64 / self.total_sent as f64) * 100.0
        } else {
            0.0
        };

        info!(
            "[client-stats] sent: {}, confirmed: {}, failed: {}, TPS: {:.2}, success rate: {:.1}%",
            self.total_sent, self.total_confirmed, self.total_failed, tps, success_rate
        );
    }
}

pub struct PersistentConnection {
    // stream: TcpStream,
    write_stream: tokio::net::tcp::OwnedWriteHalf, // Store only the write half
    node_id: usize,
    connected_at: Instant,
    is_pompe: bool,
    is_smrol: bool,
}

impl PersistentConnection {
    pub async fn new(
        node_id: usize,
        response_tx: tokio::sync::mpsc::UnboundedSender<ResponseCommand>,
        is_pompe: bool,
        is_smrol: bool,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let hostname = format!("node{}", node_id);
        // let port = 9000 + node_id as u16;
        let port = 9000;
        let addr_str = resolve_target(node_id, port); // Use resolver helper
        info!("Resolved address for node {}: {}", node_id, addr_str);
        // let addr_str = format!("{}:{}", hostname, port);

        info!(
            "[client] establishing persistent connection to node {}: {}",
            node_id, addr_str
        );

        let stream = TcpStream::connect(&addr_str).await?;

        // Split read/write halves to avoid locking
        let (read_half, write_half) = stream.into_split();

        tokio::spawn(async move {
            if let Err(e) = handle_node_responses(node_id, read_half, response_tx).await {
                error!("[client] node {} response receiver error: {}", node_id, e);
            }
        });
        info!(
            "[client] established persistent connection to node {}",
            node_id
        );

        Ok(Self {
            write_stream: write_half,
            node_id,
            connected_at: Instant::now(),
            is_pompe,
            is_smrol,
        })
    }

    pub async fn send_batch(
        &mut self,
        transactions: &[TestTransaction],
        client_id: &str,
    ) -> Result<usize, Box<dyn std::error::Error>> {
        let mut batch_buffer = Vec::new();

        if self.is_pompe {
            for transaction in transactions {
                let client_message = ClientMessage {
                    message_type: "pompe_transaction".to_string(),
                    transaction: Some(transaction.clone()),
                    client_id: client_id.to_string(),
                };

                let serialized = serde_json::to_vec(&client_message)?;
                let message_length = serialized.len() as u32;
                // Average message length ~170 bytes
                // info!("[client] sending Pompe message, length: {} bytes", message_length);

                batch_buffer.extend_from_slice(&message_length.to_be_bytes());
                batch_buffer.extend_from_slice(&serialized);
            }
        } else if self.is_smrol {
            for transaction in transactions {
                let client_message = ClientMessage {
                    message_type: "smrol_transaction".to_string(),
                    transaction: Some(transaction.clone()),
                    client_id: client_id.to_string(),
                };

                let serialized = serde_json::to_vec(&client_message)?;
                let message_length = serialized.len() as u32;
                // info!("[client] sending message, length: {} bytes", message_length);

                batch_buffer.extend_from_slice(&message_length.to_be_bytes());
                batch_buffer.extend_from_slice(&serialized);
            }
        } else {
            for transaction in transactions {
                let client_message = ClientMessage {
                    message_type: "transaction".to_string(),
                    transaction: Some(transaction.clone()),
                    client_id: client_id.to_string(),
                };

                let serialized = serde_json::to_vec(&client_message)?;
                let message_length = serialized.len() as u32;
                // info!("[client] sending message, length: {} bytes", message_length);

                batch_buffer.extend_from_slice(&message_length.to_be_bytes());
                batch_buffer.extend_from_slice(&serialized);
            }
        }

        self.write_stream.write_all(&batch_buffer).await?;
        self.write_stream.flush().await?;

        Ok(transactions.len())
    }

    pub fn uptime(&self) -> Duration {
        self.connected_at.elapsed()
    }
}

pub struct LoadTestConfig {
    pub target_tps: u32,
    pub duration_secs: u64,
}

fn setup_tracing_logger(mode: &str) {
    create_dir_all("logs").expect("Unable to create logs directory");

    let path = match mode {
        "interactive" => "client".to_string(),
        "load_test" => "load_test".to_string(),
        _ => {
            warn!("[logger] unknown mode; falling back to default configuration");
            "default".to_string()
        }
    };

    let _ = fs::remove_file(format!("logs/{}.log", path));

    let log_file = File::options()
        .create(true)
        .append(true)
        .open(format!("logs/{}.log", path))
        .expect("Unable to open log file");

    let result = tracing_subscriber::registry()
        // .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")))
        .with(
            fmt::layer()
                .with_writer(std::io::stdout)
                .with_target(true)
                .with_thread_ids(true)
                .with_ansi(true),
        )
        .with(
            fmt::layer()
                .with_writer(log_file)
                .with_target(true)
                .with_thread_ids(true)
                .with_ansi(false),
        )
        .try_init();

    match result {
        Ok(_) => info!("[logger] client logging initialized"),
        Err(_) => warn!("[logger] logging already initialized; skipping"),
    }
}

// Adjust response parsing to support batched messages
async fn handle_node_responses(
    node_id: usize,
    mut read_half: tokio::net::tcp::OwnedReadHalf,
    response_tx: tokio::sync::mpsc::UnboundedSender<ResponseCommand>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut length_buf = [0u8; 4];

    info!("[client] starting response receiver for node {}", node_id);

    loop {
        match read_half.read_exact(&mut length_buf).await {
            Ok(_) => {
                let message_length = u32::from_be_bytes(length_buf) as usize;

                if message_length > 1024 * 1024 {
                    warn!(
                        "[client] oversized response from node {}: {} bytes",
                        node_id, message_length
                    );
                    continue;
                }

                let mut message_buf = vec![0u8; message_length];
                read_half.read_exact(&mut message_buf).await?;

                // Parse response messages; support batched tx_ids
                if let Ok(response_json) = serde_json::from_slice::<serde_json::Value>(&message_buf)
                {
                    if let Some(message_type) =
                        response_json.get("message_type").and_then(|v| v.as_str())
                    {
                        // Support either single tx_id or an array of tx_ids
                        let tx_ids = if let Some(tx_ids_array) = response_json.get("tx_ids") {
                            // Batched transaction IDs
                            serde_json::from_value::<Vec<u64>>(tx_ids_array.clone())
                                .unwrap_or_else(|_| Vec::new())
                        } else if let Some(tx_id) =
                            response_json.get("tx_id").and_then(|v| v.as_u64())
                        {
                            // Single transaction ID (backward compatibility)
                            vec![tx_id]
                        } else {
                            warn!("[client] response missing tx_id or tx_ids");
                            continue;
                        };

                        if tx_ids.is_empty() {
                            warn!("[client] response contained empty tx_ids array");
                            continue;
                        }

                        let tx_ids_len = tx_ids.len(); // Store length before moving tx_ids

                        let response_cmd = match message_type {
                            "pompe_ordering1_response" => {
                                ResponseCommand::Ordering1Response { tx_ids }
                            }
                            "smrol_ordering_response" => {
                                ResponseCommand::SmrolOrderingResponse { tx_ids }
                            }
                            // "smrol_ordering1_response" => {
                            //     ResponseCommand::Ordering1Response { tx_ids }
                            // }
                            "pushed2hotstuff" => ResponseCommand::PushedToHotStuff { tx_ids },
                            "consensus_response" => ResponseCommand::HotStuffCommitted { tx_ids },
                            "error_response" => {
                                let error_msg = response_json
                                    .get("error_msg")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("unknown error")
                                    .to_string();
                                ResponseCommand::Error { tx_ids, error_msg }
                            }
                            _ => {
                                warn!("[client] unknown response type: {}", message_type);
                                continue;
                            }
                        };

                        // Dispatch batched response command
                        let _ = response_tx.send(response_cmd);
                        // info!("[client] processed batched response from node {}: {} transactions",
                        //       node_id, message_type, tx_ids_len);
                    }
                } else {
                    warn!("[client] failed to parse response from node {}", node_id);
                }
            }
            Err(e) => {
                info!("[client] node {} connection closed: {}", node_id, e);
                break;
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client_id = env::var("CLIENT_ID").unwrap_or_else(|_| "client_1".to_string());
    let mode = env::var("CLIENT_MODE").unwrap_or_else(|_| "interactive".to_string());
    setup_tracing_logger(mode.as_str());

    let ordering_mode = env::var("CLIENT_ORDERING_MODE")
        .unwrap_or_else(|_| "smrol".to_string())
        .to_lowercase();
    let (is_pompe, is_smrol) = match ordering_mode.as_str() {
        "pompe" => (true, false),
        "smrol" => (false, true),
        other => {
            warn!(
                "[client] unknown CLIENT_ORDERING_MODE='{}'; defaulting to SMROL pipeline",
                other
            );
            (false, true)
        }
    };
    info!(
        "[client] transaction send mode: {}",
        if is_pompe {
            "Pompe"
        } else if is_smrol {
            "Smrol"
        } else {
            "Legacy"
        }
    );

    let node_least_id: usize = env::var("NODE_LEAST_ID")
        .unwrap_or_else(|_| "0".to_string())
        .parse()
        .expect("NODE_LEAST_ID must be numeric");
    let node_num: usize = env::var("NODE_NUM")
        .unwrap_or_else(|_| "4".to_string())
        .parse()
        .expect("NODE_NUM must be numeric");

    info!("[client] starting separated-state client: {}", client_id);

    // Create channels
    let (cmd_tx, mut cmd_rx) = tokio::sync::mpsc::unbounded_channel::<ClientCommand>();
    let (response_tx, mut response_rx) = tokio::sync::mpsc::unbounded_channel::<ResponseCommand>();

    let latency_tracker = Arc::new(Mutex::new(LatencyTracker::new()));

    // Launch latency tracker task
    {
        let tracker = Arc::clone(&latency_tracker);
        tokio::spawn(async move {
            let mut cmd_rx = cmd_rx;
            let mut response_rx = response_rx;
            let mut stats_reporter = StatsReporter::new();

            loop {
                tokio::select! {
                    Some(cmd) = cmd_rx.recv() => {
                        match cmd {
                            ClientCommand::PrintStats => {
                                let tracker_guard = tracker.lock().await;
                                tracker_guard.print_comprehensive_stats();
                            }
                            _ => {} // Other commands handled by the main task
                        }
                    }
                    Some(response_cmd) = response_rx.recv() => {
                        {
                            let mut tracker_guard = tracker.lock().await;
                            match response_cmd {
                                ResponseCommand::Ordering1Response { tx_ids } => {
                                    tracker_guard.handle_ordering_response(tx_ids);
                                }
                                ResponseCommand::SmrolOrderingResponse { tx_ids } => {
                                    tracker_guard.handle_ordering_response(tx_ids);
                                }
                                ResponseCommand::PushedToHotStuff { tx_ids } => {
                                    tracker_guard.handle_pushed_to_hotstuff(tx_ids);
                                }
                                ResponseCommand::HotStuffCommitted { tx_ids } => {
                                    tracker_guard.handle_consensus_response(tx_ids);
                                }
                                ResponseCommand::Error { tx_ids, error_msg } => {
                                    error!("[client] {} transactions failed: {}", tx_ids.len(), error_msg);
                                    for tx_id in tx_ids {
                                        error!("[client] transaction {} failed", tx_id);
                                    }
                                }
                            }
                        }

                        stats_reporter.record_response();
                        if stats_reporter.should_print_stats() {
                            let tracker_guard = tracker.lock().await;
                            tracker_guard.print_comprehensive_stats();
                        }
                    }
                }
            }
        });
    }

    // Create and start the client core
    let mut client_core = ClientNode::new(client_id, is_pompe, is_smrol);

    // Wait for consensus nodes to start
    info!("[client] waiting for consensus nodes to start...");
    tokio::time::sleep(Duration::from_secs(20)).await;

    // Establish persistent connections
    if let Err(e) = client_core
        .establish_connections(node_least_id, node_num, response_tx.clone())
        .await
    {
        error!("[client] failed to establish connections: {}", e);
        return Err(e);
    }

    // Run primary logic
    match mode.as_str() {
        "load_test" => {
            let target_tps: u32 = env::var("TARGET_TPS")
                .unwrap_or_else(|_| "100".to_string())
                .parse()
                .unwrap_or(100);
            info!("[client] target TPS: {} x n", target_tps);

            let duration: u64 = env::var("TEST_DURATION")
                .unwrap_or_else(|_| "60".to_string())
                .parse()
                .unwrap_or(60);

            let config = LoadTestConfig {
                target_tps,
                duration_secs: duration,
            };

            client_core
                .run_load_test(
                    config,
                    node_least_id,
                    node_num,
                    Arc::clone(&latency_tracker),
                )
                .await;

            info!("[client] load test complete; awaiting response processing...");
            tokio::time::sleep(Duration::from_secs(30)).await;

            // Request the final report
            let _ = cmd_tx.send(ClientCommand::PrintStats);
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        _ => {
            client_core
                .run_interactive_mode(node_least_id, node_num, Arc::clone(&latency_tracker))
                .await;
        }
    }

    Ok(())
}
