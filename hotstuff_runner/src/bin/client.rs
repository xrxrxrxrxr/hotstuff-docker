// 修改后的高效客户端节点 - 分离状态架构
// hotstuff_runner/src/bin/client.rs

use std::collections::HashMap;
use std::net::SocketAddr;
use std::env;
use std::fs::{File, create_dir_all};
use std::time::{Duration, Instant};
use tracing::{info, warn, error};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt};
use std::thread;
use std::fs;
use ed25519_dalek::SigningKey;
use serde::{Serialize, Deserialize};
use tokio::net::TcpStream;
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use rand::Rng;
use std::sync::Arc;
use std::collections::HashSet;

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

// 分离状态：核心业务逻辑（无需共享）
pub struct ClientNode {
    client_id: String,
    connections: HashMap<usize, PersistentConnection>,
    tx_generator: TransactionGenerator,
    stats: ClientStats,
    response_tx: Option<tokio::sync::mpsc::UnboundedSender<ResponseCommand>>, 
}

// 分离状态：延迟跟踪器（独立运行）
pub struct LatencyTracker {
    send_timestamps: HashMap<u64, Instant>,
    ordering_latencies: Vec<u128>,
    consensus_latencies: Vec<u128>,
    ordering_recorded: HashSet<u64>, 
    consensus_recorded: HashSet<u64>
}

// 分离状态：统计报告器（独立运行）
pub struct StatsReporter {
    total_responses: usize,
}

// 响应命令枚举：延迟跟踪器处理响应
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ResponseCommand {
    Ordering1Response { tx_ids: Vec<u64> },
    HotStuffCommitted { tx_ids: Vec<u64> },
    Error { tx_ids: Vec<u64>, error_msg: String },
}

// #[derive(Serialize, Deserialize, Debug, Clone)]
// pub enum ResponseMessageContent {
//     Ordering1Response {
//         tx_id: u64,
//         timestamp_us: u64,
//         node_id: usize,
//     },
//     HotStuffCommitted {
//         tx_id: u64,
//         timestamp_us: u64,
//         node_id: usize,
//     },
//     Error {
//         tx_id: u64,
//         error_msg: String,
//     },
// }

// #[derive(Serialize, Deserialize, Debug, Clone)]
// pub struct ResponseMessage {
//     pub message_type: String,
//     pub response: Option<ResponseMessageContent>,
//     pub node_id: usize,
// }

impl LatencyTracker {
    pub fn new() -> Self {
        Self {
            send_timestamps: HashMap::new(),
            ordering_latencies: Vec::new(),
            consensus_latencies: Vec::new(),
            ordering_recorded: HashSet::new(),
            consensus_recorded: HashSet::new(),
        }
    }

    pub fn record_send_time(&mut self, tx_id: u64) {
        self.send_timestamps.insert(tx_id, Instant::now());
    }

    // 🔥 修改：支持批量处理 ordering 响应
    pub fn handle_ordering_response(&mut self, tx_ids: Vec<u64>) {
        for tx_id in tx_ids {
            // 只记录第一次
            if self.ordering_recorded.contains(&tx_id) {
                continue;
            }
            if let Some(send_time) = self.send_timestamps.get(&tx_id) {
                let latency = send_time.elapsed().as_micros();
                let latency_ms=latency as f64 / 1000.0;
                self.ordering_latencies.push(latency);
                self.ordering_recorded.insert(tx_id);
                info!("📊 交易 {} ordering延迟: {}ms", tx_id, latency_ms);
            }
        }
    }

    // 🔥 修改：支持批量处理 consensus 响应
    pub fn handle_consensus_response(&mut self, tx_ids: Vec<u64>) {
        for tx_id in tx_ids {
            if self.consensus_recorded.contains(&tx_id) {
                continue;
            }
            if let Some(send_time) = self.send_timestamps.remove(&tx_id) {
                let latency = send_time.elapsed().as_micros();
                let latency_ms=latency as f64 / 1000.0;
                self.consensus_latencies.push(latency);
                self.consensus_recorded.insert(tx_id);
                info!("📊 交易 {} consensus延迟: {}ms", tx_id, latency_ms);
            }
        }
    }

    pub fn get_stats(&self) -> (usize, usize) {
        (self.ordering_latencies.len(), self.consensus_latencies.len())
    }

    pub fn print_ordering_stats(&self) {
        if self.ordering_latencies.is_empty() { return; }
        
        let mut sorted = self.ordering_latencies.clone();
        sorted.sort();
        
        let avg = sorted.iter().sum::<u128>() as f64 / sorted.len() as f64;
        let p50 = sorted[sorted.len() / 2];
        let p95 = sorted[sorted.len() * 95 / 100];
        let p99 = sorted[sorted.len() * 99 / 100];
        
        info!("📈 Ordering延迟统计 (样本: {}):", sorted.len());
        info!("  平均值: {:.2} ms", avg as f64 / 1000.0);
        info!("  P50: {} ms", p50 as f64 / 1000.0);
        info!("  P95: {} ms", p95 as f64 / 1000.0);
        info!("  P99: {} ms", p99 as f64 / 1000.0);
    }

    pub fn print_consensus_stats(&self) {
        if self.consensus_latencies.is_empty() { return; }
        
        let mut sorted = self.consensus_latencies.clone();
        sorted.sort();
        
        let avg = sorted.iter().sum::<u128>() as f64 / sorted.len() as f64;
        let p50 = sorted[sorted.len() / 2];
        let p95 = sorted[sorted.len() * 95 / 100];
        let p99 = sorted[sorted.len() * 99 / 100];
        
        info!("📈 Consensus延迟统计 (样本: {}):", sorted.len());
        info!("  平均值: {:.2} ms", avg as f64 / 1000.0);
        info!("  P50: {} ms", p50 as f64 / 1000.0);
        info!("  P95: {} ms", p95 as f64 / 1000.0);
        info!("  P99: {} ms", p99 as f64 / 1000.0);
    }

    pub fn print_comprehensive_stats(&self) {
        info!("📊 ============= 综合延迟统计报告 =============");
        self.print_ordering_stats();
        self.print_consensus_stats();
        
        if !self.ordering_latencies.is_empty() && !self.consensus_latencies.is_empty() {
            let avg_ordering_ms = self.ordering_latencies.iter().sum::<u128>() as f64 / self.ordering_latencies.len() as f64 / 1000.0;
            let avg_consensus_ms = self.consensus_latencies.iter().sum::<u128>() as f64 / self.consensus_latencies.len() as f64 / 1000.0;
            
            info!("📊 延迟对比分析:");
            info!("  Ordering平均延迟: {:.2} ms", avg_ordering_ms);
            info!("  Consensus平均延迟: {:.2} ms", avg_consensus_ms);
            info!("  Consensus/Ordering比值: {:.2}x", avg_consensus_ms / avg_ordering_ms);
        }
        info!("📊 ==========================================");
    }

    pub fn save_latency_data(&self, ordering_file: &str, consensus_file: &str) -> Result<(), Box<dyn std::error::Error>> {
        use std::io::Write;
        
        if !self.ordering_latencies.is_empty() {
            let mut file = File::create(ordering_file)?;
            writeln!(file, "latency_us")?;
            for latency in &self.ordering_latencies {
                writeln!(file, "{}", latency)?;
            }
            info!("💾 Ordering延迟数据已保存到: {}", ordering_file);
        }

        if !self.consensus_latencies.is_empty() {
            let mut file = File::create(consensus_file)?;
            writeln!(file, "latency_us")?;
            for latency in &self.consensus_latencies {
                writeln!(file, "{}", latency)?;
            }
            info!("💾 Consensus延迟数据已保存到: {}", consensus_file);
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

// 命令枚举：业务逻辑与延迟跟踪通信
#[derive(Debug)]
pub enum ClientCommand {
    SendBatch {
        node_id: usize,
        transactions: Vec<TestTransaction>,
        reply_tx: tokio::sync::oneshot::Sender<Result<usize, Box<dyn std::error::Error + Send>>>,
    },
    RecordSendTimes {
        tx_ids: Vec<u64>,
    },
    PrintStats,
    GetConnectionCount {
        reply_tx: tokio::sync::oneshot::Sender<usize>,
    },
}


impl ClientNode {
    pub fn new(client_id: String) -> Self {
        info!("🚀 初始化客户端核心: {}", client_id);
        
        let tx_generator = TransactionGenerator::new(client_id.clone());

        Self {
            client_id,
            connections: HashMap::new(),
            tx_generator,
            stats: ClientStats::default(),
            response_tx: None,
        }
    }
    pub fn set_response_sender(&mut self, response_tx: tokio::sync::mpsc::UnboundedSender<ResponseCommand>) {
        self.response_tx = Some(response_tx);
    }

    pub async fn establish_connections(
        &mut self, 
        node_least_id: usize, 
        node_num: usize,
        response_tx: tokio::sync::mpsc::UnboundedSender<ResponseCommand>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        info!("🌐 建立到所有节点的持久连接...");
        self.response_tx = Some(response_tx.clone());
         

        for node_id in node_least_id..(node_least_id + node_num) {
            match PersistentConnection::new(node_id,response_tx.clone()).await {
                Ok(conn) => {
                    self.connections.insert(node_id, conn);
                    info!("✅ 连接到节点 {} 成功", node_id);
                }
                Err(e) => {
                    error!("❌ 连接到节点 {} 失败: {}", node_id, e);
                }
            }
        }

        info!("🎯 成功建立 {} 个持久连接", self.connections.len());
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
                    error!("❌ 批量发送到节点 {} 失败: {}", node_id, e);
                    self.stats.record_failed(transactions.len() as u64);
                    
                    // 尝试重新连接
                    if let Some(response_tx) = &self.response_tx {
                    info!("🔄 尝试重新连接到节点 {}", node_id);
                        match PersistentConnection::new(node_id,response_tx.clone()).await {
                            Ok(new_conn) => {
                                self.connections.insert(node_id, new_conn);
                                info!("✅ 重新连接到节点 {} 成功", node_id);
                            }
                            Err(reconnect_err) => {
                                error!("❌ 重新连接到节点 {} 失败: {}", node_id, reconnect_err);
                            }
                        }
                    }
                    Err(e)
                }
            }
        } else {
            error!("❌ 没有到节点 {} 的连接", node_id);
            Err("没有连接".into())
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
        cmd_tx: tokio::sync::mpsc::UnboundedSender<ClientCommand>,
    ) {
        info!("🚀 开始负载测试 - TPS目标: {}, 持续时间: {}秒", 
            config.target_tps, config.duration_secs);

        let batch_size = std::cmp::max(100, config.target_tps / 5);
        let batch_interval = Duration::from_millis(200);
        let end_time = Instant::now() + Duration::from_secs(config.duration_secs);

        let mut total_sent = 0;
        let mut batch_counter = 0;

        while Instant::now() < end_time {
            for node_offset in 0..node_num {
                let node_id = node_least_id + node_offset;
                let transactions = self.tx_generator.generate_batch(batch_size as usize);
                
                // 先通知延迟跟踪器记录发送时间
                let tx_ids: Vec<u64> = transactions.iter().map(|tx| tx.id).collect();
                let _ = cmd_tx.send(ClientCommand::RecordSendTimes { tx_ids });

                match self.send_batch_to_node(node_id, transactions).await {
                    Ok(sent_count) => {
                        total_sent += sent_count;
                        info!("📦 批次 {} 发送 {} 个交易到节点 {}", batch_counter + 1, sent_count, node_id);
                    }
                    Err(e) => {
                        warn!("批次 {} 发送到节点 {} 失败: {}", batch_counter + 1, node_id, e);
                    }
                }
            }

            batch_counter += 1;

            if total_sent >= 5000 && total_sent % 5000 == 0 {
                self.stats.log_summary();
            }

            tokio::time::sleep(batch_interval).await;
        }

        info!("🏁 负载测试完成，总计发送 {} 个交易", total_sent);
        self.stats.log_summary();
    }

    pub async fn run_interactive_mode(
        &mut self, 
        node_least_id: usize, 
        node_num: usize, 
        cmd_tx: tokio::sync::mpsc::UnboundedSender<ClientCommand>, 
    ) {
        info!("🎮 进入交互模式");

        let mut tx_counter = 0;
        
        loop {
            let batch_size = 5;
            let transactions = self.tx_generator.generate_batch(batch_size);
            let target_node = (tx_counter / batch_size) % node_num + node_least_id;

            // 先通知延迟跟踪器记录发送时间
            let tx_ids: Vec<u64> = transactions.iter().map(|tx| tx.id).collect();
            let _ = cmd_tx.send(ClientCommand::RecordSendTimes { tx_ids });

            match self.send_batch_to_node(target_node, transactions).await {
                Ok(sent_count) => {
                    tx_counter += sent_count;
                    info!("✅ 成功发送 {} 个交易到节点 {}, 总计: {}", sent_count, target_node, tx_counter);
                }
                Err(e) => {
                    error!("❌ 发送批次失败到节点 {}: {}", target_node, e);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            }

            if tx_counter >= 100 && tx_counter % 100 == 0 {
                self.stats.log_summary();
            }

            tokio::time::sleep(Duration::from_millis(1000)).await;
        }
    }
}

// 其他结构体保持不变...
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
            "📊 客户端统计 - 发送: {}, 确认: {}, 失败: {}, TPS: {:.2}, 成功率: {:.1}%",
            self.total_sent,
            self.total_confirmed,
            self.total_failed,
            tps,
            success_rate
        );
    }
}

pub struct PersistentConnection {
    // stream: TcpStream,
    write_stream: tokio::net::tcp::OwnedWriteHalf, // 🔥 只保存写流
    node_id: usize,
    connected_at: Instant,
}

impl PersistentConnection {
    pub async fn new(
        node_id: usize,
        response_tx: tokio::sync::mpsc::UnboundedSender<ResponseCommand>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let hostname = format!("node{}", node_id);
        let port = 9000 + node_id as u16;
        let addr_str = format!("{}:{}", hostname, port);

        info!("🔗 建立持久连接到节点 {}: {}", node_id, addr_str);

        let stream = TcpStream::connect(&addr_str).await?;
    
        // 🔥 关键：分离读写流
        let (read_half, write_half) = stream.into_split();

        tokio::spawn(async move {
            if let Err(e) = handle_node_responses(node_id, read_half, response_tx).await {
                error!("❌ 节点 {} 响应接收失败: {}", node_id, e);
            }
        });
        info!("✅ 成功建立持久连接到节点 {}", node_id);

        Ok(Self {
            write_stream: write_half,
            node_id,
            connected_at: Instant::now(),
        })
    }

    pub async fn send_batch(&mut self, transactions: &[TestTransaction], client_id: &str) -> Result<usize, Box<dyn std::error::Error>> {
        let mut batch_buffer = Vec::new();

        let is_pompe = true; /////// 调试修改点

        if is_pompe {
            for transaction in transactions {
                let client_message = ClientMessage {
                    message_type: "pompe_transaction".to_string(),
                    transaction: Some(transaction.clone()),
                    client_id: client_id.to_string(),
                };
            
                let serialized = serde_json::to_vec(&client_message)?;
                let message_length = serialized.len() as u32;
                info!("📦 ******* 客户端发送消息，长度: {} bytes", message_length);

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
                info!("📦 ******* 客户端发送消息，长度: {} bytes", message_length);

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
    create_dir_all("logs").expect("无法创建日志目录");

    let path = match mode {
        "interactive" => "client".to_string(),
        "load_test" => "load_test".to_string(),
        _ => {
            warn!("⚠️ 未知模式，使用默认日志配置");
            "default".to_string()
        }
    };

    let _ = fs::remove_file(format!("logs/{}.log", path));

    let log_file = File::options()
        .create(true)
        .append(true)
        .open(format!("logs/{}.log", path))
        .expect("无法打开日志文件");
    
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
                .with_writer(log_file)
                .with_target(true)
                .with_thread_ids(true)
                .with_ansi(false)
        )
        .try_init();
    
    match result {
        Ok(_) => info!("📝 客户端日志系统初始化成功"),
        Err(_) => warn!("⚠️ 日志系统已经初始化过了，跳过"),
    }
}

// 🔥 修改网络响应解析，支持批量消息
async fn handle_node_responses(
    node_id: usize,
    mut read_half: tokio::net::tcp::OwnedReadHalf,
    response_tx: tokio::sync::mpsc::UnboundedSender<ResponseCommand>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut length_buf = [0u8; 4];
    
    info!("🎧 启动节点 {} 的响应接收器", node_id);
    
    loop {
        match read_half.read_exact(&mut length_buf).await {
            Ok(_) => {
                let message_length = u32::from_be_bytes(length_buf) as usize;
                
                if message_length > 1024 * 1024 {
                    warn!("⚠️ 从节点 {} 收到过大响应: {}", node_id, message_length);
                    continue;
                }
                
                let mut message_buf = vec![0u8; message_length];
                read_half.read_exact(&mut message_buf).await?;
                
                // 🔥 解析响应消息，支持批量 tx_ids
                if let Ok(response_json) = serde_json::from_slice::<serde_json::Value>(&message_buf) {
                    if let Some(message_type) = response_json.get("message_type").and_then(|v| v.as_str()) {
                        
                        // 🔥 支持单个 tx_id 或 tx_ids 数组
                        let tx_ids = if let Some(tx_ids_array) = response_json.get("tx_ids") {
                            // 批量交易 ID
                            serde_json::from_value::<Vec<u64>>(tx_ids_array.clone())
                                .unwrap_or_else(|_| Vec::new())
                        } else if let Some(tx_id) = response_json.get("tx_id").and_then(|v| v.as_u64()) {
                            // 单个交易 ID（向后兼容）
                            vec![tx_id]
                        } else {
                            warn!("⚠️ 响应消息中没有 tx_id 或 tx_ids");
                            continue;
                        };
                        
                        if tx_ids.is_empty() {
                            warn!("⚠️ 响应消息中 tx_ids 为空");
                            continue;
                        }
                        
                        let tx_ids_len = tx_ids.len(); // Store length before moving tx_ids
                        
                        let response_cmd = match message_type {
                            "pompe_ordering1_response" => {
                                ResponseCommand::Ordering1Response { tx_ids }
                            }
                            "consensus_response" => {
                                ResponseCommand::HotStuffCommitted { tx_ids }
                            }
                            "error_response" => {
                                let error_msg = response_json.get("error_msg")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("未知错误")
                                    .to_string();
                                ResponseCommand::Error { tx_ids, error_msg }
                            }
                            _ => {
                                warn!("⚠️ 未知响应类型: {}", message_type);
                                continue;
                            }
                        };
                        
                        // 发送批量响应命令
                        let _ = response_tx.send(response_cmd);
                        info!("✅ 从节点 {} 处理批量响应: {} {} 个交易", 
                              node_id, message_type, tx_ids_len);
                    }
                } else {
                    warn!("⚠️ 无法解析从节点 {} 收到的响应", node_id);
                }
            }
            Err(e) => {
                info!("🔌 节点 {} 连接断开: {}", node_id, e);
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
    
    let node_least_id: usize = env::var("NODE_LEAST_ID")
        .unwrap_or_else(|_| "0".to_string())
        .parse()
        .expect("NODE_LEAST_ID 必须是数字");
    let node_num: usize = env::var("NODE_NUM")
        .unwrap_or_else(|_| "4".to_string())
        .parse()
        .expect("NODE_NUM 必须是数字");

    info!("🏃 启动分离状态客户端: {}", client_id);

    // 创建通道
    let (cmd_tx, mut cmd_rx) = tokio::sync::mpsc::unbounded_channel::<ClientCommand>();
    let (response_tx, mut response_rx) = tokio::sync::mpsc::unbounded_channel::<ResponseCommand>();

    // 启动延迟跟踪器任务
    let latency_cmd_tx = cmd_tx.clone();
    tokio::spawn(async move {
        let mut latency_tracker = LatencyTracker::new();
        let mut stats_reporter = StatsReporter::new();

        loop {
            tokio::select! {
                Some(cmd) = cmd_rx.recv() => {
                    match cmd {
                        ClientCommand::RecordSendTimes { tx_ids } => {
                            for tx_id in tx_ids {
                                latency_tracker.record_send_time(tx_id);
                            }
                        }
                        ClientCommand::PrintStats => {
                            latency_tracker.print_comprehensive_stats();
                        }
                        _ => {} // 其他命令由主任务处理
                    }
                }
                Some(response_cmd) = response_rx.recv() => {
                    match response_cmd {
                        // 🔥 修改：处理批量 ordering 响应
                        ResponseCommand::Ordering1Response { tx_ids } => {
                            info!("🎉 收到 {} 个 Ordering1 响应 for {:?}", tx_ids.len(), tx_ids);
                            latency_tracker.handle_ordering_response(tx_ids);
                        }
                        // 🔥 修改：处理批量 consensus 响应
                        ResponseCommand::HotStuffCommitted { tx_ids } => { 
                            info!("🎉 收到 {} 个 Consensus 响应", tx_ids.len());
                            latency_tracker.handle_consensus_response(tx_ids);
                        }
                        ResponseCommand::Error { tx_ids, error_msg } => {
                            error!("❌ {} 个交易处理失败: {}", tx_ids.len(), error_msg);
                            for tx_id in tx_ids {
                                error!("❌ 交易 {} 失败", tx_id);
                            }
                        }
                    }
                    
                    stats_reporter.record_response();
                    if stats_reporter.should_print_stats() {
                        latency_tracker.print_comprehensive_stats();
                    }
                }
            }
        }
    });

    // 创建并启动客户端核心
    let mut client_core = ClientNode::new(client_id);

    // 等待共识节点启动
    info!("⏳ 等待共识节点启动...");
    tokio::time::sleep(Duration::from_secs(15)).await;

    // 建立连接
    if let Err(e) = client_core.establish_connections(node_least_id, node_num, response_tx.clone()).await {
        error!("❌ 建立连接失败: {}", e);
        return Err(e);
    }

    // 运行主要逻辑
    match mode.as_str() {
        "load_test" => {
            let target_tps: u32 = env::var("TARGET_TPS")
                .unwrap_or_else(|_| "100".to_string())
                .parse()
                .unwrap_or(100);
            
            let duration: u64 = env::var("TEST_DURATION")
                .unwrap_or_else(|_| "60".to_string())
                .parse()
                .unwrap_or(60);

            let config = LoadTestConfig {
                target_tps,
                duration_secs: duration,
            };

            client_core.run_load_test(config, node_least_id, node_num, cmd_tx.clone()).await;

            info!("✅ 负载测试完成，等待响应处理...");
            tokio::time::sleep(Duration::from_secs(30)).await;
            
            // 请求打印最终报告
            let _ = cmd_tx.send(ClientCommand::PrintStats);
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        _ => {
            client_core.run_interactive_mode(node_least_id, node_num, cmd_tx.clone()).await;
        }
    }

    Ok(())
}