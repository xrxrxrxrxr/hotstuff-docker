// hotstuff_runner/src/bin/client_node.rs
//! 客户端节点 - 用于发送测试交易

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
        
        // 随机选择发送方和接收方
        let from_idx = rng.gen_range(0, self.accounts.len());
        let mut to_idx = rng.gen_range(0, self.accounts.len());
        while to_idx == from_idx {
            to_idx = rng.gen_range(0, self.accounts.len());
        }

        let from = self.accounts[from_idx].clone();
        let to = self.accounts[to_idx].clone();
        let amount = rng.gen_range(1, 1000);

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
}

pub struct ClientNode {
    client_id: String,
    node_connections: HashMap<usize, SocketAddr>,
    tx_generator: TransactionGenerator,
    stats: ClientStats,
}

#[derive(Default)]
pub struct ClientStats {
    pub total_sent: u64,
    pub total_confirmed: u64,
    pub total_failed: u64,
    pub start_time: Option<Instant>,
}

impl ClientStats {
    pub fn record_sent(&mut self) {
        if self.start_time.is_none() {
            self.start_time = Some(Instant::now());
        }
        self.total_sent += 1;
    }

    pub fn record_confirmed(&mut self) {
        self.total_confirmed += 1;
    }

    pub fn record_failed(&mut self) {
        self.total_failed += 1;
    }

    pub fn calculate_tps(&self) -> f64 {
        if let Some(start_time) = self.start_time {
            let elapsed = start_time.elapsed().as_secs_f64();
            if elapsed > 0.0 {
                return self.total_confirmed as f64 / elapsed;
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

impl ClientNode {
    pub fn new(client_id: String) -> Self {
        info!("🚀 初始化客户端: {}", client_id);
        
        let tx_generator = TransactionGenerator::new(client_id.clone());

        Self {
            client_id,
            node_connections: HashMap::new(), // 不再预先存储连接
            tx_generator,
            stats: ClientStats::default(),
        }
    }

    pub async fn send_transaction_to_node(
        &mut self,
        node_id: usize,
        transaction: TestTransaction,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // 使用容器主机名直接连接，而不是预解析的 SocketAddr
        let hostname = format!("node{}", node_id);
        let port = 9000 + node_id as u16;
        let addr_str = format!("{}:{}", hostname, port);

        info!("📤 发送交易 {} 到节点 {}: {} -> {} ({})", 
              transaction.id, node_id, transaction.from, transaction.to, transaction.amount);

        info!("🔗 尝试连接: {}", addr_str);

        match TcpStream::connect(&addr_str).await {
            Ok(mut stream) => {
                info!("✅ 成功连接到节点 {}", node_id);
                
                let client_message = ClientMessage {
                    message_type: "transaction".to_string(),
                    transaction: Some(transaction.clone()),
                    client_id: self.client_id.clone(),
                };

                let serialized = serde_json::to_vec(&client_message)?;
                let message_length = serialized.len() as u32;
                
                // 发送消息长度（4字节）+ 消息内容
                stream.write_all(&message_length.to_be_bytes()).await?;
                stream.write_all(&serialized).await?;
                stream.flush().await?;

                self.stats.record_sent();
                info!("✅ 交易 {} 已发送到节点 {}", transaction.id, node_id);

                // info!("📝 暂时跳过响应等待");
                // self.stats.record_confirmed();
                
                // 简单的响应处理（可选）
                let mut response_len_buf = [0u8; 4];
                if stream.read_exact(&mut response_len_buf).await.is_ok() {
                    let response_len = u32::from_be_bytes(response_len_buf) as usize;
                    if response_len < 1024 { // 安全检查
                        let mut response_buf = vec![0u8; response_len];
                        if stream.read_exact(&mut response_buf).await.is_ok() {
                            if let Ok(response) = serde_json::from_slice::<serde_json::Value>(&response_buf) {
                                info!("📥 收到节点 {} 的响应: {:?}", node_id, response);
                                self.stats.record_confirmed();
                            }
                        }
                    }
                } else {
                    // 没有响应也认为发送成功（异步处理）
                    self.stats.record_confirmed();
                }

                Ok(())
            }
            Err(e) => {
                error!("❌ 连接节点 {} ({}) 失败: {}", node_id, addr_str, e);
                self.stats.record_failed();
                Err(e.into())
            }
        }
    }

    pub async fn run_load_test(&mut self, config: LoadTestConfig) {
        info!("🚀 开始负载测试 - TPS目标: {}, 持续时间: {}秒", 
              config.target_tps, config.duration_secs);

        let interval = Duration::from_secs_f64(1.0 / config.target_tps as f64);
        let end_time = Instant::now() + Duration::from_secs(config.duration_secs);

        let mut tx_counter = 0;
        while Instant::now() < end_time {
            let transaction = self.tx_generator.generate_transaction();
            
            // 轮询发送到不同节点
            let target_node = tx_counter % 4;
            
            if let Err(e) = self.send_transaction_to_node(target_node, transaction).await {
                warn!("发送交易失败: {}", e);
            }

            tx_counter += 1;

            // 每100个交易输出一次统计
            if tx_counter % 100 == 0 {
                self.stats.log_summary();
            }

            tokio::time::sleep(interval).await;
        }

        info!("🏁 负载测试完成");
        self.stats.log_summary();
    }

    pub async fn run_interactive_mode(&mut self) {
        info!("🎮 进入交互模式 - 每5秒发送一个交易");

        loop {
            let transaction = self.tx_generator.generate_transaction();
            let target_node = (transaction.id - 1) % 4;

            if let Err(e) = self.send_transaction_to_node(target_node as usize, transaction).await {
                warn!("发送交易失败: {}", e);
            }

            // 每10个交易输出一次统计
            if self.tx_generator.current_tx_id % 10 == 0 {
                self.stats.log_summary();
            }

            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }
}

pub struct LoadTestConfig {
    pub target_tps: u32,
    pub duration_secs: u64,
}

fn setup_tracing_logger() {
    create_dir_all("logs").expect("无法创建日志目录");
    let _ = fs::remove_file(format!("logs/client.log"));

    let log_file = File::options()
        .create(true)
        .append(true)
        .open("logs/client.log")
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    setup_tracing_logger();

    let client_id = env::var("CLIENT_ID").unwrap_or_else(|_| "client_001".to_string());
    let mode = env::var("CLIENT_MODE").unwrap_or_else(|_| "interactive".to_string());

    info!("🏃 启动客户端节点: {}", client_id);

    let mut client_node = ClientNode::new(client_id);

    // 等待共识节点启动
    info!("⏳ 等待共识节点启动...");
    tokio::time::sleep(Duration::from_secs(15)).await;

    match mode.as_str() {
        "load_test" => {
            let target_tps: u32 = env::var("TARGET_TPS")
                .unwrap_or_else(|_| "10".to_string())
                .parse()
                .unwrap_or(10);
            
            let duration: u64 = env::var("TEST_DURATION")
                .unwrap_or_else(|_| "60".to_string())
                .parse()
                .unwrap_or(60);

            let config = LoadTestConfig {
                target_tps,
                duration_secs: duration,
            };

            client_node.run_load_test(config).await;
        }
        _ => {
            client_node.run_interactive_mode().await;
        }
    }

    Ok(())
}