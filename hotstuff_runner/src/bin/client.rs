// 修改后的高效客户端节点

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
use hotstuff_runner::pompe::{PompeTransaction, send_pompe_transaction_to_node}; 

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

// 新增：持久连接管理器
pub struct PersistentConnection {
    stream: TcpStream,
    node_id: usize,
    connected_at: Instant,
}

impl PersistentConnection {
    pub async fn new(node_id: usize) -> Result<Self, Box<dyn std::error::Error>> {
        let hostname = format!("node{}", node_id);
        let port = 9000 + node_id as u16;
        let addr_str = format!("{}:{}", hostname, port);

        info!("🔗 建立持久连接到节点 {}: {}", node_id, addr_str);

        let stream = TcpStream::connect(&addr_str).await?;
        
        info!("✅ 成功建立持久连接到节点 {}", node_id);

        Ok(Self {
            stream,
            node_id,
            connected_at: Instant::now(),
        })
    }

    // ↓ 添加新的 Pompe 交易发送方法 ↓
    // ↓ 修改 send_pompe_transaction 方法，添加更多调试信息 ↓
    pub async fn send_pompe_transaction(&mut self, transaction: &TestTransaction, client_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        // 检查是否启用 Pompe
        let pompe_enabled = std::env::var("POMPE_ENABLE")
            .unwrap_or_else(|_| "false".to_string())
            .parse()
            .unwrap_or(false);
        
        info!("🔧 Pompe 启用状态: {}", pompe_enabled);
            
        if pompe_enabled {
            info!("🎯 使用 Pompe 模式发送交易 ID: {}", transaction.id);
            
            // 构建客户端消息 - 使用特殊的消息类型标识
            let client_message = ClientMessage {
                message_type: "pompe_transaction".to_string(), // ← 关键标识
                transaction: Some(transaction.clone()),
                client_id: client_id.to_string(),
            };

            let serialized = serde_json::to_vec(&client_message)?;
            let message_length = serialized.len() as u32;
            
            // 发送消息长度（4字节）+ 消息内容
            self.stream.write_all(&message_length.to_be_bytes()).await?;
            self.stream.write_all(&serialized).await?;
            self.stream.flush().await?;

            info!("📤 Pompe 交易已发送: ID={}, Size={}bytes", transaction.id, serialized.len());
        } else {
            info!("📨 使用标准模式发送交易 ID: {}", transaction.id);
            // 使用原有方式发送
            self.send_transaction(transaction, client_id).await?;
        }
        
        Ok(())
    }
    // ↑ Pompe 交易发送方法结束 ↑

    pub async fn send_transaction(&mut self, transaction: &TestTransaction, client_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        let client_message = ClientMessage {
            message_type: "pompe_transaction".to_string(),
            transaction: Some(transaction.clone()),
            client_id: client_id.to_string(),
        };

        let serialized = serde_json::to_vec(&client_message)?;
        let message_length = serialized.len() as u32;
        
        // 发送消息长度（4字节）+ 消息内容
        self.stream.write_all(&message_length.to_be_bytes()).await?;
        self.stream.write_all(&serialized).await?;
        self.stream.flush().await?;

        Ok(())
    }

    // pub async fn send_batch(&mut self, transactions: &[TestTransaction], client_id: &str) -> Result<usize, Box<dyn std::error::Error>> {
    //     let mut sent_count = 0;
        
    //     for transaction in transactions {
    //         match self.send_transaction(transaction, client_id).await {
    //             Ok(_) => sent_count += 1,
    //             Err(e) => {
    //                 warn!("发送交易 {} 到节点 {} 失败: {}", transaction.id, self.node_id, e);
    //                 break;
    //             }
    //         }
    //     }
    //     info!("已成功发送 {} 个交易到节点 {}", sent_count, self.node_id);
    //     Ok(sent_count)
    // }

    // Pompe mode
    pub async fn send_batch(&mut self, transactions: &[TestTransaction], client_id: &str) -> Result<usize, Box<dyn std::error::Error>> {
        // 预先序列化所有交易到一个缓冲区
        let mut batch_buffer = Vec::new();
        let is_pompe = false; /////// 调试修改点

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
        // 一次性发送所有数据
        self.stream.write_all(&batch_buffer).await?;
        self.stream.flush().await?;
        
        Ok(transactions.len())
    }

    pub fn uptime(&self) -> Duration {
        self.connected_at.elapsed()
    }
}

pub struct ClientNode {
    client_id: String,
    connections: HashMap<usize, PersistentConnection>,
    tx_generator: TransactionGenerator,
    stats: ClientStats,
}

impl ClientNode {
    pub fn new(client_id: String) -> Self {
        info!("🚀 初始化客户端: {}", client_id);
        
        let tx_generator = TransactionGenerator::new(client_id.clone());

        Self {
            client_id,
            connections: HashMap::new(),
            tx_generator,
            stats: ClientStats::default(),
        }
    }

    // 建立到所有节点的持久连接
    pub async fn establish_connections(&mut self, node_least_id: usize, node_num: usize) -> Result<(), Box<dyn std::error::Error>> {
        info!("🌐 建立到所有节点的持久连接...");

        for node_id in node_least_id..(node_least_id + node_num) {
            match PersistentConnection::new(node_id).await {
                Ok(conn) => {
                    self.connections.insert(node_id, conn);
                    info!("✅ 连接到节点 {} 成功", node_id);
                }
                Err(e) => {
                    error!("❌ 连接到节点 {} 失败: {}", node_id, e);
                    // 继续尝试连接其他节点
                }
            }
        }

        info!("🎯 成功建立 {} 个持久连接", self.connections.len());
        Ok(())
    }

    pub async fn send_batch_to_node(&mut self, node_id: usize, transactions: Vec<TestTransaction>) -> Result<usize, Box<dyn std::error::Error>> {
        if let Some(connection) = self.connections.get_mut(&node_id) {
            match connection.send_batch(&transactions, &self.client_id).await {
                Ok(sent_count) => {
                    self.stats.record_sent(sent_count as u64);
                    self.stats.record_confirmed(sent_count as u64); // 假设都成功
                    Ok(sent_count)
                }
                Err(e) => {
                    error!("❌ 批量发送到节点 {} 失败: {}", node_id, e);
                    self.stats.record_failed(transactions.len() as u64);
                    
                    // 尝试重新连接
                    info!("🔄 尝试重新连接到节点 {}", node_id);
                    match PersistentConnection::new(node_id).await {
                        Ok(new_conn) => {
                            self.connections.insert(node_id, new_conn);
                            info!("✅ 重新连接到节点 {} 成功", node_id);
                        }
                        Err(reconnect_err) => {
                            error!("❌ 重新连接到节点 {} 失败: {}", node_id, reconnect_err);
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

    // ↓ 修改批量发送方法以支持 Pompe ↓
    // pub async fn send_batch_to_node(&mut self, node_id: usize, transactions: Vec<TestTransaction>) -> Result<usize, Box<dyn std::error::Error>> {
    //     if let Some(connection) = self.connections.get_mut(&node_id) {
    //         let mut sent_count = 0;
            
    //         for transaction in &transactions {
    //             // ↓ 修改这里使用 Pompe 发送 ↓
    //             match connection.send_pompe_transaction(transaction, &self.client_id).await {
    //                 Ok(_) => sent_count += 1,
    //                 Err(e) => {
    //                     warn!("发送交易 {} 到节点 {} 失败: {}", transaction.id, node_id, e);
    //                     break;
    //                 }
    //             }
    //             // ↑ Pompe 发送结束 ↑
    //         }
            
    //         if sent_count > 0 {
    //             self.stats.record_sent(sent_count as u64);
    //             self.stats.record_confirmed(sent_count as u64);
    //             info!("✅ 成功发送 {} 个交易到节点 {}", sent_count, node_id);
    //         }
            
    //         Ok(sent_count)
    //     } else {
    //         error!("❌ 没有到节点 {} 的连接", node_id);
    //         Err("没有连接".into())
    //     }
    // }
    // ↑ 批量发送修改结束 ↑

    // 高效的负载测试 - 使用批量发送
    // pub async fn run_load_test(&mut self, config: LoadTestConfig, node_least_id: usize, node_num: usize) {
    //     info!("🚀 开始高效负载测试 - TPS目标: {}, 持续时间: {}秒", 
    //           config.target_tps, config.duration_secs);

    //     // 建立连接
    //     if let Err(e) = self.establish_connections(node_least_id, node_num).await {
    //         error!("❌ 建立连接失败: {}", e);
    //         return;
    //     }

    //     let batch_size = std::cmp::max(1, config.target_tps / 10); // 每批次大小
    //     let batch_interval = Duration::from_secs_f64(batch_size as f64 / config.target_tps as f64);
    //     let end_time = Instant::now() + Duration::from_secs(config.duration_secs);

    //     let mut total_sent = 0;
    //     let mut batch_counter = 0;

    //     while Instant::now() < end_time {
    //         // 生成一批交易
    //         let transactions = self.tx_generator.generate_batch(batch_size as usize);
            
    //         // 轮询发送到不同节点
    //         let target_node = (batch_counter % node_num) + node_least_id;
            
    //         match self.send_batch_to_node(target_node, transactions).await {
    //             Ok(sent_count) => {
    //                 total_sent += sent_count;
    //                 info!("📦 批次 {} 发送 {} 个交易到节点 {}", batch_counter + 1, sent_count, target_node);
    //             }
    //             Err(e) => {
    //                 warn!("❌ 批次 {} 发送失败: {}", batch_counter + 1, e);
    //             }
    //         }

    //         batch_counter += 1;

    //         // 每1000个交易输出一次统计
    //         if total_sent >= 1000 && total_sent % 1000 == 0 {
    //             self.stats.log_summary();
    //         }

    //         tokio::time::sleep(batch_interval).await;
    //     }

    //     info!("🏁 高效负载测试完成，总计发送 {} 个交易", total_sent);
    //     self.stats.log_summary();
    // }

    // 关键修改：对每个节点并发发送交易
    pub async fn run_load_test(&mut self, config: LoadTestConfig, node_least_id: usize, node_num: usize) {
        info!("开始负载测试 - TPS目标: {}, 持续时间: {}秒", 
            config.target_tps, config.duration_secs);

        // 建立连接
        if let Err(e) = self.establish_connections(node_least_id, node_num).await {
            error!("建立连接失败: {}", e);
            return;
        }

        let batch_size = std::cmp::max(100, config.target_tps / 5);
        let batch_interval = Duration::from_millis(200);
        let end_time = Instant::now() + Duration::from_secs(config.duration_secs);
        // let batch_size = std::cmp::max(50, config.target_tps / 10); // 每批次大小
    //     let batch_interval = Duration::from_secs_f64(batch_size as f64 / config.target_tps as f64);
    //     let end_time = Instant::now() + Duration::from_secs(config.duration_secs);


        let mut total_sent = 0;
        let mut batch_counter = 0;

        while Instant::now() < end_time {
            // 为每个节点顺序发送，避免并发借用问题
            for node_offset in 0..node_num {
                let node_id = node_least_id + node_offset;
                let transactions = self.tx_generator.generate_batch(batch_size as usize);
                
                match self.send_batch_to_node(node_id, transactions).await {
                    Ok(sent_count) => {
                        total_sent += sent_count;
                        info!("批次 {} 发送 {} 个交易到节点 {}", batch_counter + 1, sent_count, node_id);
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

        info!("负载测试完成，总计发送 {} 个交易", total_sent);
        self.stats.log_summary();
    }


    // 高效的交互模式 - 保持连接
    pub async fn run_interactive_mode(&mut self, node_least_id: usize, node_num: usize) {
        info!("🎮 进入高效交互模式");

        // 建立连接
        if let Err(e) = self.establish_connections(node_least_id, node_num).await {
            error!("❌ 建立连接失败: {}", e);
            return;
        }

        let mut tx_counter = 0;
        
        // ↓ 添加调试信息 ↓
        info!("🚀 开始发送交易循环...");
        
        loop {
            // 每次发送一小批交易（比如5个）来提高效率
            let batch_size = 5;
            let transactions = self.tx_generator.generate_batch(batch_size);
            let target_node = (tx_counter / batch_size) % node_num + node_least_id;

            // ↓ 添加详细日志 ↓
            info!("📤 准备发送批次到节点 {}, 包含 {} 个交易", target_node, transactions.len());
            
            match self.send_batch_to_node(target_node, transactions).await {
                Ok(sent_count) => {
                    tx_counter += sent_count;
                    info!("✅ 成功发送 {} 个交易到节点 {}, 总计: {}", sent_count, target_node, tx_counter);
                }
                Err(e) => {
                    error!("❌ 发送批次失败到节点 {}: {}", target_node, e);
                    
                    // ↓ 添加重试逻辑 ↓
                    warn!("🔄 等待5秒后重试...");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            }

            // 每100个交易输出一次统计
            if tx_counter >= 100 && tx_counter % 100 == 0 {
                self.stats.log_summary();
            }

            // ↓ 修改等待时间，让日志更清晰 ↓
            tokio::time::sleep(Duration::from_millis(1000)).await; // 改为1秒一批
        }
    }

}

pub struct LoadTestConfig {
    pub target_tps: u32,
    pub duration_secs: u64,
}

fn setup_tracing_logger(mode : &str) {
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

    info!("🏃 启动高效客户端节点: {}", client_id);

    let mut client_node = ClientNode::new(client_id);

    // 等待共识节点启动
    info!("⏳ 等待共识节点启动...");
    tokio::time::sleep(Duration::from_secs(15)).await;

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

            client_node.run_load_test(config, node_least_id, node_num).await;
            info!("✅ 负载测试完成，保持客户端运行状态...");
        }
        _ => {
            client_node.run_interactive_mode(node_least_id, node_num).await;
        }
    }

    Ok(())
}