// hotstuff_runner/src/bin/docker_node.rs
//! Docker环境中的单节点启动程序

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
};
use std::sync::{Arc, Mutex}; // 统一使用 std::sync::Mutex
use tokio::sync::mpsc;
use std::collections::{VecDeque, HashMap};
use std::net::SocketAddr;
use std::env;
use std::fs;
use std::fs::{File, create_dir_all};
use tracing::{info, error, warn};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt};
use std::time::Duration;
use std::thread;
use ed25519_dalek::SigningKey;
// 添加异步支持
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use serde::{Serialize, Deserialize};

// 交易池和性能统计 - 统一使用 std::sync::Mutex
type TransactionPool = Arc<Mutex<VecDeque<TestTransaction>>>;

#[derive(Debug, Clone)]
struct PerformanceStats {
    submitted_count: u64,
    confirmed_count: u64,
    start_time: Option<std::time::Instant>,
}

impl PerformanceStats {
    fn new() -> Self {
        Self {
            submitted_count: 0,
            confirmed_count: 0,
            start_time: None,
        }
    }

    fn record_submitted(&mut self) {
        if self.start_time.is_none() {
            self.start_time = Some(std::time::Instant::now());
        }
        self.submitted_count += 1;
    }

    fn record_confirmed(&mut self, count: u64) {
        self.confirmed_count += count;
    }

    fn get_tps(&self) -> f64 {
        if let Some(start) = self.start_time {
            let elapsed = start.elapsed().as_secs_f64();
            if elapsed > 0.0 {
                return self.confirmed_count as f64 / elapsed;
            }
        }
        0.0
    }
}

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

fn setup_tracing_logger(node_id: usize) {
    // 创建日志目录
    create_dir_all("logs").expect("无法创建日志目录");
    
    // 清理旧的日志文件
    let _ = fs::remove_file(format!("logs/node{}.log", node_id));

    // 节点专用日志文件
    let node_log_file = File::options()
        .create(true)
        .append(true)
        .open(format!("logs/node{}.log", node_id))
        .expect("无法打开节点日志文件");
    
    // 共享的 main.log 文件（直接使用 File，不用 Arc<Mutex>）
    let main_log_file = File::options()
        .create(true)
        .append(true)
        .open("logs/main.log")
        .expect("无法打开 main.log 文件");
    
    let result = tracing_subscriber::registry()
        .with(
            fmt::layer()
                .with_writer(std::io::stdout)
                .with_target(true)
                .with_thread_ids(true)
                .with_ansi(true)
        )  // 控制台输出
        .with(
            fmt::layer()
                .with_writer(node_log_file)
                .with_target(true)
                .with_thread_ids(true)
                .with_ansi(false)
        )  // 节点文件输出
        .with(
            fmt::layer()
                .with_writer(main_log_file)
                .with_target(true)
                .with_thread_ids(true)
                .with_ansi(false)
        )  // 主日志文件输出
        .try_init();
    
    match result {
        Ok(_) => info!("📝 日志系统初始化成功"),
        Err(_) => warn!("⚠️ 日志系统已经初始化过了，跳过"),
    }
}

// 共识层模拟处理器
async fn consensus_processor(
    node_id: usize,
    mut consensus_receiver: mpsc::UnboundedReceiver<Vec<TestTransaction>>,
    stats: Arc<Mutex<PerformanceStats>>
) {
    while let Some(transactions) = consensus_receiver.recv().await {
        // 模拟共识处理时间
        tokio::time::sleep(Duration::from_millis(200)).await;
        
        let tx_count = transactions.len();
        info!("🔄 节点 {} 共识处理完成 {} 个交易", node_id, tx_count);
        
        // 这里可以集成真正的 HotStuff 共识
        // hotStuff_node.process_transactions(transactions).await;
    }
}

fn create_peer_address(i: usize) -> Result<SocketAddr, Box<dyn std::error::Error>> {
    let hostname = format!("node{}", i);
    let port = 10000 + i as u16;
    let addr_str = format!("{}:{}", hostname, port);
    
    info!("🔍 尝试解析地址: {}", addr_str);
    
    // 尝试DNS解析
    match std::net::ToSocketAddrs::to_socket_addrs(&addr_str) {
        Ok(mut addrs) => {
            if let Some(addr) = addrs.next() {
                info!("✅ 成功解析地址: {} -> {}", addr_str, addr);
                Ok(addr)
            } else {
                Err(format!("没有找到地址: {}", addr_str).into())
            }
        }
        Err(e) => {
            warn!("⚠️ DNS解析失败 {}: {}", addr_str, e);
            
            // 回退方案：直接使用IP地址
            let fallback_addr = format!("127.0.0.1:{}", port);
            info!("🔄 尝试回退地址: {}", fallback_addr);
            
            fallback_addr.parse::<SocketAddr>()
                .map_err(|e| format!("回退地址解析失败: {}", e).into())
        }
    }
}

// 客户端监听器 - 将交易加入共享交易池
async fn start_client_listener(
    node_id: usize, 
    port: u16, 
    shared_tx_queue: Arc<Mutex<Vec<String>>>, // 使用共享队列
    stats: Arc<Mutex<PerformanceStats>>
) -> Result<(), Box<dyn std::error::Error>> {
    let addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&addr).await?;
    
    info!("🎧 节点 {} 开始监听客户端连接: {}", node_id, addr);
    
    loop {
        match listener.accept().await {
            Ok((mut socket, client_addr)) => {
                // info!("📞 节点 {} 接收到客户端连接: {}", node_id, client_addr);
                
                let node_id_copy = node_id;
                let tx_queue_clone = shared_tx_queue.clone();
                let stats_clone = stats.clone();
                
                tokio::spawn(async move {
                    if let Err(e) = handle_client_connection(node_id_copy, &mut socket, tx_queue_clone, stats_clone).await {
                        error!("节点 {} 处理客户端连接失败: {}", node_id_copy, e);
                    }
                });
            }
            Err(e) => {
                error!("节点 {} 接受客户端连接失败: {}", node_id, e);
            }
        }
    }
}

async fn handle_client_connection(
    node_id: usize, 
    socket: &mut TcpStream,
    shared_tx_queue: Arc<Mutex<Vec<String>>>,
    stats: Arc<Mutex<PerformanceStats>>
) -> Result<(), Box<dyn std::error::Error>> {
    let mut length_buf = [0u8; 4];
    let mut tx_count = 0;
    
    loop {
        match socket.read_exact(&mut length_buf).await {
            Ok(_) => {
                let message_length = u32::from_be_bytes(length_buf) as usize;
                
                if message_length > 1024 * 1024 {
                    break;
                }
                
                let mut message_buf = vec![0u8; message_length];
                socket.read_exact(&mut message_buf).await?;
                
                if let Ok(client_message) = serde_json::from_slice::<ClientMessage>(&message_buf) {
                    if let Some(transaction) = client_message.transaction {
                        tx_count += 1;
                        
                        let tx_string = format!("{}:{}->{}:{}", transaction.id, transaction.from, transaction.to, transaction.amount);
                        
                        // 使用 spawn_blocking 来处理同步的 Mutex 操作
                        let queue_clone = shared_tx_queue.clone();
                        let tx_string_clone = tx_string.clone();
                        tokio::task::spawn_blocking(move || {
                            let mut queue = queue_clone.lock().unwrap();
                            queue.push(tx_string_clone);
                            
                            // 限制交易池大小
                            if queue.len() > 10000 {
                                queue.remove(0);
                            }
                        }).await.unwrap();
                        
                        // 更新统计 - 也使用 spawn_blocking
                        let stats_clone = stats.clone();
                        tokio::task::spawn_blocking(move || {
                            let mut stats_guard = stats_clone.lock().unwrap();
                            stats_guard.record_submitted();
                        }).await.unwrap();
                        
                        if tx_count % 10 == 0 {
                            let queue_clone = shared_tx_queue.clone();
                            let stats_clone = stats.clone();
                            
                            let (pool_size, current_tps) = tokio::task::spawn_blocking(move || {
                                let pool_size = queue_clone.lock().unwrap().len();
                                let current_stats = stats_clone.lock().unwrap();
                                let tps = current_stats.submitted_count as f64 / 
                                    current_stats.start_time.unwrap_or(std::time::Instant::now()).elapsed().as_secs_f64();
                                (pool_size, tps)
                            }).await.unwrap();

                            // info!("📊 Node {} 接收统计: {} 个交易, 交易池: {}, 提交TPS: {:.1}", 
                            //       node_id, tx_count, pool_size, current_tps);
                            info!("📊 Node {} 接收统计: {} 个交易, 交易池: {}", 
                                  node_id, tx_count, pool_size);
                        }
                        
                        // 发送简单确认响应
                        // let response = serde_json::json!({
                        //     "status": "received",
                        //     "transaction_id": transaction.id,
                        //     "node_id": node_id
                        // });
                        
                        // let response_bytes = serde_json::to_vec(&response)?;
                        // let response_length = response_bytes.len() as u32;
                        
                        // if socket.write_all(&response_length.to_be_bytes()).await.is_ok() {
                        //     let _ = socket.write_all(&response_bytes).await;
                        //     let _ = socket.flush().await;
                        // }
                    }
                }
            }
            Err(_) => {
                if tx_count > 0 {
                    let pool_size = tokio::task::spawn_blocking({
                        let queue_clone = shared_tx_queue.clone();
                        move || queue_clone.lock().unwrap().len()
                    }).await.unwrap();
                    
                    // info!("📋 Node {} 客户端断开，本次接收 {} 个交易，交易池: {}", node_id, tx_count, pool_size);
                }
                break;
            }
        }
    }
    
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 从环境变量读取配置
    let node_id: usize = env::var("NODE_ID")
        .unwrap_or_else(|_| "9".to_string())
        .parse()
        .expect("NODE_ID 必须是数字");
    
    let my_port: u16 = env::var("NODE_PORT")
        .unwrap_or_else(|_| (10000 + node_id).to_string())
        .parse()
        .expect("NODE_PORT 必须是数字");

    let node_least_id: usize = env::var("NODE_LEAST_ID")
        .unwrap_or_else(|_| "1".to_string())
        .parse()
        .expect("NODE_LEAST_ID 必须是数字");
    let node_num: usize = env::var("NODE_NUM")
        .unwrap_or_else(|_| "4".to_string())
        .parse()
        .expect("NODE_NUM 必须是数字");

    // 初始化日志系统
    setup_tracing_logger(node_id);
    
    info!("🚀 启动Docker节点 {} (端口: {})", node_id, my_port);
    
    // 生成密钥（使用确定性种子以保持一致性）
    let secret_bytes: [u8; 32] = [(node_id + 1) as u8; 32];
    let signing_key = SigningKey::from_bytes(&secret_bytes);
    let my_verifying_key = VerifyingKey::from(signing_key.verifying_key());
    
    info!("🔑 节点密钥: {:?}", my_verifying_key.to_bytes()[0..8].to_vec());
    
    // 创建所有节点的密钥和地址映射
    let mut peer_addrs = HashMap::new();
    let mut all_verifying_keys = Vec::new();

    for i in node_least_id..=(node_least_id+node_num-1) {
    // for i in 0..4 {
        let peer_secret: [u8; 32] = [(i + 1) as u8; 32];
        let peer_signing_key = SigningKey::from_bytes(&peer_secret);
        let peer_verifying_key = VerifyingKey::from(peer_signing_key.verifying_key());
        
        let addr = create_peer_address(i).expect("无法创建对等节点地址");
        
        peer_addrs.insert(peer_verifying_key, addr);
        all_verifying_keys.push(peer_verifying_key);
        
        info!("📋 节点 {}: {:?} -> {}", 
              i, 
              peer_verifying_key.to_bytes()[0..4].to_vec(), 
              addr);
    }

    
    // 创建验证者集合更新
    let init_app_state_updates = AppStateUpdates::new();
    let mut init_validator_set_updates = ValidatorSetUpdates::new();
    for key in &all_verifying_keys {
        init_validator_set_updates.insert(*key, Power::new(1));
    }
    
    info!("👥 验证者集合: {} 个验证者", all_verifying_keys.len());
    
    // 创建TCP网络配置
    let my_addr: SocketAddr = format!("0.0.0.0:{}", my_port)
        .parse()
        .expect("无效的本地地址");
    
    let tcp_config = TcpNetworkConfig {
        my_addr,
        peer_addrs,
        my_key: my_verifying_key,
    };
    
    // 创建TCP网络
    info!("🌐 创建TCP网络...");
    let tcp_network = match TcpNetwork::new(tcp_config) {
        Ok(network) => network,
        Err(e) => {
            error!("❌ 创建TCP网络失败: {}", e);
            return Err(e.into());
        }
    };
    
    info!("✅ TCP网络创建成功");
    
    // 创建真正共享的交易队列 - 统一使用 std::sync::Mutex
    let shared_tx_queue: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let performance_stats: Arc<Mutex<PerformanceStats>> = Arc::new(Mutex::new(PerformanceStats::new()));
    
    // 启动客户端监听器
    let client_listener_node_id = node_id;
    let client_listener_port = my_port - 1000; // Notice：当前客户端监听器端口为节点端口减1000
    let tx_queue_for_listener = shared_tx_queue.clone();
    let stats_for_listener = performance_stats.clone();
    
    tokio::spawn(async move {
        if let Err(e) = start_client_listener(
            client_listener_node_id, 
            client_listener_port, 
            tx_queue_for_listener,
            stats_for_listener
        ).await {
            error!("客户端监听器失败: {}", e);
        }
    });
    
    // 等待其他节点启动
    info!("⏳ 等待其他节点启动...");
    tokio::time::sleep(Duration::from_secs(10)).await;

    
    // 创建节点
    info!("🏗️ 创建HotStuff节点...");
    
    let _node = Node::new(
        node_id,
        signing_key.clone(),
        tcp_network.clone(),
        init_app_state_updates.clone(),
        init_validator_set_updates.clone(),
        shared_tx_queue.clone(),  // 直接使用共享队列
    );

    let node_for_main_loop = Arc::new(_node);
    let queue_for_main_loop = shared_tx_queue.clone();
    let stats_for_main_loop = performance_stats.clone();

    
    // 主循环：从共享队列中提取交易进行打包
    loop {
        tokio::time::sleep(Duration::from_millis(500)).await;
        
        // 使用 spawn_blocking 来处理同步操作
        let transactions = tokio::task::spawn_blocking({
            let queue_clone = queue_for_main_loop.clone();
            move || {
                let mut queue = queue_clone.lock().unwrap();
                let batch_size = std::cmp::min(queue.len(), 100);
                
                let mut batch = Vec::new();
                for _ in 0..batch_size {
                    if let Some(tx) = queue.pop() {
                        batch.push(tx);
                    }
                }
                batch
            }
        }).await.unwrap();
        
        if !transactions.is_empty() {
            let tx_count = transactions.len();
            
            info!("📦 节点 {} 从共享队列提取了 {} 个交易进行打包", node_id, tx_count);
            
            // 更新统计
            tokio::task::spawn_blocking({
                let stats_clone = stats_for_main_loop.clone();
                move || {
                    let mut stats_guard = stats_clone.lock().unwrap();
                    stats_guard.record_confirmed(tx_count as u64);
                }
            }).await.unwrap();
        }
    }
}