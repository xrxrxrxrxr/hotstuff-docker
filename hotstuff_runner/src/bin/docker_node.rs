// hotstuff_runner/src/bin/docker_node.rs - 无锁版本
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
    stats::PerformanceStats
};
use std::sync::Arc;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::env;
use std::fs::{File, create_dir_all};
use tracing::{info, error, warn};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt};
use std::time::Duration;
use std::thread;
use ed25519_dalek::SigningKey;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use serde::{Serialize, Deserialize};
use crossbeam::queue::SegQueue;
use std::sync::atomic::{AtomicU64, Ordering};

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
    create_dir_all("logs").expect("无法创建日志目录");
    
    let _ = std::fs::remove_file(format!("logs/node{}.log", node_id));

    let node_log_file = File::options()
        .create(true)
        .append(true)
        .open(format!("logs/node{}.log", node_id))
        .expect("无法打开节点日志文件");
    
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
        Ok(_) => info!("日志系统初始化成功"),
        Err(_) => warn!("日志系统已经初始化过了，跳过"),
    }
}

fn create_peer_address(i: usize) -> Result<SocketAddr, Box<dyn std::error::Error>> {
    let hostname = format!("node{}", i);
    let port = 10000 + i as u16;
    let addr_str = format!("{}:{}", hostname, port);
    
    info!("尝试解析地址: {}", addr_str);
    
    match std::net::ToSocketAddrs::to_socket_addrs(&addr_str) {
        Ok(mut addrs) => {
            if let Some(addr) = addrs.next() {
                info!("成功解析地址: {} -> {}", addr_str, addr);
                Ok(addr)
            } else {
                Err(format!("没有找到地址: {}", addr_str).into())
            }
        }
        Err(e) => {
            warn!("DNS解析失败 {}: {}", addr_str, e);
            
            let fallback_addr = format!("127.0.0.1:{}", port);
            info!("尝试回退地址: {}", fallback_addr);
            
            fallback_addr.parse::<SocketAddr>()
                .map_err(|e| format!("回退地址解析失败: {}", e).into())
        }
    }
}

// 客户端监听器 - 使用无锁队列
async fn start_client_listener(
    node_id: usize, 
    port: u16, 
    shared_tx_queue: Arc<SegQueue<String>>, // 无锁队列
    stats: Arc<PerformanceStats>
) -> Result<(), Box<dyn std::error::Error>> {
    let addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&addr).await?;
    
    info!("节点 {} 开始监听客户端连接: {}", node_id, addr);
    
    loop {
        match listener.accept().await {
            Ok((mut socket, client_addr)) => {
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
    shared_tx_queue: Arc<SegQueue<String>>, // 无锁队列
    stats: Arc<PerformanceStats>
) -> Result<(), Box<dyn std::error::Error>> {
    let mut length_buf = [0u8; 4];
    let mut tx_count = 0;

    info!("Node {} 新的客户端连接建立", node_id);
    
    loop {
        match socket.read_exact(&mut length_buf).await {
            Ok(_) => {
                let message_length = u32::from_be_bytes(length_buf) as usize;
                
                if message_length > 1024 * 1024 {
                    warn!("Node {} 消息过大: {}, 断开连接", node_id, message_length);
                    break;
                }
                
                let mut message_buf = vec![0u8; message_length];
                socket.read_exact(&mut message_buf).await?;
                
                if let Ok(client_message) = serde_json::from_slice::<ClientMessage>(&message_buf) {
                    if let Some(transaction) = client_message.transaction {
                        tx_count += 1;

                        let tx_string = format!("{}:{}->{}:{}", transaction.id, transaction.from, transaction.to, transaction.amount);
                        
                        // 直接推入无锁队列 - 无需异步操作
                        shared_tx_queue.push(tx_string);
                        
                        // 限制队列大小
                        if shared_tx_queue.len() > 10000 {
                            let _ = shared_tx_queue.pop(); // 移除最老的交易
                        }
                        
                        // 更新统计
                        stats.record_submitted();
                        
                        // 每100个交易显示统计
                        if tx_count % 100 == 0 {
                            let pool_size = shared_tx_queue.len();
                            let current_tps = stats.get_submission_tps();

                            info!("Node {} 接收统计: {} 个交易, 交易池: {}, 提交 TPS: {:.1}", 
                                  node_id, tx_count, pool_size, current_tps);
                        }
                    }
                    else {
                        warn!("Node {} 收到的消息没有交易数据", node_id);
                    }
                }
                else {
                    error!("Node {} JSON解析失败，消息长度: {}", node_id, message_length);
                }
            }
            Err(e) => {
                if tx_count > 0 {
                    let pool_size = shared_tx_queue.len();
                    
                    info!("Node {} 客户端断开 ({}), 本次接收 {} 个交易，最终队列: {}", 
                          node_id, e, tx_count, pool_size);
                } else {
                    info!("Node {} 客户端断开 ({}), 本次未接收交易", node_id, e);
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
    
    info!("启动Docker节点 {} (端口: {})", node_id, my_port);
    
    // 生成密钥
    let secret_bytes: [u8; 32] = [(node_id + 1) as u8; 32];
    let signing_key = SigningKey::from_bytes(&secret_bytes);
    let my_verifying_key = VerifyingKey::from(signing_key.verifying_key());
    
    info!("节点密钥: {:?}", my_verifying_key.to_bytes()[0..8].to_vec());
    
    // 创建所有节点的密钥和地址映射
    let mut peer_addrs = HashMap::new();
    let mut all_verifying_keys = Vec::new();

    for i in node_least_id..=(node_least_id+node_num-1) {
        let peer_secret: [u8; 32] = [(i + 1) as u8; 32];
        let peer_signing_key = SigningKey::from_bytes(&peer_secret);
        let peer_verifying_key = VerifyingKey::from(peer_signing_key.verifying_key());
        
        let addr = create_peer_address(i).expect("无法创建对等节点地址");
        
        peer_addrs.insert(peer_verifying_key, addr);
        all_verifying_keys.push(peer_verifying_key);
        
        info!("节点 {}: {:?} -> {}", 
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
    
    info!("验证者集合: {} 个验证者", all_verifying_keys.len());
    
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
    info!("创建TCP网络...");
    let tcp_network = match TcpNetwork::new(tcp_config) {
        Ok(network) => network,
        Err(e) => {
            error!("创建TCP网络失败: {}", e);
            return Err(e.into());
        }
    };
    
    info!("TCP网络创建成功");
    
    // 创建无锁的共享数据结构
    let shared_tx_queue: Arc<SegQueue<String>> = Arc::new(SegQueue::new());
    let performance_stats: Arc<PerformanceStats> = Arc::new(PerformanceStats::new());
    
    // 启动客户端监听器
    let client_listener_node_id = node_id;
    let client_listener_port = my_port - 1000;
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
    info!("等待其他节点启动...");
    tokio::time::sleep(Duration::from_secs(10)).await;

    // 创建节点
    info!("创建HotStuff节点...");
    
    let _node = Node::new(
        node_id,
        signing_key.clone(),
        tcp_network.clone(),
        init_app_state_updates.clone(),
        init_validator_set_updates.clone(),
        shared_tx_queue.clone(),
        performance_stats.clone(),
    );

    // 主监控循环 - 使用无锁访问
    let queue_for_monitoring = shared_tx_queue.clone();
    let stats_for_monitoring = performance_stats.clone();

    let mut loop_counter = 0;
    let mut last_queue_size = 0;
    let mut last_confirmed_txs = 0;
    
    loop {
        tokio::time::sleep(Duration::from_secs(5)).await;
        loop_counter += 1;
        
        // 无锁访问队列大小
        let current_queue_size = queue_for_monitoring.len();
        
        // 无锁访问统计数据
        let submission_tps = stats_for_monitoring.get_submission_tps();
        let consensus_tps = stats_for_monitoring.get_end_to_end_tps();
        let total_submitted = stats_for_monitoring.get_submitted_count();
        let total_confirmed_txs = stats_for_monitoring.get_confirmed_transactions();
        let total_confirmed_blocks = stats_for_monitoring.get_confirmed_blocks();
        let recent_tps = stats_for_monitoring.get_recent_tps(30.0);
        
        // 交易确认进度提醒
        if total_confirmed_txs != last_confirmed_txs {
            let confirmed_diff = total_confirmed_txs - last_confirmed_txs;
            info!("Node {} 新确认交易: +{} (总计: {})", 
                node_id, confirmed_diff, total_confirmed_txs);
            last_confirmed_txs = total_confirmed_txs;
        }
        
        // 队列大小变化提醒
        if current_queue_size != last_queue_size {
            if current_queue_size > last_queue_size {
                info!("Node {} 队列增长: {} -> {} (+{})", 
                      node_id, last_queue_size, current_queue_size, 
                      current_queue_size - last_queue_size);
            } else {
                info!("Node {} 队列减少: {} -> {} (-{})", 
                      node_id, last_queue_size, current_queue_size, 
                      last_queue_size - current_queue_size);
            }
            last_queue_size = current_queue_size;
        }

        // 每50个循环进行深度系统检查
        if loop_counter % 50 == 0 {
            info!("Node {} 系统资源深度检查 (第{}次):", node_id, loop_counter);
            
            let pid = std::process::id();
            info!("  进程ID: {}", pid);
            
            if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
                for line in status.lines() {
                    if line.starts_with("VmRSS:") || line.starts_with("VmSize:") {
                        info!("  {}", line);
                    }
                }
            }
            
            if let Ok(fd_count) = std::fs::read_dir("/proc/self/fd") {
                let fd_num = fd_count.count();
                info!("  文件描述符: {}", fd_num);
                
                if fd_num > 100 {
                    warn!("文件描述符数量异常: {}", fd_num);
                }
            }
            
            info!("  运行时长: {} 秒", loop_counter * 5);
            info!("  平均区块速度: {:.2} 区块/分钟", 
                total_confirmed_blocks as f64 / (loop_counter * 5) as f64 * 60.0);
        }
        
        // 检查队列积压情况
        if current_queue_size > 1000 {
            warn!("Node {} 交易队列积压严重: {} 个交易", node_id, current_queue_size);
        }
        
        // 使用静态变量检查停滞（简化实现）
        static mut LAST_SUBMITTED_TXS: u64 = 0;
        static mut LAST_CONFIRMED_TXS: u64 = 0;

        let last_submitted = unsafe { LAST_SUBMITTED_TXS };
        let last_confirmed = unsafe { LAST_CONFIRMED_TXS };

        if total_submitted == last_submitted && current_queue_size > 0 {
            warn!("Node {} 可能出现交易提交停滞", node_id);
        }

        if total_confirmed_txs == last_confirmed && total_submitted > total_confirmed_txs {
            warn!("Node {} 可能出现共识确认停滞", node_id);
        }

        unsafe { 
            LAST_SUBMITTED_TXS = total_submitted;
            LAST_CONFIRMED_TXS = total_confirmed_txs;
        }
    }
}