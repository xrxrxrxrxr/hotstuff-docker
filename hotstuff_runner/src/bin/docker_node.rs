// hotstuff_runner/src/bin/docker_node.rs
//! Docker环境中的单节点启动程序 - 修复Pompe网络问题

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
    pompe::{PompeManager, load_pompe_config},
};
use std::sync::{Arc, Mutex};
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
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use serde::{Serialize, Deserialize};
use hotstuff_runner::diagnose::run_pompe_network_diagnostic;

type TransactionPool = Arc<Mutex<VecDeque<TestTransaction>>>;

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
    let _ = fs::remove_file(format!("logs/node{}.log", node_id));

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
        Ok(_) => info!("📝 日志系统初始化成功"),
        Err(_) => warn!("⚠️ 日志系统已经初始化过了，跳过"),
    }
}

fn create_peer_address(i: usize) -> Result<SocketAddr, Box<dyn std::error::Error>> {
    let hostname = format!("node{}", i);
    let port = 10000 + i as u16;
    let addr_str = format!("{}:{}", hostname, port);
    
    info!("🔍 尝试解析地址: {}", addr_str);
    
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
            let fallback_addr = format!("127.0.0.1:{}", port);
            info!("🔄 尝试回退地址: {}", fallback_addr);
            fallback_addr.parse::<SocketAddr>()
                .map_err(|e| format!("回退地址解析失败: {}", e).into())
        }
    }
}

async fn start_client_listener(
    node_id: usize, 
    port: u16, 
    shared_tx_queue: Arc<Mutex<Vec<String>>>,
    stats: Arc<Mutex<PerformanceStats>>
) -> Result<(), Box<dyn std::error::Error>> {
    let addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&addr).await?;
    
    info!("🎧 节点 {} 开始监听客户端连接: {}", node_id, addr);
    
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
    shared_tx_queue: Arc<Mutex<Vec<String>>>,
    stats: Arc<Mutex<PerformanceStats>>
) -> Result<(), Box<dyn std::error::Error>> {
    let mut length_buf = [0u8; 4];
    let mut tx_count = 0;

    info!("🔗 Node {} 新的客户端连接建立", node_id);
    
    loop {
        match socket.read_exact(&mut length_buf).await {
            Ok(_) => {
                let message_length = u32::from_be_bytes(length_buf) as usize;
                
                if message_length > 1024 * 1024 {
                    warn!("⚠️ Node {} 消息过大: {}, 断开连接", node_id, message_length);
                    break;
                }
                
                let mut message_buf = vec![0u8; message_length];
                socket.read_exact(&mut message_buf).await?;
                
                if let Ok(client_message) = serde_json::from_slice::<ClientMessage>(&message_buf) {
                    if let Some(transaction) = client_message.transaction {
                        tx_count += 1;
                        let is_pompe = client_message.message_type == "pompe_transaction";
                        let tx_string = format!("{}:{}->{}:{}", transaction.id, transaction.from, transaction.to, transaction.amount);

                        info!("💰 Node {} 接收{} ID={}, {}->{}:{}", 
                              node_id, 
                              if is_pompe { "Pompe交易" } else { "标准交易" },
                              transaction.id, transaction.from, transaction.to, transaction.amount);

                        if is_pompe {
                            let queue_clone = shared_tx_queue.clone();
                            let tx_string_clone = tx_string.clone();
                            
                            let queue_size_after = tokio::task::spawn_blocking(move || {
                                let mut queue = queue_clone.lock().unwrap();
                                queue.push(tx_string_clone);
                                
                                if queue.len() > 10000 {
                                    queue.remove(0);
                                }
                                
                                queue.len()
                            }).await.unwrap();
                            
                            info!("📝 Node {} Pompe交易已入队: {} -> 队列大小: {}", 
                                  node_id, tx_string, queue_size_after);
                        } else {
                            info!("📝 Node {} 标准交易跳过Pompe，直接处理: {}", node_id, tx_string);
                            
                            let queue_clone = shared_tx_queue.clone();
                            let standard_tx = format!("standard:{}", tx_string);
                            
                            tokio::task::spawn_blocking(move || {
                                let mut queue = queue_clone.lock().unwrap();
                                queue.push(standard_tx);
                            }).await.unwrap();
                        }
                        
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
                                let tps = current_stats.get_submission_tps();
                                (pool_size, tps)
                            }).await.unwrap();

                            info!("📊 Node {} 接收统计: {} 个交易, 交易池: {}, 提交 TPS: {:.1}", 
                                  node_id, tx_count, pool_size, current_tps);
                        }
                    }
                    else {
                        warn!("⚠️ Node {} 收到的消息没有交易数据", node_id);
                    }
                }
                else {
                    error!("❌ Node {} JSON解析失败，消息长度: {}", node_id, message_length);
                }
            }
            Err(e) => {
                if tx_count > 0 {
                    let pool_size = tokio::task::spawn_blocking({
                        let queue_clone = shared_tx_queue.clone();
                        move || queue_clone.lock().unwrap().len()
                    }).await.unwrap();
                    
                    info!("📋 Node {} 客户端断开 ({}), 本次接收 {} 个交易，最终队列: {}", 
                          node_id, e, tx_count, pool_size);
                } else {
                    info!("🔌 Node {} 客户端断开 ({}), 本次未接收交易", node_id, e);
                }
                break;
            }
        }
    }
    
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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

    setup_tracing_logger(node_id);
    info!("🚀 启动Docker节点 {} (端口: {})", node_id, my_port);
    
    let secret_bytes: [u8; 32] = [(node_id + 1) as u8; 32];
    let signing_key = SigningKey::from_bytes(&secret_bytes);
    let my_verifying_key = VerifyingKey::from(signing_key.verifying_key());
    
    info!("🔑 节点密钥: {:?}", my_verifying_key.to_bytes()[0..8].to_vec());

    
    // 🚨 关键修复：确保所有节点都在peer_addrs中
    let mut peer_addrs = HashMap::new();
    let mut all_verifying_keys = Vec::new();

    for i in node_least_id..=(node_least_id+node_num-1) {
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

    // 🚨 重要：验证当前节点在peer_addrs中
    if !peer_addrs.contains_key(&my_verifying_key) {
        error!("❌ 当前节点 {} 不在 peer_addrs 中！", node_id);
        return Err("节点配置错误".into());
    }

    info!("👥 验证者集合: {} 个验证者", all_verifying_keys.len());
    info!("🔍 当前节点在验证者集合中: {}", 
          all_verifying_keys.contains(&my_verifying_key));
    
    let init_app_state_updates = AppStateUpdates::new();
    let mut init_validator_set_updates = ValidatorSetUpdates::new();
    for key in &all_verifying_keys {
        init_validator_set_updates.insert(*key, Power::new(1));
    }
    
    let my_addr: SocketAddr = format!("0.0.0.0:{}", my_port)
        .parse()
        .expect("无效的本地地址");
    
    let tcp_config = TcpNetworkConfig {
        my_addr,
        peer_addrs,
        my_key: my_verifying_key,
    };
    
    info!("🌐 创建TCP网络...");
    let tcp_network = match TcpNetwork::new(tcp_config) {
        Ok(network) => network,
        Err(e) => {
            error!("❌ 创建TCP网络失败: {}", e);
            return Err(e.into());
        }
    };
    
    info!("✅ TCP网络创建成功");
    
    let shared_tx_queue: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let performance_stats: Arc<Mutex<PerformanceStats>> = Arc::new(Mutex::new(PerformanceStats::new()));        
    
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
    
    info!("⏳ 等待其他节点启动...");
    tokio::time::sleep(Duration::from_secs(10)).await;

    info!("🏗️ 创建HotStuff节点...");
    
    let _node = Node::new(
        node_id,
        signing_key.clone(),
        tcp_network.clone(),
        init_app_state_updates.clone(),
        init_validator_set_updates.clone(),
        shared_tx_queue.clone(),
        performance_stats.clone(),
    );

    // 🚨 关键修复：创建Pompe管理器时传递正确的节点列表
    let pompe_config = load_pompe_config();
    let pompe_manager = if pompe_config.enable {
        info!("🎯 启用 Pompe BFT，批次大小: {}", pompe_config.batch_size);

        // 🚨 修复：使用所有节点ID创建Pompe网络
        let all_node_ids: Vec<usize> = (node_least_id..=(node_least_id+node_num-1)).collect();
        info!("🔍 Pompe 网络节点列表: {:?}", all_node_ids);

        let pompe = Arc::new(PompeManager::new_with_complete_network(
            node_id,
            all_node_ids, // 🚨 传递完整的节点ID列表
            pompe_config,
            tcp_network.clone(),
        ));
        
        let pompe_clone = Arc::clone(&pompe);
        tokio::spawn(async move {
            if let Err(e) = pompe_clone.start_network_message_loop().await {
                error!("❌ Pompe网络循环启动失败: {}", e);
            }
        });
    
        Some(pompe)
    } else {
        info!("🚫 Pompe BFT 已禁用");
        None
    };

    // tokio::spawn(async move {
    //     tokio::time::sleep(Duration::from_secs(5)).await;
    //     run_pompe_network_diagnostic(node_id, node_least_id, node_num).await;
    // });

    let queue_for_monitoring = shared_tx_queue.clone();
    let stats_for_monitoring = performance_stats.clone();

    let mut loop_counter = 0;
    let mut last_queue_size = 0;
    let mut last_confirmed_txs = 0;

    loop {
        tokio::time::sleep(Duration::from_millis(50)).await; //Fix P
        loop_counter += 1;
        
        let current_queue_size = tokio::task::spawn_blocking({
            let queue_clone = shared_tx_queue.clone();
            move || queue_clone.lock().unwrap().len()
        }).await.unwrap();
        
        if let Some(ref pompe) = pompe_manager {
            // if loop_counter == 1 {
            //     pompe.debug_config();
            // }
            
            // let (w1, w2, cs, ready, _) = pompe.get_detailed_stats();
            
            // if loop_counter % 10 == 0 && (w1 > 0 || w2 > 0 || cs > 0) {
            //     info!("📊 [详细状态] Node {} Pompe状态详情:", node_id);
            //     info!("  📋 等待Ordering1: {} 个交易", w1);
            //     info!("  📋 等待Ordering2: {} 个交易", w2);
            //     info!("  📋 提交集大小: {} 个交易", cs);
            //     info!("  📋 共识就绪: {}", ready);
            // }
            
            match pompe.process_transaction_batch(shared_tx_queue.clone()).await {
                Ok(processed_count) => {
                    if processed_count > 0 {
                        info!("🔍 [系统监控] Node {} Pompe 处理了 {} 个交易", node_id, processed_count);
                    }
                }
                Err(e) => {
                    error!("❌ [系统监控] Node {} Pompe 处理失败: {}", node_id, e);
                }
            }
            
            let ordered_txs = pompe.get_ordered_transactions();
            if !ordered_txs.is_empty() {
                info!("🚀 Pompe输出 {} 个交易给HotStuff", ordered_txs.len());
                let mut queue = shared_tx_queue.lock().unwrap();
                let old_size = queue.len();
                
                info!("🔍 [系统监控] Node {} 准备将 {} 个Pompe交易加入HotStuff队列", 
                  node_id, ordered_txs.len());
            
                for (i, tx) in ordered_txs.iter().enumerate() {
                    queue.push(tx.clone());
                    info!("  📤 [{}] 加入HotStuff队列: {}", i + 1, tx);
                }
                
                let new_size = queue.len();
                drop(queue);
                
                info!("🚀 [系统监控] Node {} Pompe->HotStuff 完成: {} 个交易 (队列: {} -> {})", 
                      node_id, ordered_txs.len(), old_size, new_size);
            } else {
                info!("🔍 [系统监控] Node {} Pompe 暂无交易输出", node_id);
            }
        }
        
        if loop_counter % 5 == 0 {
            info!("📊 [性能统计] =========================");
            info!("📊 [性能统计] =========================");
        }   
    }
}