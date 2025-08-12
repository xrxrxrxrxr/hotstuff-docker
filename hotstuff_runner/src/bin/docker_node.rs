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
    stats::PerformanceStats
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
    // let main_log_file = File::options()
    //     .create(true)
    //     .append(true)
    //     .open("logs/main.log")
    //     .expect("无法打开 main.log 文件");
    
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
        // .with(
        //     fmt::layer()
        //         .with_writer(main_log_file)
        //         .with_target(true)
        //         .with_thread_ids(true)
        //         .with_ansi(false)
        // )  // 主日志文件输出
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

                        let tx_string = format!("{}:{}->{}:{}", transaction.id, transaction.from, transaction.to, transaction.amount);
                        // info!("💰 Node {} 接收交易 #{} (连接内): ID={}, {}->{}:{}", 
                            //   node_id, tx_count, transaction.id, transaction.from, transaction.to, transaction.amount);

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

                        // 验证交易确实被添加到队列
                        let current_queue_size = tokio::task::spawn_blocking({
                            let queue_clone = shared_tx_queue.clone();
                            move || queue_clone.lock().unwrap().len()
                        }).await.unwrap();
                        
                        // info!("📝 Node {} 交易已添加到共享队列，当前队列大小: {}", node_id, current_queue_size);

                        // 更新统计
                        let stats_clone = stats.clone();
                        tokio::task::spawn_blocking(move || {
                            let mut stats_guard = stats_clone.lock().unwrap();
                            stats_guard.record_submitted();
                        }).await.unwrap();
                        
                        // 每100个交易显示统计（调试用）
                        if tx_count % 100 == 0 {
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
        performance_stats.clone(),
    );

    // let node_for_main_loop = Arc::new(_node);
    // let queue_for_main_loop = shared_tx_queue.clone();
    // let stats_for_main_loop = performance_stats.clone();
    // 修正后的主循环：只做监控，不提取交易
    let queue_for_monitoring = shared_tx_queue.clone();
    let stats_for_monitoring = performance_stats.clone();

    // 增强的主循环：详细追踪交易处理流程
    let mut loop_counter = 0;
    let mut last_queue_size = 0;
    let mut last_confirmed_txs = 0;
    let mut total_processed = 0;
    
    loop {
        tokio::time::sleep(Duration::from_secs(5)).await; // 改为5秒监控一次
        loop_counter += 1;
        
        // 只监控状态，不修改队列
        let current_queue_size = tokio::task::spawn_blocking({
            let queue_clone = queue_for_monitoring.clone();
            move || queue_clone.lock().unwrap().len()
        }).await.unwrap();
        
        // let (current_tps, avg_tps, total_txs) = tokio::task::spawn_blocking({
        //     let stats_clone = stats_for_monitoring.clone();
        //     move || {
        //         let stats = stats_clone.lock().unwrap();
        //         let current_tps = if let Some(start) = stats.start_time {
        //             stats.submitted_count as f64 / start.elapsed().as_secs_f64()
        //         } else { 0.0 };
        //         (current_tps, stats.get_tps(), stats.submitted_count)
        //     }
        // }).await.unwrap();
        // 获取所有统计数据
        let (submission_tps, consensus_tps, total_submitted, total_confirmed_txs, total_confirmed_blocks, recent_tps) = tokio::task::spawn_blocking({
            let stats_clone = stats_for_monitoring.clone();
            move || {
                let stats = stats_clone.lock().unwrap();
                (
                    stats.get_submission_tps(),
                    stats.get_end_to_end_tps(),
                    stats.get_submitted_count(),
                    stats.get_confirmed_transactions(),
                    stats.get_confirmed_blocks(),
                    stats.get_recent_tps(30.0)
                )
            }
        }).await.unwrap();
        
        // 定期报告系统状态
        // if loop_counter % 2 == 0 { // 每10秒报告一次
        //     info!("📊 Node {} 完整系统状态报告:", node_id);
        //     info!("  🔄 运行时间: {} 个监控周期", loop_counter);
        //     info!("  📦 待处理交易队列: {}", current_queue_size);
        //     info!("  📥 提交TPS: {:.2} ({} 个交易已提交)", submission_tps, total_submitted);
        //     info!("  🎯 端到端TPS: {:.2} ({} 个交易已确认)", consensus_tps, total_confirmed_txs);
        //     info!("  ⏱️ 最近TPS: {:.2} (最近30秒平均)", recent_tps);
        //     info!("  📈 确认区块数: {}", total_confirmed_blocks);
            
        //     // 显示效率指标
        //     if total_submitted > 0 {
        //         let confirmation_rate = (total_confirmed_txs as f64 / total_submitted as f64) * 100.0;
        //         info!("  ✅ 确认率: {:.1}%", confirmation_rate);
        //     }
        // }
        // 交易确认进度提醒
        if total_confirmed_txs != last_confirmed_txs {
            let confirmed_diff = total_confirmed_txs - last_confirmed_txs;
            info!("🎯 Node {} 新确认交易: +{} (总计: {})", 
                node_id, confirmed_diff, total_confirmed_txs);
            last_confirmed_txs = total_confirmed_txs;
        }
        
        // 队列大小变化提醒
        if current_queue_size != last_queue_size {
            if current_queue_size > last_queue_size {
                info!("📈 Node {} 队列增长: {} -> {} (+{})", 
                      node_id, last_queue_size, current_queue_size, 
                      current_queue_size - last_queue_size);
            } else {
                info!("📉 Node {} 队列减少: {} -> {} (-{})", 
                      node_id, last_queue_size, current_queue_size, 
                      last_queue_size - current_queue_size);
            }
            last_queue_size = current_queue_size;
        }

        // 🚨 每50个循环（250秒）进行深度系统检查
        if loop_counter % 50 == 0 {
            info!("🔍 Node {} 系统资源深度检查 (第{}次):", node_id, loop_counter);
            
            // 检查进程内存使用
            let pid = std::process::id();
            info!("  🆔 进程ID: {}", pid);
            
            // 在Linux系统上，可以读取 /proc/self/status
            if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
                for line in status.lines() {
                    if line.starts_with("VmRSS:") || line.starts_with("VmSize:") {
                        info!("  📊 {}", line);
                    }
                }
            }
            
            // 检查文件描述符数量（可能有连接泄漏）
            if let Ok(fd_count) = std::fs::read_dir("/proc/self/fd") {
                let fd_num = fd_count.count();
                info!("  📁 文件描述符: {}", fd_num);
                
                if fd_num > 100 {
                    warn!("⚠️ 文件描述符数量异常: {}", fd_num);
                }
            }
            
            // 检查当前时间和运行时长
            let uptime = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            info!("  ⏰ 运行时长: {} 秒", loop_counter * 5);
            info!("  📈 平均区块速度: {:.2} 区块/分钟", 
                total_confirmed_blocks as f64 / (loop_counter * 5) as f64 * 60.0);
            
            // // 预警系统
            // if total_confirmed_blocks > 400 && total_confirmed_blocks < 600 {
            //     warn!("🚨 Node {} 接近历史故障区间 (500-600区块)!", node_id);
            //     warn!("   当前区块数: {}", total_confirmed_blocks);
            //     warn!("   监控内存和网络状态...");
            // }
        }
        
        // // 🔥 特殊监控：接近500区块时的密集检查
        // if total_confirmed_blocks > 450 && total_confirmed_blocks < 550 {
        //     if loop_counter % 2 == 0 { // 每10秒检查一次
        //         warn!("🎯 Node {} 临界区间监控 (区块 {}):", node_id, total_confirmed_blocks);
        //         warn!("   队列大小: {}", current_queue_size);
        //         warn!("   瞬时TPS: {:.2}", consensus_tps);
        //         warn!("   内存使用: 检查进程状态");
                
        //         // 检查是否有异常的空区块
        //         static mut EMPTY_BLOCK_COUNT: u32 = 0;
        //         if current_queue_size == 0 {
        //             unsafe { EMPTY_BLOCK_COUNT += 1; }
        //             if unsafe { EMPTY_BLOCK_COUNT } > 5 {
        //                 warn!("⚠️ 连续空区块: {} 次", unsafe { EMPTY_BLOCK_COUNT });
        //             }
        //         } else {
        //             unsafe { EMPTY_BLOCK_COUNT = 0; }
        //         }
        //     }
        // }


        // 检查队列积压情况
        if current_queue_size > 1000 {
            warn!("⚠️ Node {} 交易队列积压严重: {} 个交易", node_id, current_queue_size);
        }
        
        // 检查是否有交易处理停滞
        static mut LAST_SUBMITTED_TXS: u64 = 0;
        static mut LAST_CONFIRMED_TXS: u64 = 0;

        let last_submitted = unsafe { LAST_SUBMITTED_TXS };
        let last_confirmed = unsafe { LAST_CONFIRMED_TXS };

        // 检查提交停滞
        if total_submitted == last_submitted && current_queue_size > 0 {
            warn!("⚠️ Node {} 可能出现交易提交停滞", node_id);
        }

        // 检查共识停滞  
        if total_confirmed_txs == last_confirmed && total_submitted > total_confirmed_txs {
            warn!("⚠️ Node {} 可能出现共识确认停滞", node_id);
        }

        // 更新记录
        unsafe { 
            LAST_SUBMITTED_TXS = total_submitted;
            LAST_CONFIRMED_TXS = total_confirmed_txs;
        }
    }
}