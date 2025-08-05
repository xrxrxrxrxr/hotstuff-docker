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
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::env;
use std::fs;
use std::fs::{File, create_dir_all};
use tracing::{info, error, warn};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt};
use std::time::Duration;
use std::thread;
use ed25519_dalek::SigningKey;

fn setup_tracing_logger(node_id: usize) {
    // 创建日志目录
    create_dir_all("logs").expect("无法创建日志目录");
    
    // 清理旧的日志文件
    let _ = fs::remove_file(format!("logs/node{}.log", node_id));

    // 创建节点专用的日志文件
    let log_file = File::options()
        .create(true)
        .append(true)
        .open(format!("logs/node{}.log", node_id))
        .expect("无法打开日志文件");
    
    // 使用 try_init 避免重复初始化错误
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
                .with_writer(log_file)
                .with_target(true)
                .with_thread_ids(true)
                .with_ansi(false)  // 文件中不使用颜色
        )  // 文件输出
        .try_init();
    
    match result {
        Ok(_) => info!("📝 日志系统初始化成功"),
        Err(_) => warn!("⚠️ 日志系统已经初始化过了，跳过"),
    }
}

fn create_peer_address(i: usize) -> Result<SocketAddr, Box<dyn std::error::Error>> {
    let hostname = format!("node{}", i);
    let port = 8000 + i as u16;
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

fn main() {
    // 从环境变量读取配置
    let node_id: usize = env::var("NODE_ID")
        .unwrap_or_else(|_| "0".to_string())
        .parse()
        .expect("NODE_ID 必须是数字");
    
    let my_port: u16 = env::var("NODE_PORT")
        .unwrap_or_else(|_| (8000 + node_id).to_string())
        .parse()
        .expect("NODE_PORT 必须是数字");
    
    // 首先初始化日志系统
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
    
    for i in 0..4 {
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
            return;
        }
    };
    
    info!("✅ TCP网络创建成功");
    
    // 等待其他节点启动
    info!("⏳ 等待其他节点启动...");
    thread::sleep(Duration::from_secs(10));
    
    // 创建节点
    info!("🏗️ 创建HotStuff节点...");
    let _node = Node::new(
        node_id,
        signing_key.clone(),
        tcp_network.clone(),
        init_app_state_updates.clone(),
        init_validator_set_updates.clone(),
    );
    
    info!("✅ 节点 {} 启动完成！", node_id);
    
    // // 创建ReplicaSpec
    // let app = (); // 替换为实际的应用程序实例
    // let kv_store = (); // 替换为实际的KV存储实例
    // let config = (); // 替换为实际的配置实例
    
    // let replica_spec = ReplicaSpec::builder()
    //     .app(app)
    //     .network(tcp_network)  // 直接使用 TcpNetwork
    //     .kv_store(kv_store)
    //     .configuration(config)
    //     .build();
    
    info!("🔄 节点运行中...");
    let mut heartbeat_counter = 0;
    loop {
        thread::sleep(Duration::from_secs(30));
        heartbeat_counter += 1;
        info!("💓 节点 {} 心跳 #{}", node_id, heartbeat_counter);
        
        // 每5分钟输出一次状态
        if heartbeat_counter % 10 == 0 {
            info!("📊 节点 {} 运行状态良好 ({}分钟)", node_id, heartbeat_counter / 2);
        }
    }
}