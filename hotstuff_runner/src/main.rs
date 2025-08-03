// hotstuff_runner/src/main.rs
use hotstuff_rs::{
    types::{
        crypto_primitives::VerifyingKey,
        data_types::Power,
        update_sets::{AppStateUpdates, ValidatorSetUpdates},
    },
};
use hotstuff_runner::{
    node::Node,
    network::create_mock_network,
};
use log::{info, debug, warn, error};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use ed25519_dalek::SigningKey;
use std::fs;
use chrono::{DateTime, Local};

fn setup_logger() {
    fs::create_dir_all("log").unwrap();
    
    // 清理旧的日志文件
    for i in 0..4 {
        let _ = fs::remove_file(format!("log/node{}.log", i));
    }
    let _ = fs::remove_file("log/main.log");
    
    let dispatch = fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "[{}][{}][{}] {}",
                chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f"),
                record.level(),
                record.target(),
                message
            ))
        })
        .level(log::LevelFilter::Info)
        .chain(std::io::stdout())
        .chain(fern::log_file("log/main.log").unwrap())
        .chain(
            fern::Dispatch::new()
                .filter(|metadata| metadata.target() == "node_0")
                .chain(fern::log_file("log/node0.log").unwrap())  // ← 这个字段创建 node0.log
        )
        .chain(
            fern::Dispatch::new()
                .filter(|metadata| metadata.target() == "node_1")
                .chain(fern::log_file("log/node1.log").unwrap())  // ← 这个字段创建 node1.log
        )
        .chain(
            fern::Dispatch::new()
                .filter(|metadata| metadata.target() == "node_2")
                .chain(fern::log_file("log/node2.log").unwrap())  // ← 这个字段创建 node2.log
        )
        .chain(
            fern::Dispatch::new()
                .filter(|metadata| metadata.target() == "node_3")
                .chain(fern::log_file("log/node3.log").unwrap())  // ← 这个字段创建 node3.log
        );
    
    dispatch.apply().unwrap();
}

fn main() {
    setup_logger();
    info!("🚀 启动HotStuff多节点集群 (4个节点)");

    // 1. 生成4个节点的签名密钥
    let mut keypairs = Vec::new();
    let mut verifying_keys = Vec::new();
    
    for i in 0..4 {
        let secret_bytes: [u8; 32] = [i as u8 + 1; 32];
        let signing_key = SigningKey::from_bytes(&secret_bytes);
        let verifying_key = signing_key.verifying_key();
        
        keypairs.push(signing_key);
        verifying_keys.push(VerifyingKey::from(verifying_key));
        
        info!("🔑 为节点 {} 生成密钥对", i);
    }

    // 2. 创建初始应用状态更新
    let init_app_state_updates = AppStateUpdates::new();
    info!("📱 创建初始应用状态更新");

    // 3. 创建初始验证者集合更新（包含所有4个节点）
    let init_validator_set_updates = {
        let mut vs_updates = ValidatorSetUpdates::new();
        // 添加所有4个节点作为初始验证者，每个权力为1
        for i in 0..4 {
            vs_updates.insert(verifying_keys[i].clone(), Power::new(1));
            info!("👥 添加验证者 {} 到初始集合", i);
        }
        vs_updates
    };

    // 4. 使用修正的网络创建方法
    info!("🌐 创建4节点模拟网络...");
    let (_shared_network, node_networks) = create_mock_network(verifying_keys.clone());
    info!("✅ 网络创建完成，所有节点已注册");

    // 5. 按照官方模式创建所有节点
    info!("🏗️ 创建所有4个节点...");
    let mut nodes = Vec::new();
    
    for i in 0..4 {
        info!("启动节点 {}", i);
        
        let node = Node::new(
            keypairs[i].clone(),
            node_networks[i].clone(),
            init_app_state_updates.clone(),
            init_validator_set_updates.clone(),
        );
        
        nodes.push(node);
        info!("✅ 节点 {} 启动完成", i);
        
        // 给节点间隔启动时间
        thread::sleep(Duration::from_millis(500));
    }

    info!("🎉 所有4个节点已启动，等待共识建立...");
    thread::sleep(Duration::from_secs(3));

    // 6. 验证初始状态
    info!("🔍 验证集群初始状态:");
    for (i, node) in nodes.iter().enumerate() {
        let vs = node.committed_validator_set();
        let view = node.highest_view_entered().int();
        info!("   节点 {}: {} 验证者, 视图 {}", i, vs.len(), view);
    }

    // 7. 监控循环 - 检查集群健康状态
    info!("📊 开始监控集群状态...");
    info!("日志文件:");
    info!("  - 主日志: log/main.log");
    info!("  - 节点日志: log/node0.log, log/node1.log, log/node2.log, log/node3.log");

    let start_time = std::time::Instant::now();
    let mut last_views = vec![0u64; 4]; // 跟踪每个节点的最后视图
    
    // 初始化最后视图
    for (i, node) in nodes.iter().enumerate() {
        last_views[i] = node.highest_view_entered().int();
    }

    loop {
        thread::sleep(Duration::from_secs(5));
        
        let elapsed = start_time.elapsed();
        
        // 检查所有节点的状态
        let mut progress_detected = false;
        for (i, node) in nodes.iter().enumerate() {
            let current_view = node.highest_view_entered().int();
            
            if current_view != last_views[i] {
                info!("🔄 节点 {} 视图进展: {} -> {}", i, last_views[i], current_view);
                last_views[i] = current_view;
                progress_detected = true;
            }
        }
        
        if !progress_detected {
            debug!("⏰ 运行时间: {:.1}秒 - 无视图变化", elapsed.as_secs_f64());
        }
        
        // 每30秒打印详细状态
        if elapsed.as_secs() % 30 == 0 {
            info!("📈 集群状态摘要 (运行 {:.0}秒):", elapsed.as_secs_f64());
            for (i, node) in nodes.iter().enumerate() {
                let vs = node.committed_validator_set();
                let view = node.highest_view_entered().int();
                info!("   节点 {}: 验证者={}, 当前视图={}", i, vs.len(), view);
            }
            
            // 检查视图同步情况
            let views: Vec<u64> = nodes.iter().map(|n| n.highest_view_entered().int()).collect();
            let min_view = *views.iter().min().unwrap();
            let max_view = *views.iter().max().unwrap();
            
            if max_view - min_view <= 1 {
                info!("✅ 集群视图同步良好 (差异 <= 1)");
            } else {
                warn!("⚠️ 集群视图分歧较大: 最小={}, 最大={}", min_view, max_view);
            }
        }
        
        // 如果运行超过5分钟，报告状态并继续
        if elapsed > Duration::from_secs(300) {
            info!("📊 集群已稳定运行5分钟");
            
            // 重置计时器
            let start_time = std::time::Instant::now();
        }
    }
}