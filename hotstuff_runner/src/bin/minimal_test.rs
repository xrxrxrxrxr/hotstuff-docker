// hotstuff_runner/src/bin/minimal_test.rs
//! 极简单节点测试，用于诊断基本问题

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
use std::time::Duration;
use std::thread;
use ed25519_dalek::SigningKey;
use log::{info, error};

fn setup_logger() {
    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "[{}][{}] {}",
                chrono::Local::now().format("%H:%M:%S%.3f"),
                record.level(),
                message
            ))
        })
        .level(log::LevelFilter::Info)
        .chain(std::io::stdout())
        .apply()
        .unwrap();
}

fn main() {
    setup_logger();
    info!("🔍 极简单节点测试 - 诊断共识启动问题");

    // 1. 创建单个节点
    let secret_bytes: [u8; 32] = [1; 32];
    let signing_key = SigningKey::from_bytes(&secret_bytes);
    let verifying_key = VerifyingKey::from(signing_key.verifying_key());

    info!("🔑 创建单节点密钥: {:?}", verifying_key.to_bytes()[0..8].to_vec());

    // 2. 创建更新
    let init_app_state_updates = AppStateUpdates::new();
    let mut init_validator_set_updates = ValidatorSetUpdates::new();
    init_validator_set_updates.insert(verifying_key.clone(), Power::new(1));

    info!("📋 创建初始状态更新");

    // 3. 创建网络
    let (_global_network, mut node_networks) = create_mock_network(vec![verifying_key.clone()]);
    let node_network = node_networks.pop().unwrap();

    info!("🌐 创建单节点网络");

    // 4. 创建节点
    info!("🚀 创建单节点...");
    let node = Node::new(
        signing_key,
        node_network,
        init_app_state_updates,
        init_validator_set_updates,
    );

    info!("✅ 节点创建完成");

    // 5. 监控 - 重点看是否有任何事件发生
    info!("👀 开始监控（注意观察是否有任何HotStuff事件）...");
    info!("期望至少看到：🚀 开始视图 0");
    info!("");

    let start_time = std::time::Instant::now();
    let mut last_view = node.highest_view_entered().int();
    
    for i in 1..=20 {
        thread::sleep(Duration::from_secs(3));
        
        let current_view = node.highest_view_entered().int();
        let elapsed = start_time.elapsed().as_secs();
        
        if current_view != last_view {
            info!("✅ 检查点 {}: 视图推进 {} -> {} ({}秒)", 
                  i, last_view, current_view, elapsed);
            last_view = current_view;
        } else {
            info!("⏸️ 检查点 {}: 视图保持 {} ({}秒)", i, current_view, elapsed);
        }
        
        // 如果15秒内都没有开始视图0，说明有严重问题
        if elapsed >= 15 && current_view == 0 {
            error!("❌ 15秒内没有看到任何视图启动！");
            error!("可能的问题:");
            error!("1. HotStuff 副本没有正确启动");
            error!("2. 网络层问题阻止了内部通信");
            error!("3. 配置参数问题");
            error!("4. 验证者集合配置错误");
            break;
        }
        
        // 如果有进展，继续观察
        if current_view > 0 {
            info!("🎉 检测到视图推进！继续观察...");
        }
    }

    let final_view = node.highest_view_entered().int();
    info!("");
    info!("📊 测试结果:");
    info!("   开始视图: 0");
    info!("   结束视图: {}", final_view);
    info!("   总时间: {:.1}秒", start_time.elapsed().as_secs_f64());
    
    if final_view > 0 {
        info!("✅ 单节点共识正常工作！");
    } else {
        error!("❌ 单节点共识未启动，需要进一步调试");
    }
}