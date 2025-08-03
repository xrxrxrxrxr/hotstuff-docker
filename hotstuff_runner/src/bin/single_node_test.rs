// hotstuff_runner/src/bin/single_node_test.rs
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
use log::{info, debug, warn, error};

fn setup_logger() {
    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "[{}][{}][{}] {}",
                chrono::Local::now().format("%H:%M:%S%.3f"),
                record.level(),
                record.target(),
                message
            ))
        })
        .level(log::LevelFilter::Info)  // 减少日志噪音
        .chain(std::io::stdout())
        .apply()
        .unwrap();
}

fn main() {
    setup_logger();
    info!("=== 官方风格单节点测试 ===");
    
    // 1. 生成密钥对（模仿官方测试）
    let mut seed = [0u8; 32];
    seed[0] = 1;
    let signing_key = SigningKey::from_bytes(&seed);
    let verifying_key: VerifyingKey = signing_key.verifying_key().into();
    
    info!("🔑 生成密钥对");
    info!("   验证密钥: {:?}", verifying_key.to_bytes()[0..8].to_vec());
    
    // 2. 创建初始应用状态更新
    let init_app_state_updates = AppStateUpdates::new();
    info!("📱 创建初始应用状态更新");
    
    // 3. 创建初始验证者集合更新
    let init_validator_set_updates = {
        let mut vs_updates = ValidatorSetUpdates::new();
        vs_updates.insert(verifying_key.clone(), Power::new(1));
        vs_updates
    };
    info!("👥 创建验证者集合更新: 1个验证者，权力=1");
    
    // 4. 创建网络（模仿官方的mock_network）
    let (_shared_network, mut node_networks) = create_mock_network(vec![verifying_key.clone()]);
    let node_network = node_networks.pop().unwrap();
    info!("🌐 创建模拟网络");
    
    // 5. 按照官方模式创建节点
    info!("🚀 按照官方模式创建节点...");
    let node = Node::new(
        signing_key,
        node_network,
        init_app_state_updates,
        init_validator_set_updates,
    );
    
    info!("✅ 节点创建完成！");
    
    // 6. 验证初始状态
    info!("🔍 验证节点初始状态:");
    info!("   验证密钥: {:?}", node.verifying_key().to_bytes()[0..8].to_vec());
    
    let initial_vs = node.committed_validator_set();
    info!("   已提交验证者集合: {} 个验证者, 总权力: {}", 
         initial_vs.len(), 
         initial_vs.total_power().int());
    
    let initial_view = node.highest_view_entered();
    info!("   最高视图: {}", initial_view);
    
    info!("");
    info!("=== 期望事件序列 ===");
    info!("1. 🚀 节点启动并开始视图 0");
    info!("2. 📤 节点提议区块（作为唯一的领导者）");
    info!("3. 📥 节点接收自己的提议");
    info!("4. 🗳️ 节点对自己的提议投票");
    info!("5. 📨 节点接收自己的投票");
    info!("6. 🎯 节点收集PC（1票足够）");
    info!("7. 💎 节点提交区块");
    info!("");
    
    // 7. 监控运行并定期检查状态
    let start_time = std::time::Instant::now();
    let mut last_check_time = start_time;
    let mut last_view = initial_view;
    
    loop {
        thread::sleep(Duration::from_millis(1000));  // 每秒检查一次
        
        let elapsed = start_time.elapsed();
        
        // 每3秒检查一次状态变化
        if elapsed - last_check_time.duration_since(start_time) >= Duration::from_secs(3) {
            let current_view = node.highest_view_entered();
            
            if current_view != last_view {
                info!("📊 状态变化: 视图从 {} 变为 {}", last_view, current_view);
                last_view = current_view;
                last_check_time = std::time::Instant::now();
            } else {
                debug!("⏰ 运行时间: {:.1}秒, 当前视图: {}", 
                      elapsed.as_secs_f64(), 
                      current_view);
                last_check_time = std::time::Instant::now();
            }
        }
        
        // 如果在30秒内没有看到视图推进
        if elapsed > Duration::from_secs(30) {
            let final_view = node.highest_view_entered();
            
            if final_view == initial_view {
                error!("❌ 30秒内没有看到任何视图推进！");
                error!("   初始视图: {}", initial_view);
                error!("   最终视图: {}", final_view);
                error!("");
                error!("可能的问题:");
                error!("1. 网络消息传递失败");
                error!("2. 节点没有成为领导者");
                error!("3. 共识算法配置问题");
                error!("4. 应用程序问题");
            } else {
                info!("✅ 节点正常运行！");
                info!("   视图推进: {} -> {}", initial_view, final_view);
                info!("   运行时间: {:.1}秒", elapsed.as_secs_f64());
            }
            break;
        }
    }
    
    info!("🏁 测试完成");
}