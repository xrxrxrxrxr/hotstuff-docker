// hotstuff_runner/src/bin/single_node.rs
use hotstuff_rs::{
    replica::{Configuration, ReplicaSpec, Replica},
    types::{
        data_types::{ChainID, BufferSize, EpochLength, Power},
        update_sets::AppStateUpdates,
        validator_set::{ValidatorSet, ValidatorSetState},
    },
};
use hotstuff_runner::{
    app::TestApp,
    network::{TestNetwork, NodeNetwork},
    kv_store::MemoryKVStore,
};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::thread;
use ed25519_dalek::SigningKey;
use log::info;

fn setup_logger() {
    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "[SINGLE][{}] {}",
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
    // setup_logger();
    // info!("Starting single node test");

    // // 生成密钥对
    // let secret_bytes: [u8; 32] = [1; 32]; // 固定的密钥
    // let signing_key = SigningKey::from_bytes(&secret_bytes);
    // let verifying_key = signing_key.verifying_key();

    // // 创建单节点验证者集合
    // let mut initial_validator_set = ValidatorSet::new();
    // initial_validator_set.put(&verifying_key.into(), Power::new(100));
    
    // // ValidatorSetState 需要4个参数：当前集合、之前集合、更新高度、更新是否完成
    // let initial_validator_set_state = ValidatorSetState::new(
    //     initial_validator_set.clone(),
    //     initial_validator_set.clone(), // 初始时，之前的集合与当前相同
    //     None,                          // 没有更新高度
    //     true,                          // 更新已完成
    // );

    // // 创建网络层
    // let network = Arc::new(Mutex::new(TestNetwork::new()));
    // {
    //     let mut net = network.lock().unwrap();
    //     net.set_validator_keys(vec![verifying_key.into()]);
    // }

    // // 创建KV存储
    // let kv_store = MemoryKVStore::new();

    // // 初始化区块树
    // let initial_app_state = AppStateUpdates::new();
    // Replica::initialize(
    //     kv_store.clone(),
    //     initial_app_state,
    //     initial_validator_set_state,
    // );

    // // 创建App
    // let app = TestApp::new("single-node".to_string());

    // // 创建网络接口
    // let node_network = NodeNetwork::new(0, network);

    // // 创建配置
    // let config = Configuration::builder()
    //     .me(signing_key)
    //     .chain_id(ChainID::new(0))
    //     .block_sync_request_limit(10)
    //     .block_sync_server_advertise_time(Duration::from_secs(1))
    //     .block_sync_response_timeout(Duration::from_secs(5))
    //     .block_sync_blacklist_expiry_time(Duration::from_secs(30))
    //     .block_sync_trigger_min_view_difference(5)
    //     .block_sync_trigger_timeout(Duration::from_secs(10))
    //     .progress_msg_buffer_capacity(BufferSize::new(1024))
    //     .epoch_length(EpochLength::new(10))
    //     .max_view_time(Duration::from_secs(5))
    //     .log_events(true)
    //     .build();

    // // 创建并启动副本
    // let _replica = ReplicaSpec::builder()
    //     .app(app)
    //     .network(node_network)
    //     .kv_store(kv_store)
    //     .configuration(config)
    //     .on_commit_block(|event| {
    //         info!("Committed block: {:?}", event.block);
    //     })
    //     .on_propose(|_event| {
    //         info!("Proposed block");
    //     })
    //     .on_start_view(|event| {
    //         info!("Started view {}", event.view);
    //     })
    //     .build()
    //     .start();

    // info!("Single node started. Press Ctrl+C to stop...");

    // // 保持运行
    // loop {
    //     thread::sleep(Duration::from_secs(60));
    // }
}