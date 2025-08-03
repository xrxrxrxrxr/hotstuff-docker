// hotstuff_runner/src/node.rs
use hotstuff_rs::{
    replica::{Configuration, ReplicaSpec, Replica},
    types::{
        crypto_primitives::VerifyingKey,
        data_types::{ChainID, BufferSize, EpochLength, ViewNumber},
        update_sets::{AppStateUpdates, ValidatorSetUpdates},
        validator_set::{ValidatorSet, ValidatorSetState},
    },
    events::*,
};
use crate::{
    app::TestApp,
    network::NodeNetwork,
    kv_store::MemoryKVStore,
};
use std::time::Duration;
use ed25519_dalek::SigningKey;
use log::info;

pub struct Node {
    verifying_key: VerifyingKey,
    replica: Replica<MemoryKVStore>,
}

impl Node {
    /// 按照hotstuff_rs官方模式创建节点
    pub fn new(
        keypair: SigningKey,
        network: NodeNetwork,
        init_app_state_updates: AppStateUpdates,
        init_validator_set_updates: ValidatorSetUpdates,
    ) -> Self {
        let verifying_key: VerifyingKey = keypair.verifying_key().into();
        
        info!("创建节点，验证密钥: {:?}", verifying_key.to_bytes()[0..8].to_vec());
        
        // 1. 从更新构造验证者集合
        let mut initial_validator_set = ValidatorSet::new();
        initial_validator_set.apply_updates(&init_validator_set_updates);
        
        info!("节点验证者集合: {} 个验证者，总权力: {}", 
             initial_validator_set.len(), 
             initial_validator_set.total_power().int());
        
        // 2. 创建验证者集合状态
        let validator_set_state = ValidatorSetState::new(
            initial_validator_set.clone(),
            initial_validator_set.clone(),
            None,
            true, // is_genesis
        );
        
        // 3. 创建KV存储
        let kv_store = MemoryKVStore::new();
        
        // 4. 初始化副本存储
        Replica::initialize(
            kv_store.clone(),
            init_app_state_updates,
            validator_set_state,
        );
        
        // 5. 创建应用程序
        let app = TestApp::new(format!("node-{:?}", verifying_key.to_bytes()[0..4].to_vec()));
        
        // 6. 创建配置 - 使用与官方完全相同的参数
        let config = Configuration::builder()
            .me(keypair)
            .chain_id(ChainID::new(0))
            .block_sync_request_limit(10)
            .block_sync_server_advertise_time(Duration::new(10, 0))      // 官方: 10秒
            .block_sync_response_timeout(Duration::new(3, 0))            // 官方: 3秒
            .block_sync_blacklist_expiry_time(Duration::new(10, 0))      // 官方: 10秒
            .block_sync_trigger_min_view_difference(2)                   // 官方: 2
            .block_sync_trigger_timeout(Duration::new(60, 0))            // 官方: 60秒
            .progress_msg_buffer_capacity(BufferSize::new(1024))
            .epoch_length(EpochLength::new(50))                          // 官方: 50
            .max_view_time(Duration::from_millis(2000))                  // 官方: 2000ms
            .log_events(false)                                           // 官方: false
            .build();
        
        // 7. 启动副本 - 添加详细的事件处理器（类似官方）
        let replica = ReplicaSpec::builder()
            .app(app)
            .network(network)
            .kv_store(kv_store)
            .configuration(config)
            // === 最关键的事件 ===
            .on_start_view({
                let key = verifying_key;
                move |event| {
                    info!("🚀 节点 {:?} 开始视图 {}", 
                          key.to_bytes()[0..4].to_vec(), event.view);
                }
            })
            .on_propose({
                let key = verifying_key;
                move |event| {
                    info!("📤 节点 {:?} 提议区块，视图: {}, 高度: {:?}", 
                          key.to_bytes()[0..4].to_vec(), event.proposal.view, event.proposal.block.height);
                }
            })
            .on_receive_proposal({
                let key = verifying_key;
                move |event| {
                    info!("📥 节点 {:?} 接收提议，来源: {:?}, 视图: {}", 
                          key.to_bytes()[0..4].to_vec(),
                          event.origin.to_bytes()[0..4].to_vec(),
                          event.proposal.view);
                }
            })
            .on_phase_vote({
                let key = verifying_key;
                move |event| {
                    info!("🗳️ 节点 {:?} 阶段投票，视图: {}, 阶段: {:?}",
                          key.to_bytes()[0..4].to_vec(),
                          event.vote.view,
                          event.vote.phase);
                }
            })
            .on_receive_phase_vote({
                let key = verifying_key;
                move |event| {
                    info!("📨 节点 {:?} 接收投票，来源: {:?}, 视图: {}, 阶段: {:?}",
                          key.to_bytes()[0..4].to_vec(),
                          event.origin.to_bytes()[0..4].to_vec(),
                          event.phase_vote.view,
                          event.phase_vote.phase);
                }
            })
            .on_collect_pc({
                let key = verifying_key;
                move |event| {
                    info!("🎯 节点 {:?} 收集PC，视图: {}, 签名数: {}",
                          key.to_bytes()[0..4].to_vec(),
                          event.phase_certificate.view,
                          event.phase_certificate.signatures.iter().filter(|sig| sig.is_some()).count());
                }
            })
            .on_commit_block({
                let key = verifying_key;
                move |event| {
                    info!("💎 节点 {:?} 提交区块，哈希: {:?}", 
                          key.to_bytes()[0..4].to_vec(),
                          event.block.bytes()[0..4].to_vec());
                }
            })
            .on_update_highest_pc({
                let key = verifying_key;
                move |event| {
                    info!("📈 节点 {:?} 更新最高PC，视图: {}, 阶段: {:?}",
                          key.to_bytes()[0..4].to_vec(),
                          event.highest_pc.view,
                          event.highest_pc.phase);
                }
            })
            // === 超时和视图变更事件 ===
            .on_view_timeout({
                let key = verifying_key;
                move |event| {
                    info!("⏱️ 节点 {:?} 视图 {} 超时！", 
                          key.to_bytes()[0..4].to_vec(), event.view);
                }
            })
            .on_timeout_vote({
                let key = verifying_key;
                move |event| {
                    info!("⏰ 节点 {:?} 发送超时投票，视图: {}", 
                          key.to_bytes()[0..4].to_vec(), event.timeout_vote.view);
                }
            })
            .on_receive_timeout_vote({
                let key = verifying_key;
                move |event| {
                    info!("📩 节点 {:?} 接收超时投票，来源: {:?}, 视图: {}", 
                          key.to_bytes()[0..4].to_vec(),
                          event.origin.to_bytes()[0..4].to_vec(),
                          event.timeout_vote.view);
                }
            })
            .on_collect_tc({
                let key = verifying_key;
                move |event| {
                    info!("🔄 节点 {:?} 收集TC，视图: {}", 
                          key.to_bytes()[0..4].to_vec(), event.timeout_certificate.view);
                }
            })
            .on_advance_view({
                let key = verifying_key;
                move |event| {
                    info!("⏭️ 节点 {:?} 推进视图到: {}", 
                          key.to_bytes()[0..4].to_vec(), event.advance_view.progress_certificate.view());
                }
            })
            .on_new_view({
                let key = verifying_key;
                move |event| {
                    info!("🆕 节点 {:?} 发送新视图消息，视图: {}", 
                          key.to_bytes()[0..4].to_vec(), event.new_view.view);
                }
            })
            .on_receive_new_view({
                let key = verifying_key;
                move |event| {
                    info!("📬 节点 {:?} 接收新视图消息，来源: {:?}, 视图: {}", 
                          key.to_bytes()[0..4].to_vec(),
                          event.origin.to_bytes()[0..4].to_vec(),
                          event.new_view.view);
                }
            })
            // === 网络和同步事件 ===
            .on_insert_block({
                let key = verifying_key;
                move |event| {
                    info!("🔗 节点 {:?} 插入区块，哈希: {:?}", 
                          key.to_bytes()[0..4].to_vec(),
                          event.block.hash.bytes()[0..4].to_vec());
                }
            })
            .build()
            .start();
        
        info!("✅ 节点 {:?} 已启动", verifying_key.to_bytes()[0..4].to_vec());
        
        Self {
            verifying_key,
            replica,
        }
    }

    /// 查询节点的验证密钥
    pub fn verifying_key(&self) -> VerifyingKey {
        self.verifying_key
    }

    /// 查询当前提交的验证者集合
    pub fn committed_validator_set(&self) -> ValidatorSet {
        self.replica
            .block_tree_camera()
            .snapshot()
            .committed_validator_set()
            .expect("应该能够从区块树获取已提交的验证者集合")
    }

    /// 查询进入的最高视图号
    pub fn highest_view_entered(&self) -> ViewNumber {
        self.replica
            .block_tree_camera()
            .snapshot()
            .highest_view_entered()
            .expect("应该能够从区块树获取进入的最高视图")
    }
}