// crate/src/node.rs
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
use std::sync::{Arc, Mutex};
use ed25519_dalek::SigningKey;
use log::info;

pub struct Node {
    verifying_key: VerifyingKey,
    replica: Replica<MemoryKVStore>,
    node_id: usize,
    // 添加对应用的引用以支持交易提交
    app_handle: Arc<Mutex<TestApp>>,
}

impl Node {
    /// 按照hotstuff_rs官方模式创建Node
    pub fn new(
        node_id: usize,  // 添加NodeID参数
        keypair: SigningKey,
        network: NodeNetwork,
        init_app_state_updates: AppStateUpdates,
        init_validator_set_updates: ValidatorSetUpdates,
    ) -> Self {
        let verifying_key: VerifyingKey = keypair.verifying_key().into();
        
        info!("创建Node，验证密钥: {:?}", verifying_key.to_bytes()[0..8].to_vec());
        
        // 1. 从更新构造验证者集合
        let mut initial_validator_set = ValidatorSet::new();
        initial_validator_set.apply_updates(&init_validator_set_updates);
        
        info!("Node验证者集合: {} 个验证者，总权力: {}", 
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
        
        // 5. 创建应用程序并保存引用
        let app = TestApp::new(format!("node-{:?}", verifying_key.to_bytes()[0..4].to_vec()));
        let app_handle = Arc::new(Mutex::new(app.clone()));
        
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
                move |event| {
                    let msg = format!("🚀 Node {} 开始View {}", node_id, event.view);
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_propose({
                move |event| {
                    let msg = format!(
                        "📤 Node {} 提议区块，View: {}, 高度: {:?}",
                        node_id,
                        event.proposal.view,
                        event.proposal.block.height
                    );
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_receive_proposal({
                move |event| {
                    let msg = format!(
                        "📥 Node {} 接收提议，来源: {:?}, View: {}",
                        node_id,
                        event.origin.to_bytes()[0..4].to_vec(),
                        event.proposal.view
                    );
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_phase_vote({
                move |event| {
                    let msg = format!(
                        "🗳️ Node {} 阶段投票，View: {}, 阶段: {:?}",
                        node_id,
                        event.vote.view,
                        event.vote.phase
                    );
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_receive_phase_vote({
                move |event| {
                    let msg = format!(
                        "📨 Node {} 接收投票，来源: {:?}, View: {}, 阶段: {:?}",
                        node_id,
                        event.origin.to_bytes()[0..4].to_vec(),
                        event.phase_vote.view,
                        event.phase_vote.phase
                    );
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_collect_pc({
                move |event| {
                    let msg = format!(
                        "🎯 Node {} 收集PC，View: {}, 签名数: {}",
                        node_id,
                        event.phase_certificate.view,
                        event.phase_certificate.signatures.iter().filter(|sig| sig.is_some()).count()
                    );
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_commit_block({
                move |event| {
                    let msg = format!(
                        "💎 Node {} 提交区块，哈希: {:?}",
                        node_id,
                        event.block.bytes()[0..4].to_vec()
                    );
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_update_highest_pc({
                move |event| {
                    let msg = format!(
                        "📈 Node {} 更新最高PC，View: {}, 阶段: {:?}",
                        node_id,
                        event.highest_pc.view,
                        event.highest_pc.phase
                    );
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            // === 超时和View变更事件 ===
            .on_view_timeout({
                move |event| {
                    let msg = format!(
                        "⏱️ Node {} View {} 超时！",
                        node_id,
                        event.view
                    );
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_timeout_vote({
                move |event| {
                    let msg = format!(
                        "⏰ Node {} 发送超时投票，View: {}",
                        node_id,
                        event.timeout_vote.view
                    );
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_receive_timeout_vote({
                move |event| {
                    let msg = format!(
                        "📩 Node {} 接收超时投票，来源: {:?}, View: {}",
                        node_id,
                        event.origin.to_bytes()[0..4].to_vec(),
                        event.timeout_vote.view
                    );
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_collect_tc({
                move |event| {
                    let msg = format!(
                        "🔄 Node {} 收集TC，View: {}",
                        node_id,
                        event.timeout_certificate.view
                    );
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_advance_view({
                move |event| {
                    let msg = format!(
                        "⏭️ Node {} 推进View到: {}",
                        node_id,
                        event.advance_view.progress_certificate.view()
                    );
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_new_view({
                move |event| {
                    let msg = format!(
                        "🆕 Node {} 发送新View消息，View: {}",
                        node_id,
                        event.new_view.view
                    );
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_receive_new_view({
                move |event| {
                    let msg = format!(
                        "📬 Node {} 接收新View消息，来源: {:?}, View: {}",
                        node_id,
                        event.origin.to_bytes()[0..4].to_vec(),
                        event.new_view.view
                    );
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            // === 网络和同步事件 ===
            .on_insert_block({
                move |event| {
                    let msg = format!(
                        "🔗 Node {} 插入区块，哈希: {:?}",
                        node_id,
                        event.block.hash.bytes()[0..4].to_vec()
                    );
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .build()
            .start();
        
        info!("✅ Node {} 已启动", node_id);
        
        Self {
            verifying_key,
            replica,
            node_id,
            app_handle,  // 保存应用引用
        }
    }

    /// 查询Node的验证密钥
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

    /// 查询进入的最高View号
    pub fn highest_view_entered(&self) -> ViewNumber {
        self.replica
            .block_tree_camera()
            .snapshot()
            .highest_view_entered()
            .expect("应该能够从区块树获取进入的最高View")
    }

    /// 提交交易到Node
    pub fn submit_transaction(&self, transaction: String) {
        let mut app = self.app_handle.lock().unwrap();
        app.add_transaction(transaction.clone());
        crate::log_node(self.node_id, log::Level::Info, 
                                  &format!("📝 接收交易: {}", transaction));
    }

    /// 批量提交交易
    pub fn submit_transactions(&self, transactions: Vec<String>) {
        let mut app = self.app_handle.lock().unwrap();
        for tx in &transactions {
            app.add_transaction(tx.clone());
        }
        crate::log_node(self.node_id, log::Level::Info, 
                                  &format!("📝 接收 {} 个交易", transactions.len()));
    }
}