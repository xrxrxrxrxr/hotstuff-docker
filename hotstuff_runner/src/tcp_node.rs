// crate/src/node.rs
use hotstuff_rs::{
    block_tree::{pluggables::KVGet, variables::HIGHEST_COMMITTED_BLOCK}, events::*, replica::{self, Configuration, Replica, ReplicaSpec}, types::{
        crypto_primitives::VerifyingKey,
        data_types::{BufferSize, ChainID, EpochLength, ViewNumber, Data},
        block::Block,
        update_sets::{AppStateUpdates, ValidatorSetUpdates},
        validator_set::{ValidatorSet, ValidatorSetState},
    }
};
use crate::{
    app::TestApp,
    network::NodeNetwork,
    tcp_network::TcpNetwork,
    kv_store::MemoryKVStore,
    stats::PerformanceStats,
};
use std::time::Duration;
use std::sync::{Arc, Mutex};
use ed25519_dalek::SigningKey;
// use log::info;
use tracing::{info, warn};
use crossbeam::queue::SegQueue;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct Node {
    verifying_key: VerifyingKey,
    replica: Replica<MemoryKVStore>,
    node_id: usize,
    // 添加对应用的引用以支持交易提交
    // app_handle: Arc<Mutex<TestApp>>,
    tx_queue: Arc<SegQueue<String>>,     
    stats: Arc<PerformanceStats>,       
}

impl Node {
    /// 按照hotstuff_rs官方模式创建Node
    pub fn new(
        node_id: usize,  // 添加NodeID参数
        keypair: SigningKey,
        network: TcpNetwork,    // 使用TcpNetwork替代NodeNetwork
        init_app_state_updates: AppStateUpdates,
        init_validator_set_updates: ValidatorSetUpdates,
        tx_queue: Arc<SegQueue<String>>,  // 新增参数：外部交易队列
        stats: Arc<PerformanceStats>,  // 新增性能统计
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
        // let app = TestApp::new(format!("node-{:?}", verifying_key.to_bytes()[0..4].to_vec()));
        let app = TestApp::new(
            node_id,
            tx_queue.clone()
        );
        // let app_handle = Arc::new(Mutex::new(app.clone()));
        
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
            .max_view_time(Duration::from_millis(3000))                  // 官方: 2000ms
            .log_events(false)                                           // 官方: false
            .build();

        let kv_clone_commit = kv_store.clone();
        // let kv_clone_insert = kv_store.clone();
        // let kv_clone_receive = kv_store.clone();
        let stats_for_commit = stats.clone();

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
                        "📤 Node {} 提议区块，View: {}, 高度: {:?}, hash: {:?}",
                        node_id,
                        event.proposal.view,
                        event.proposal.block.height,
                        event.proposal.block.hash
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
            // .on_commit_block({
            //     move |event|{
            //         let msg = format!(
            //             "💎 Node {} Commit block, Hash: {:?}",
            //             node_id, event.block
            //         );
            //         crate::log_node(node_id, log::Level::Info, &msg);
            //     }
            // })
            .on_commit_block({
                move |event| {
                    let block_hash = event.block;
                    let commit_time = event.timestamp;
                    
                    match kv_clone_commit.block(&block_hash) {
                        Ok(Some(block)) => {
                            let height = block.height.int();
                            
                            // 关键修正：正确解析交易数量
                            let tx_count = if block.data.vec().len() >= 2 {
                                let tx_count_bytes = block.data.vec()[1].bytes();
                                if tx_count_bytes.len() >= 4 {
                                    let mut bytes = [0u8; 4];
                                    bytes.copy_from_slice(&tx_count_bytes[0..4]);
                                            u32::from_le_bytes(bytes)
                                } else {
                                        0
                                        }
                            } else {
                                    0
                                    };
                            
                            // 🎯 更新统计并获取多种TPS指标
                            let (end_to_end_tps, pure_consensus_tps, submission_tps, total_confirmed_txs, total_confirmed_blocks, is_first_commit) = {
                                // let mut stats = stats_for_commit.lock().unwrap();
                                
                                // 检查是否是第一个确认
                                let is_first = stats_for_commit.get_confirmed_blocks() == 0;
                                
                                // 记录区块确认
                                stats_for_commit.record_block_committed(tx_count.into());

                                (
                                    stats_for_commit.get_end_to_end_tps(),        // 端到端TPS
                                    stats_for_commit.get_pure_consensus_tps(),    // 纯共识TPS
                                    stats_for_commit.get_submission_tps(),        // 提交TPS
                                    stats_for_commit.get_confirmed_transactions(),
                                    stats_for_commit.get_confirmed_blocks(),
                                    is_first
                                )
                            };

                            
                            // 主要的统计日志
                            let msg = format!(
                                "💎 Node {} Commit block - Height: {}, TxCount: {}, E2E_TPS: {:.2}, Pure_TPS: {:.2}, Submit_TPS: {:.2}, TotalTxs: {}, TotalBlocks: {}",
                                node_id, height, tx_count, end_to_end_tps, pure_consensus_tps, submission_tps, total_confirmed_txs, total_confirmed_blocks
                            );
                            crate::log_node(node_id, log::Level::Info, &msg);

                            // 🎯 每10个区块显示详细分析
                            if total_confirmed_blocks % 10 == 0 {
                                // let stats_guard = stats_for_commit.lock().unwrap();
                                let recent_tps = stats_for_commit.get_recent_consensus_tps(30.0);
                                
                                info!("📊 Node {} 共识统计报告 (第{}个区块):", node_id, total_confirmed_blocks);
                                info!("  📥 提交TPS: {:.2} (客户端 → 队列)", submission_tps);
                                info!("  🔄 端到端TPS: {:.2} (队列 → 确认)", end_to_end_tps);
                                info!("  🎯 纯共识TPS: {:.2} (共识层性能)", pure_consensus_tps);
                                info!("  ⏱️ 最近TPS: {:.2} (最近30秒)", recent_tps);
                                info!("  📈 确认交易总数: {}", total_confirmed_txs);
                                info!("  📦 确认区块总数: {}", total_confirmed_blocks);
                                
                                // 🚨 性能分析
                                if submission_tps > end_to_end_tps * 1.2 {
                                    warn!("⚠️ 检测到交易积压: 提交速度({:.1}) > 确认速度({:.1})", 
                                        submission_tps, end_to_end_tps);
                                }
                                
                                if pure_consensus_tps > 0.0 {
                                    let queue_overhead = (end_to_end_tps / pure_consensus_tps - 1.0) * 100.0;
                                    if queue_overhead > 10.0 {
                                        warn!("⚠️ 排队开销较大: {:.1}%", queue_overhead);
                                    } else {
                                        info!("✅ 排队开销: {:.1}%", queue_overhead);
                                    }
                                }
                                
                                // drop(stats_guard);
                            }
                        },
                        Ok(None) => {
                            let msg = format!(
                                "💎 Node {} 提交区块 - 哈希: {:?} (区块详情未找到)",
                                node_id, &block_hash.bytes()[0..8]
                            );
                            crate::log_node(node_id, log::Level::Warn, &msg);
                        },
                        Err(e) => {
                            let msg = format!(
                                "💎 Node {} 提交区块 - 哈希: {:?} (读取错误: {:?})",
                                node_id, &block_hash.bytes()[0..8], e
                            );
                            crate::log_node(node_id, log::Level::Error, &msg);
                        }
                    }
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
            // .on_insert_block({
            //     move |event| {
            //         let block_hash = event.block.hash;
            //         let tx_count = if event.block.data.vec().len() >= 2 {
            //             let tx_count_bytes = event.block.data.vec()[1].bytes();
            //             if tx_count_bytes.len() >= 4 {
            //                 let mut bytes = [0u8; 4];
            //                 bytes.copy_from_slice(&tx_count_bytes[0..4]);
            //                         u32::from_le_bytes(bytes)
            //                     } else {
            //                         0
            //                     }
            //                 } else {
            //                     0
            //                 };
            //         let msg = format!(
            //                     "🔗 Node {} 插入区块事件 - 交易数: {}, 哈希: {:?}",
            //                     node_id, tx_count, &block_hash
            //                 );
            //                 crate::log_node(node_id, log::Level::Info, &msg);
                    
            //         // 从 KV store 读取区块内容
            //         match kv_clone_insert.block(&block_hash) {
            //             Ok(Some(block)) => {
            //                 let height = block.height.int();
            //                 let data_items = block.data.len();
                            
            //                 // 解析交易数量
            //                 let tx_count = if block.data.vec().len() >= 2 {
            //                 let tx_count_bytes = block.data.vec()[1].bytes();
            //                 if tx_count_bytes.len() >= 4 {
            //                     let mut bytes = [0u8; 4];
            //                     bytes.copy_from_slice(&tx_count_bytes[0..4]);
            //                             u32::from_le_bytes(bytes)
            //                         } else {
            //                             0
            //                         }
            //                     } else {
            //                         0
            //                     };
                             
            //                 let msg = format!(
            //                     "🔗 Node {} 插入区块kv_store - Height: {}, 交易数: {}, 数据项: {}, 哈希: {:?}",
            //                     node_id, height, tx_count, data_items.int(), &block_hash
            //                 );
            //                 crate::log_node(node_id, log::Level::Info, &msg);
            //             },
            //             _ => {
            //                 let msg = format!(
            //                     "🔗 Node {} 插入区块kv_store - 哈希: {:?} (无法读取详情)",
            //                     node_id, &block_hash.bytes()[0..8]
            //                 );
            //                 crate::log_node(node_id, log::Level::Warn, &msg);
            //             }
            //         }
            //     }
            // })
            .on_insert_block({
                move |event| {
                    let msg = format!(
                        "🔗 Node {} 插入区块, 高度: {}, 哈希: {:?}",
                        node_id,
                        event.block.height.int(),
                        event.block.hash,
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
            // app_handle,  // 保存应用引用
            tx_queue,  // 保存交易队列引用
            stats,
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

    // /// 提交交易到Node
    // pub fn submit_transaction(&self, transaction: String) {
    //     let mut app = self.app_handle.lock().unwrap();
    //     app.add_transaction(transaction.clone());
    //     crate::log_node(self.node_id, log::Level::Info, 
    //                               &format!("📝 接收交易: {}", transaction));
    // }

    /// 批量提交交易
    pub fn submit_transactions(&self, transactions: Vec<String>) {
        // 直接添加到共享队列
        // let mut queue = self.tx_queue.lock().unwrap();
        for tx in transactions {
            self.tx_queue.push(tx.clone());
            info!("📝 提交交易到共享队列: {}", tx);
        }

        // let mut app = self.app_handle.lock().unwrap();
        // for tx in &transactions {
        //     app.add_transaction(tx.clone());
        //     info!("📝 add_tx 提交交易: {} 到 pending tx", tx);
        // }
        // crate::log_node(self.node_id, log::Level::Info, 
        //                           &format!("📝 接收 {} 个交易", transactions.len()));
    }
}