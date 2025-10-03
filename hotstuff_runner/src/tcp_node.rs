// crate/src/node.rs
use crate::{app::TestApp, event::SystemEvent, kv_store::MemoryKVStore, stats::PerformanceStats};
use ed25519_dalek::SigningKey;
use hotstuff_rs::networking::network::Network;
use hotstuff_rs::{
    block_tree::{pluggables::KVGet, variables::HIGHEST_COMMITTED_BLOCK},
    events::*,
    replica::{self, Configuration, Replica, ReplicaSpec},
    types::{
        block::Block,
        crypto_primitives::VerifyingKey,
        data_types::{BufferSize, ChainID, Data, EpochLength, ViewNumber},
        update_sets::{AppStateUpdates, ValidatorSetUpdates},
        validator_set::{ValidatorSet, ValidatorSetState},
    },
};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
// use log::info;
use crossbeam::queue::SegQueue;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{error, info, warn};

pub struct Node {
    verifying_key: VerifyingKey,
    replica: Replica<MemoryKVStore>,
    node_id: usize,
    // 添加对应用的引用以支持交易提交
    // app_handle: Arc<Mutex<TestApp>>,
    tx_queue: Arc<SegQueue<String>>,
    stats: Arc<PerformanceStats>,
    event_tx: broadcast::Sender<SystemEvent>, // 新增：事件发送器
}

impl Node {
    /// 按照hotstuff_rs官方模式创建Node
    pub fn new<N: Network + 'static>(
        node_id: usize, // 添加NodeID参数
        keypair: SigningKey,
        network: N, // 泛化网络实现，兼容 Tokio/TCP/mock
        init_app_state_updates: AppStateUpdates,
        init_validator_set_updates: ValidatorSetUpdates,
        tx_queue: Arc<SegQueue<String>>, // 新增参数：外部交易队列
        stats: Arc<PerformanceStats>,    // 新增性能统计
        event_tx: broadcast::Sender<SystemEvent>, // /* 🎯 */
    ) -> Self {
        let verifying_key: VerifyingKey = keypair.verifying_key().into();

        info!(
            "创建Node，验证密钥: {:?}",
            verifying_key.to_bytes()[0..8].to_vec()
        );

        // 1. 从更新构造验证者集合
        let mut initial_validator_set = ValidatorSet::new();
        initial_validator_set.apply_updates(&init_validator_set_updates);

        info!(
            "Node验证者集合: {} 个验证者，总权力: {}",
            initial_validator_set.len(),
            initial_validator_set.total_power().int()
        );

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
        let app = TestApp::new(node_id, tx_queue.clone());
        // let app_handle = Arc::new(Mutex::new(app.clone()));

        // 6. 创建配置 - 使用与官方完全相同的参数，并允许通过环境变量调优
        // 将 HotStuff 视图超时与 Pompe 稳定期对齐（若未显式配置 HS_MAX_VIEW_TIME_MS）
        // let hs_view_env: Option<u64> = std::env::var("HS_MAX_VIEW_TIME_MS").ok().and_then(|s| s.parse().ok());
        // let pompe_stable_env: Option<u64> = std::env::var("POMPE_STABLE_PERIOD_MS").ok().and_then(|s| s.parse().ok());
        // let max_view_time_ms: u64 = match (hs_view_env, pompe_stable_env) {
        //     (Some(hs), _) => hs,
        //     (None, Some(stable)) => stable,
        //     (None, None) => 500,
        // };
        let max_view_time_ms: u64 = std::env::var("HS_MAX_VIEW_TIME_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(700);
        warn!("Node {} 视图超时设置为 {} ms", node_id, max_view_time_ms);
        let progress_buf_cap: usize = std::env::var("HS_PROGRESS_BUF_CAP")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1024);
        let config = Configuration::builder()
            .me(keypair)
            .chain_id(ChainID::new(0))
            .block_sync_request_limit(10)
            .block_sync_server_advertise_time(Duration::new(2, 0)) // 官方: 10秒
            .block_sync_response_timeout(Duration::new(3, 0)) // 官方: 3秒
            // .block_sync_response_timeout(Duration::from_millis(500))
            .block_sync_blacklist_expiry_time(Duration::new(10, 0)) // 官方: 10秒
            .block_sync_trigger_min_view_difference(2) // 官方: 2
            .block_sync_trigger_timeout(Duration::new(60, 0)) // 官方: 60秒
            .progress_msg_buffer_capacity(BufferSize::new(progress_buf_cap.try_into().unwrap()))
            .epoch_length(EpochLength::new(50)) // 官方: 50
            // 可通过 HS_MAX_VIEW_TIME_MS 调整视图超时
            .max_view_time(Duration::from_millis(max_view_time_ms))
            .log_events(false) // 官方: false
            .build();

        let event_tx_for_commit = event_tx.clone(); // 克隆事件发送器
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
                let event_tx_start_view = event_tx.clone();
                move |event| {
                    let msg = format!("🚀 Node {} 开始View {}", node_id, event.view);
                    crate::log_node(node_id, log::Level::Info, &msg);
                    let _ = event_tx_start_view.send(crate::event::SystemEvent::StartView { view: event.view.int() });
                }
            })
            .on_propose({
                move |event| {
                    // let msg = format!(
                    //     "📤 Node {} 提议区块，View: {}, 高度: {:?}, hash: {:?}",
                    //     node_id,
                    //     event.proposal.view,
                    //     event.proposal.block.height,
                    //     event.proposal.block.hash
                    // );
                    // crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_receive_proposal({
                move |event| {
                    // let msg = format!(
                    //     "📥 Node {} 接收提议 View: {}",
                    //     node_id,
                    //     event.proposal.view
                    // );
                    // crate::log_node(node_id, log::Level::Debug, &msg);
                }
            })
            .on_phase_vote({
                move |event| {
                    // let msg = format!(
                    //     "🗳️ Node {} 阶段投票 View: {}, 阶段: {:?}",
                    //     node_id,
                    //     event.vote.view,
                    //     event.vote.phase
                    // );
                    // crate::log_node(node_id, log::Level::Debug, &msg);
                }
            })
            .on_receive_phase_vote({
                move |event| {
                    // let msg = format!(
                    //     "📨 Node {} 接收投票 View: {}, 阶段: {:?}",
                    //     node_id,
                    //     event.phase_vote.view,
                    //     event.phase_vote.phase
                    // );
                    // crate::log_node(node_id, log::Level::Debug, &msg);
                }
            })
            .on_collect_pc({
                move |event| {
                    // let msg = format!(
                    //     "🎯 Node {} 收集PC View: {}, 签名数: {}",
                    //     node_id,
                    //     event.phase_certificate.view,
                    //     event.phase_certificate.signatures.iter().filter(|sig| sig.is_some()).count()
                    // );
                    // crate::log_node(node_id, log::Level::Info, &msg);
                    // // EWNL: 视图关键路径终点
                    // let ewnl_end = format!("[EWNL] END view={}", event.phase_certificate.view.int());
                    // warn!(target = "hotstuff_runner::ewnl", "{}", ewnl_end);
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

                            // 🔥 关键：发送 HotStuff 提交事件，触发客户端 Consensus 响应
                            // 提取交易 ID（关键：用于客户端响应）
                            // let extract_transaction_ids_from_block_start = Instant::now();
                            let tx_ids: Vec<u64> = extract_transaction_ids_from_block(&block);
                                // .into_iter()
                                // .filter(|tx_id| *tx_id % 10 == 0)// 只发送tx_id%100==0的交易
                                // .collect();
                            // info!("!!!!! 提取tx_ids耗时: {} ms", extract_transaction_ids_from_block_start.elapsed().as_millis());
                            if !tx_ids.is_empty() {
                                if let Err(e) = event_tx_for_commit.send(SystemEvent::HotStuffCommitted {
                                    block_height: height,
                                    tx_ids: tx_ids.clone(),
                                }) {
                                    error!("❌ Node {} 发送 HotStuff 提交事件失败: {}", node_id, e);
                                }
                            }
                            info!("[Event sent] Node {} HotStuffCommitted: block_height={}, tx_ids.len= {}, tx_ids={:?}", node_id, height, tx_ids.len(), tx_ids);
                            // 🔥 关键：发送 HotStuff 提交事件，触发客户端 Consensus 响应

                            // 主要的统计日志
                            let msg = format!(
                                "💎 Node {} Commit block - Height: {}, TxCount: {}, E2E_TPS: {:.2}, Pure_TPS: {:.2}, Submit_TPS: {:.2}, TotalTxs: {}, TotalBlocks: {}, tx_ids.len= {}",
                                node_id, height, tx_count, end_to_end_tps, pure_consensus_tps, submission_tps, total_confirmed_txs, total_confirmed_blocks, tx_ids.len()
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
                                
                                // if pure_consensus_tps > 0.0 {
                                //     let queue_overhead = (end_to_end_tps / pure_consensus_tps - 1.0) * 100.0;
                                //     if queue_overhead > 10.0 {
                                //         warn!("⚠️ 排队开销较大: {:.1}%", queue_overhead);
                                //     } else {
                                //         info!("✅ 排队开销: {:.1}%", queue_overhead);
                                //     }
                                // }
                                
                                // drop(stats_guard);
                            }
                        },
                        Ok(None) => {
                            // let msg = format!(
                            //     "💎 Node {} 提交区块 - 哈希: {:?} (区块详情未找到)",
                            //     node_id, &block_hash.bytes()[0..8]
                            // );
                            // crate::log_node(node_id, log::Level::Warn, &msg);
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
                    // let msg = format!(
                    //     "📈 Node {} 更新最高PC，View: {}, 阶段: {:?}",
                    //     node_id,
                    //     event.highest_pc.view,
                    //     event.highest_pc.phase
                    // );
                    // crate::log_node(node_id, log::Level::Info, &msg);
                    // warn!("[on_update_highest_pc] Node {} 更新最高PC: view = {}", node_id, event.highest_pc.view);
                }
            })
            // === 超时和View变更事件 ===
            .on_view_timeout({
                let node_id_copy = node_id;
                move |event| {
                    warn!("Node {} View {} 超时，可能导致延迟累积", node_id_copy, event.view);
                    let msg = format!(
                        "⏱️ Node {} View {} 超时！",
                        node_id,
                        event.view.int()
                    );
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_timeout_vote({
                move |event| {
                    // let msg = format!(
                    //     "⏰ Node {} 发送超时投票，View: {}",
                    //     node_id,
                    //     event.timeout_vote.view
                    // );
                    // crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_receive_timeout_vote({
                move |event| {
                    // let msg = format!(
                    //     "📩 Node {} 接收超时投票，来源: {:?}, View: {}",
                    //     node_id,
                    //     event.origin.to_bytes()[0..4].to_vec(),
                    //     event.timeout_vote.view
                    // );
                    // crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_collect_tc({
                move |event| {
                    // let msg = format!(
                    //     "🔄 Node {} 收集TC，View: {}",
                    //     node_id,
                    //     event.timeout_certificate.view
                    // );
                    // crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_advance_view({
                move |event| {
                    // 注意：这里的 view 来自进度证书（PC/TC）的视图，不等价于本地“进入的当前视图”。
                    let pc_view = event.advance_view.progress_certificate.view();
                    // let msg = format!(
                    //     "📨 Node {} 收到 AdvanceView: PC.view={}",
                    //     node_id,
                    //     pc_view
                    // );
                    // crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_new_view({
                move |event| {
                    // 语义澄清：NewView 事件表示“为当前(旧)视图发送 NewView 消息给下一任领导”，
                    // 并非“进入新视图”。真正进入新视图请看 StartView 事件。
                    let cur_view = event.new_view.view.int();
                    let next_view = cur_view + 1;
                    // let msg = format!(
                    //     "🆕 Node {} 发送 NewView：cur_view={}, next_view(预期)={}",
                    //     node_id,
                    //     cur_view,
                    //     next_view
                    // );
                    // crate::log_node(node_id, log::Level::Info, &msg);
                    // warn!("[on_new_view] Node {} 广播 NewView for 旧视图 {} (即将进入 {})", node_id, cur_view, next_view);
                }
            })
            .on_receive_new_view({
                move |event| {
                    // let msg = format!(
                    //     "📬 Node {} 接收新View消息，来源: {:?}, View: {}",
                    //     node_id,
                    //     event.origin.to_bytes()[0..4].to_vec(),
                    //     event.new_view.view
                    // );
                    // crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_insert_block({
                move |event| {
                    // let msg = format!(
                    //     "🔗 Node {} 插入区块, 高度: {}, 哈希: {:?}",
                    //     node_id,
                    //     event.block.height.int(),
                    //     event.block.hash,
                    // );
                    // crate::log_node(node_id, log::Level::Info, &msg);
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
            tx_queue, // 保存交易队列引用
            stats,
            event_tx, // 保存事件发送器引用
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

// 🔥 关键函数：从区块中提取交易 ID
// 🔥 改进交易 ID 提取逻辑，添加调试信息
fn extract_transaction_ids_from_block(block: &Block) -> Vec<u64> {
    let mut tx_ids = Vec::new();

    // debug!("🔍 [调试] 提取交易 ID，区块数据长度: {}", block.data.vec().len());

    // 🔥 关键修改：遍历所有数据项，而不只是第一个
    for (index, data_item) in block.data.vec().iter().enumerate() {
        let tx_data_bytes = data_item.bytes();
        // debug!("🔍 [调试] 数据项 {} 字节长度: {}", index, tx_data_bytes.len());

        // 跳过太短的数据项（如8字节的空白数据）
        if tx_data_bytes.len() <= 10 {
            // debug!("🔍 [调试] 数据项 {} 太短，跳过", index);
            continue;
        }

        if let Ok(tx_data_str) = std::str::from_utf8(tx_data_bytes) {
            let preview = &tx_data_str[0..std::cmp::min(100, tx_data_str.len())];
            // debug!("🔍 [调试] 数据项 {} 字符串: {}", index, preview);

            // 跳过空白或无效数据
            if tx_data_str.trim().is_empty() {
                // debug!("🔍 [调试] 数据项 {} 为空白，跳过", index);
                continue;
            }

            // 🔥 解析这个数据项中的交易
            let item_tx_ids = parse_transaction_data_item(tx_data_str, index);
            tx_ids.extend(item_tx_ids);
        } else {
            // debug!("🔍 [调试] 数据项 {} 不是有效的 UTF-8", index);
        }
    }

    // debug!("🔍 [调试] 最终提取到 {} 个交易 ID: {:?}", tx_ids.len(),
    //   &tx_ids[0..std::cmp::min(5, tx_ids.len())]);
    tx_ids
}

// 🔥 新增：解析单个数据项中的交易
fn parse_transaction_data_item(tx_data_str: &str, data_index: usize) -> Vec<u64> {
    let mut tx_ids = Vec::new();

    // 方法1: 按行分割处理多个交易
    let lines: Vec<&str> = tx_data_str.lines().collect();
    if lines.len() > 1 {
        // debug!("🔍 [调试] 数据项 {} 包含 {} 行", data_index, lines.len());

        for (line_idx, line) in lines.iter().enumerate() {
            if line.trim().is_empty() {
                continue;
            }

            if let Some(tx_id) = parse_transaction_string(line) {
                tx_ids.push(tx_id);
                // debug!("🔍 [调试] 数据项 {} 行 {} 解析到交易 ID: {} 从: {}",
                //   data_index, line_idx, tx_id, line);
            } else {
                // warn!("⚠️ [调试] 数据项 {} 行 {} 无法解析: {}", data_index, line_idx, line);
            }
        }
    }
    // 方法2: 尝试作为单个交易字符串解析
    else if let Some(tx_id) = parse_transaction_string(tx_data_str) {
        tx_ids.push(tx_id);
        // debug!("🔍 [调试] 数据项 {} 解析到单个交易 ID: {}", data_index, tx_id);
    }
    // 方法3: 尝试作为JSON数组解析
    else if let Ok(transactions) = serde_json::from_str::<Vec<String>>(tx_data_str) {
        // debug!("🔍 [调试] 数据项 {} JSON数组解析，包含 {} 个交易", data_index, transactions.len());

        for tx_str in transactions {
            if let Some(tx_id) = parse_transaction_string(&tx_str) {
                tx_ids.push(tx_id);
            }
        }
    }
    // 方法4: 如果包含逗号，尝试逗号分割
    else if tx_data_str.contains(',') {
        // debug!("🔍 [调试] 数据项 {} 尝试逗号分割", data_index);

        for part in tx_data_str.split(',') {
            if let Some(tx_id) = parse_transaction_string(part.trim()) {
                tx_ids.push(tx_id);
            }
        }
    } else {
        // warn!("⚠️ [调试] 数据项 {} 无法识别格式", data_index);
    }

    // debug!("🔍 [调试] 数据项 {} 提取到 {} 个交易 ID", data_index, tx_ids.len());
    tx_ids
}

// 保持原有的 parse_transaction_string 函数
fn parse_transaction_string(tx_str: &str) -> Option<u64> {
    let trimmed = tx_str.trim();

    // 格式1: pompe:timestamp:tx_id:from->to:amount
    let parts: Vec<&str> = trimmed.split(':').collect();
    if parts.len() >= 4 && parts[0] == "pompe" {
        return parts[2].parse::<u64>().ok();
    }

    // 格式1b: smrol:final_sequence:tx_id:from->to:amount
    if parts.len() >= 3 && parts[0] == "smrol" {
        return parts[2].parse::<u64>().ok();
    }

    // 格式2: tx_id:from->to:amount (常规交易)
    if parts.len() >= 3 {
        return parts[0].parse::<u64>().ok();
    }

    // 格式3: "tx_123"
    if trimmed.starts_with("tx_") {
        return trimmed[3..].parse::<u64>().ok();
    }

    // 格式4: 直接是数字
    if let Ok(id) = trimmed.parse::<u64>() {
        return Some(id);
    }

    None
}
