// hotstuff_runner/src/pompe.rs
//! 完全无锁化的Pompe BFT实现 - 支持crossbeam无锁队列

use crate::pompe_network::PompeNetwork;
use crossbeam::queue::SegQueue;
use dashmap::DashMap;
use ed25519_dalek::SigningKey;
use hotstuff_rs::types::crypto_primitives::VerifyingKey;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};
// Switch Pompe internal queues to tokio::mpsc (async, non-blocking)
use crate::event::SystemEvent;
use tokio::sync::mpsc as async_mpsc;

use crate::pompe::{
    LockFreeHotStuffAdapter,
    // PompeAppState,
    PompeConfig,
    PompeMessage,
    PompeTransaction,
};
#[derive(Debug)]
pub struct PompeAppStateAdversary {
    batch_received: DashMap<String, usize>,
    ordering1_responses: DashMap<String, Vec<u64>>,
    ordering1_count: DashMap<String, usize>,
    completed_ordering1: DashMap<String, ()>,
    ordering2_responses: DashMap<String, Vec<(usize, u64)>>,
    completed_ordering2: DashMap<String, ()>,
    transaction_store: DashMap<String, PompeTransaction>,
    transaction_initiators: DashMap<String, usize>,
    commit_set: Arc<RwLock<Vec<(PompeTransaction, u64)>>>,
    exec_last_batch_clock: Arc<RwLock<u64>>,
    consensus_ready: Arc<RwLock<bool>>,
    stable_point: std::sync::Arc<std::sync::atomic::AtomicU64>,
    // 定时刷新任务是否已安排
    flusher_scheduled: std::sync::atomic::AtomicBool,
}

impl PompeAppStateAdversary {
    fn new() -> Self {
        Self {
            batch_received: DashMap::new(),
            ordering1_responses: DashMap::new(),
            ordering1_count: DashMap::new(),
            completed_ordering1: DashMap::new(),
            ordering2_responses: DashMap::new(),
            completed_ordering2: DashMap::new(),
            transaction_store: DashMap::new(),
            transaction_initiators: DashMap::new(),
            commit_set: Arc::new(RwLock::new(Vec::new())),
            exec_last_batch_clock: Arc::new(RwLock::new(0)),
            consensus_ready: Arc::new(RwLock::new(false)),
            stable_point: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            flusher_scheduled: std::sync::atomic::AtomicBool::new(false),
        }
    }
}

pub fn load_pompe_config() -> PompeConfig {
    use std::env;

    PompeConfig {
        enable: env::var("POMPE_ENABLE")
            .unwrap_or_else(|_| "true".to_string())
            .parse()
            .unwrap_or(true),
        batch_size: env::var("POMPE_BATCH_SIZE")
            .unwrap_or_else(|_| "10".to_string())
            .parse()
            .unwrap_or(1),
        stable_period_ms: env::var("POMPE_STABLE_PERIOD_MS")
            // Keep default conservative for latency: 50ms
            .unwrap_or_else(|_| "50".to_string())
            .parse()
            .unwrap_or(50),
        leader_node_id: env::var("POMPE_LEADER_NODE_ID")
            .unwrap_or_else(|_| "1".to_string())
            .parse()
            .unwrap_or(1),
        liveness_delta_ms: env::var("POMPE_LIVENESS_DELTA_MS")
            .unwrap_or_else(|_| "10".to_string())
            .parse()
            .unwrap_or(10),
        queue_capacity: env::var("POMPE_QUEUE_CAPACITY")
            .unwrap_or_else(|_| "4096".to_string())
            .parse()
            .unwrap_or(4096),
    }
}

pub struct PompeManager {
    node_id: usize,
    config: PompeConfig,
    state: Arc<PompeAppStateAdversary>,
    nfaulty: usize,
    // 所有节点列表（顺序需一致，用于按视图轮换 leader）
    all_node_ids: Vec<usize>,
    // 当前视图号与是否为当前视图 leader
    current_view: Arc<AtomicU64>,
    is_current_leader: Arc<AtomicBool>,

    ordering1_tx: async_mpsc::Sender<(usize, PompeMessage)>,
    ordering1_rx: Arc<tokio::sync::Mutex<async_mpsc::Receiver<(usize, PompeMessage)>>>,

    ordering2_tx: async_mpsc::Sender<(usize, PompeMessage)>,
    ordering2_rx: Arc<tokio::sync::Mutex<async_mpsc::Receiver<(usize, PompeMessage)>>>,

    general_tx: async_mpsc::Sender<(usize, PompeMessage)>,
    general_rx: Arc<tokio::sync::Mutex<async_mpsc::Receiver<(usize, PompeMessage)>>>,

    // 新增：专用广播通道（Tokio mpsc，避免阻塞 runtime 线程）
    broadcast_tx: async_mpsc::Sender<PompeMessage>,
    broadcast_rx: Arc<tokio::sync::Mutex<Option<async_mpsc::Receiver<PompeMessage>>>>,

    pub network: Option<Arc<crate::pompe_network::PompeNetwork>>,
    lockfree_adapter: Option<Arc<LockFreeHotStuffAdapter>>,
    event_tx: tokio::sync::broadcast::Sender<SystemEvent>,
}

impl PompeManager {
    pub fn get_network(&self) -> Option<&Arc<crate::pompe_network::PompeNetwork>> {
        self.network.as_ref()
    }

    pub async fn get_network_stats(&self) -> Option<(usize, usize)> {
        if let Some(ref network) = self.network {
            Some(network.get_connection_stats().await)
        } else {
            None
        }
    }

    pub fn cleanup_expired_states(&self) {
        if self.state.completed_ordering1.len() > 500 {
            self.state.completed_ordering1.clear();
            debug!(
                "🧹 [清理] Node {} 清理 {} 个已完成交易记录",
                self.node_id, 500
            );
        }

        let orphan_ordering1 = self.state.ordering1_responses.len();
        if orphan_ordering1 > 500 {
            self.state.ordering1_responses.clear();
            self.state.ordering1_count.clear();
            warn!(
                "🧹 [清理] Node {} 清理 {} 个孤儿ordering1状态",
                self.node_id, orphan_ordering1
            );
        }

        if self.state.transaction_initiators.len() > 1000 {
            self.state.transaction_initiators.clear();
            debug!("🧹 [清理] Node {} 清理发起者记录", self.node_id);
        }

        if self.state.completed_ordering2.len() > 1000 {
            self.state.completed_ordering2.clear();
            debug!("🧹 [清理] Node {} 清理ordering2完成标记", self.node_id);
        }
    }

    pub fn new_with_complete_network(
        node_id: usize,
        all_node_ids: Vec<usize>,
        config: PompeConfig,
        _network: impl hotstuff_rs::networking::network::Network + Clone + Send + 'static,
        event_tx: tokio::sync::broadcast::Sender<SystemEvent>,
    ) -> Self {
        let node_num = all_node_ids.len();
        let nfaulty = (node_num - 1) / 3;
        let (general_tx, general_rx) = async_mpsc::channel(config.queue_capacity);

        info!(
            "🚀 创建完整网络支持的Pompe管理器，节点 {}, f={}",
            node_id, nfaulty
        );
        info!("🔍 节点列表: {:?}", all_node_ids);

        let (ord1_tx, ord1_rx) = async_mpsc::channel(config.queue_capacity);
        let (ord2_tx, ord2_rx) = async_mpsc::channel(config.queue_capacity);
        let (broadcast_tx, broadcast_rx) = async_mpsc::channel(config.queue_capacity);

        let network = Arc::new(PompeNetwork::new(node_id, all_node_ids.clone()));

        Self {
            node_id,
            config,
            state: Arc::new(PompeAppStateAdversary::new()),
            nfaulty,
            all_node_ids: all_node_ids.clone(),
            current_view: Arc::new(AtomicU64::new(0)),
            is_current_leader: Arc::new(AtomicBool::new(false)),
            ordering1_tx: ord1_tx,
            ordering1_rx: Arc::new(tokio::sync::Mutex::new(ord1_rx)),
            ordering2_tx: ord2_tx,
            ordering2_rx: Arc::new(tokio::sync::Mutex::new(ord2_rx)),
            general_tx,
            general_rx: Arc::new(tokio::sync::Mutex::new(general_rx)),
            broadcast_tx,
            broadcast_rx: Arc::new(tokio::sync::Mutex::new(Some(broadcast_rx))),
            network: Some(network),
            lockfree_adapter: None,
            event_tx,
        }
    }

    pub fn set_lockfree_adapter(&mut self, adapter: Arc<LockFreeHotStuffAdapter>) {
        self.lockfree_adapter = Some(adapter);
        info!(
            "✅ [完全无锁设置] Node {} 设置无锁HotStuff适配器",
            self.node_id
        );
    }

    pub fn debug_config(&self) {
        info!("🔧 [配置检查] Node {} Pompe配置:", self.node_id);
        info!("  - 启用状态: {}", self.config.enable);
        info!("  - 批次大小: {}", self.config.batch_size);
        info!("  - 稳定周期: {}ms", self.config.stable_period_ms);
        info!("  - 领导者节点: {}", self.config.leader_node_id);
        info!("  - 容错节点数 f: {}", self.nfaulty);
        info!("  - 总节点数: {}", self.nfaulty * 3 + 1);
        info!("  - 需要响应数 (2f+1): {}", 2 * self.nfaulty + 1);

        if let Some(ref network) = self.network {
            info!("  - 网络节点列表: {:?}", network.peer_node_ids);
            info!(
                "  - 当前节点在网络中: {}",
                network.peer_node_ids.contains(&self.node_id)
            );
        } else {
            warn!("  - ⚠️ 网络未配置！");
        }
    }

    // Function to process a raw transaction string and call Ordering1
    pub async fn process_raw_transaction(&self, raw_tx: &str) -> Result<(), String> {
        if !self.config.enable {
            debug!("Pompe未启用，跳过: {}", raw_tx);
            return Ok(());
        }

        if let Some(transaction) =
            PompeTransaction::from_raw_string(raw_tx, format!("client_{}", self.node_id))
        {
            let tx_hash = transaction.hash();

            debug!(
                "📥 [Ordering1] Node {} 处理交易: {} -> Hash: {}, tx_id={}",
                self.node_id,
                raw_tx,
                &tx_hash[0..8],
                transaction.id
            );

            self.state
                .transaction_store
                .insert(tx_hash.clone(), transaction.clone());

            let current_count = self
                .state
                .batch_received
                .entry(tx_hash.clone())
                .and_modify(|count| *count += 1)
                .or_insert(1)
                .clone();

            debug!(
                "📊 [Ordering1] Node {} 批次计数: {} -> {}/{}",
                self.node_id,
                &tx_hash[0..8],
                current_count,
                self.config.batch_size
            );

            if current_count == self.config.batch_size {
                self.state
                    .transaction_initiators
                    .insert(tx_hash.clone(), self.node_id);
                debug!(
                    "📋 [发起者记录] Node {} 记录为交易 {} 的发起者",
                    self.node_id,
                    &tx_hash[0..8]
                );

                // 修复：调用正确的方法
                self.exec_ordering1(tx_hash, transaction).await?;
            } else {
                debug!(
                    "🔄 [Ordering1] Node {} 已有其他节点发起此交易的ordering",
                    self.node_id
                );
            }
        }

        Ok(())
    }

    async fn exec_ordering1(
        &self,
        tx_hash: String,
        transaction: PompeTransaction,
    ) -> Result<(), String> {
        debug!(
            "🚀 [Ordering1-exec] Node {} 发起ordering1阶段: {}",
            self.node_id,
            &tx_hash[0..8]
        );

        let broadcast_start = std::time::Instant::now();

        if let Some(ref network) = self.network {
            let request = PompeMessage::Ordering1Request {
                tx_hash: tx_hash.clone(),
                transaction: transaction.clone(),
                batch_size: self.config.batch_size,
                initiator_node_id: self.node_id,
            };

            // 使用专用广播通道（有界，背压）
            if let Err(e) = self.broadcast_tx.send(request).await {
                warn!("⚠️ [Ordering1-exec] 广播队列已满/关闭: {}", e);
            }

            let broadcast_duration = broadcast_start.elapsed();
            debug!(
                "⏱️ [Ordering1-exec] Node {} 广播耗时: {:?}",
                self.node_id, broadcast_duration
            );
        }

        Ok(())
    }

    pub async fn start_network_message_loop(&self) -> Result<(), String> {
        if let Some(ref network) = self.network {
            info!("🚀 Node {} 启动Pompe网络", self.node_id);

            if let Err(e) = network.start_server() {
                return Err(format!("启动Pompe服务器失败: {}", e));
            }
            // 预热连接，降低首次发送延迟
            network.warm_up_connections();

            // 启动专用广播处理器
            let broadcast_rx = {
                let mut rx_guard = self.broadcast_rx.lock().await;
                rx_guard.take()
            };

            if let Some(mut rx) = broadcast_rx {
                let net = Arc::clone(network);
                network.spawn(async move {
                    info!("📡 启动专用广播处理器");
                    while let Some(msg) = rx.recv().await {
                        if let Err(e) = net.broadcast(msg).await {
                            error!("❌ 专用广播失败: {}", e);
                        }
                    }
                });
            }

            // 监听 HotStuff 视图开始事件，计算当前视图 leader（保留非固定 leader 模式）
            {
                let mut ev_rx = self.event_tx.subscribe();
                let ids = self.all_node_ids.clone();
                let is_leader = self.is_current_leader.clone();
                let cur_view = self.current_view.clone();
                let my_id = self.node_id;
                tokio::spawn(async move {
                    loop {
                        match ev_rx.recv().await {
                            Ok(SystemEvent::StartView { view }) => {
                                cur_view.store(view, Ordering::SeqCst);
                                if !ids.is_empty() {
                                    let idx = (view as usize) % ids.len();
                                    let leader = ids[idx];
                                    let am_leader = leader == my_id;
                                    is_leader.store(am_leader, Ordering::SeqCst);
                                }
                            }
                            Ok(_) => {}
                            Err(_) => break,
                        }
                    }
                });
            }

            let network_clone = Arc::clone(network);
            let node_id = self.node_id;
            let ordering1_tx = self.ordering1_tx.clone();
            let ordering2_tx = self.ordering2_tx.clone();
            let general_tx = self.general_tx.clone();

            network.spawn(async move {
                info!("🌐 Node {} Pompe消息接收循环启动", node_id);
                let mut total_messages = 0;
                let mut ordering1_count = 0;
                let mut ordering2_count = 0;
                
                loop {
                    if let Some((sender_id, message)) = network_clone.recv().await {
                        debug!("📬 [消息接收] Node {} 收到来自节点 {} 的消息", node_id, sender_id);
                        total_messages += 1;

                        match &message {
                            PompeMessage::Ordering1Request { .. } | 
                            PompeMessage::Ordering1Response { .. } => {
                                ordering1_count += 1;
                                debug!("📨 [分发器] Node {} 分发Ordering1消息: {:?} (总计: O1={}, O2={}, 总={})", 
                                    node_id, std::mem::discriminant(&message), ordering1_count, ordering2_count, total_messages);
                                
                                if let Err(e) = ordering1_tx.send((sender_id, message)).await {
                                    error!("❌ Ordering1队列发送失败(背压/关闭): {}", e);
                                }
                            }
                            
                            PompeMessage::Ordering2Request { .. } | 
                            PompeMessage::Ordering2Response { .. } => {
                                ordering2_count += 1;
                                debug!("📨 [分发器] Node {} 分发Ordering2消息: {:?} (总计: O1={}, O2={}, 总={})", 
                                    node_id, std::mem::discriminant(&message), ordering1_count, ordering2_count, total_messages);
                                
                                if let Err(e) = ordering2_tx.send((sender_id, message)).await {
                                    error!("❌ Ordering2队列发送失败(背压/关闭): {}", e);
                                }
                            }
                            
                            _ => {
                                if let Err(e) = general_tx.send((sender_id, message)).await {
                                    error!("❌ 通用队列发送失败(背压/关闭): {}", e);
                                }
                            }
                        }
                    }
                }
            });

            self.start_ordering1_processor().await;
            self.start_ordering2_processor().await;
            self.start_general_processor().await;

            // Periodic cleanup of in-memory state to prevent unbounded growth
            let manager_clone = self.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));
                loop {
                    interval.tick().await;
                    manager_clone.cleanup_expired_states();
                }
            });

            // Periodic flusher: tick every stable_period_ms to output stable batch with tail-cut
            let node_id = self.node_id;
            let state = Arc::clone(&self.state);
            let lockfree_adapter = self.lockfree_adapter.clone();
            let config = self.config.clone();
            let network_for_flush = self.network.as_ref().map(|n| Arc::clone(n));
            let is_leader_flag = self.is_current_leader.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(
                    config.stable_period_ms,
                ));
                loop {
                    interval.tick().await;
                    // 使用与检查路径相同的逻辑
                    if let Some(net) = &network_for_flush {
                        if is_leader_flag.load(Ordering::SeqCst) {
                            // Self::check_and_output_to_hotstuff_lockfree(
                            //     node_id, &state, &lockfree_adapter, &config, net, is_leader_flag.clone()
                            // ).await;
                        }
                    }
                }
            });
        }

        Ok(())
    }

    async fn start_ordering1_processor(&self) {
        let ordering1_rx = Arc::clone(&self.ordering1_rx);
        let state = Arc::clone(&self.state);
        let network = self.network.clone();
        let node_id = self.node_id;
        let nfaulty = self.nfaulty;
        let broadcast_tx = self.broadcast_tx.clone();

        tokio::spawn(async move {
            info!("🔄 Node {} 无锁Ordering1处理器启动", node_id);

            loop {
                let message_opt = {
                    let mut rx = ordering1_rx.lock().await;
                    rx.recv().await
                };
                if let Some((sender_id, message)) = message_opt {
                    match message {
                        PompeMessage::Ordering1Request {
                            tx_hash,
                            transaction,
                            batch_size,
                            initiator_node_id,
                        } => {
                            let tx_id = transaction.id;
                            debug!("收到Ordering1请求: {}, hash = {}", tx_id, tx_hash);
                            if let Some(ref net) = network {
                                Self::handle_ordering1_request_lockfree(
                                    node_id,
                                    &state,
                                    &net,
                                    sender_id,
                                    tx_hash,
                                    transaction,
                                    batch_size,
                                    initiator_node_id,
                                )
                                .await;
                            }
                        }
                        PompeMessage::Ordering1Response {
                            tx_hash,
                            timestamp_us,
                            node_id: sender_node_id,
                            initiator_node_id,
                        } => {
                            if let Some(ref net) = network {
                                Self::handle_ordering1_response_lockfree(
                                    node_id,
                                    &state,
                                    nfaulty,
                                    &net,
                                    &broadcast_tx,
                                    sender_id,
                                    tx_hash,
                                    timestamp_us,
                                    sender_node_id,
                                    initiator_node_id,
                                )
                                .await;
                            }
                        }
                        _ => {}
                    }
                }
            }
        });
    }

    async fn start_ordering2_processor(&self) {
        let ordering2_rx = Arc::clone(&self.ordering2_rx);
        let state = Arc::clone(&self.state);
        let network = self.network.clone();
        let node_id = self.node_id;
        let lockfree_adapter = self.lockfree_adapter.clone();
        let config = self.config.clone();
        let event_tx = self.event_tx.clone();
        let is_leader_flag_for_o2 = self.is_current_leader.clone();

        tokio::spawn(async move {
            info!("🔄 Node {} 无锁Ordering2处理器启动", node_id);

            loop {
                let message_opt = {
                    let mut rx = ordering2_rx.lock().await;
                    rx.recv().await
                };
                if let Some((sender_id, message)) = message_opt {
                    match message {
                        PompeMessage::Ordering2Request {
                            tx_hash,
                            median_timestamp,
                            initiator_node_id,
                            signatures,
                        } => {
                            if let Some(ref net) = network {
                                Self::handle_ordering2_request_lockfree(
                                    node_id,
                                    &state,
                                    &net,
                                    &lockfree_adapter,
                                    &config,
                                    sender_id,
                                    tx_hash,
                                    median_timestamp,
                                    initiator_node_id,
                                    &event_tx,
                                    is_leader_flag_for_o2.clone(),
                                )
                                .await;
                            }
                        }
                        _ => {}
                    }
                }
            }
        });
    }

    async fn start_general_processor(&self) {
        let general_rx = Arc::clone(&self.general_rx);
        let lockfree_adapter = self.lockfree_adapter.clone();
        let node_id = self.node_id;
        tokio::spawn(async move {
            info!("🔄 Node {} 通用消息处理器启动", node_id);
            loop {
                let msg_opt = {
                    let mut rx = general_rx.lock().await;
                    rx.recv().await
                };
                if let Some((_sender_id, message)) = msg_opt {
                    match message {
                        PompeMessage::DeliverOrderedTxs { items, initiator } => {
                            if let Some(ref adapter) = lockfree_adapter {
                                let count = items.len();
                                adapter.push_batch(items);
                                info!("📥 [Ordered投递] Node {} 接收来自 {} 的已排序交易: {} 条，已写入HotStuff队列", node_id, initiator, count);
                            } else {
                                warn!(
                                    "⚠️ [Ordered投递] Node {} 未设置HotStuff适配器，丢弃投递",
                                    node_id
                                );
                            }
                        }
                        _ => {}
                    }
                }
            }
        });
    }

    async fn handle_ordering1_request_lockfree(
        node_id: usize,
        state: &Arc<PompeAppStateAdversary>,
        network: &Arc<crate::pompe_network::PompeNetwork>,
        _sender_id: usize,
        tx_hash: String,
        transaction: PompeTransaction,
        _batch_size: usize,
        initiator_node_id: usize,
    ) {
        let processing_start = std::time::Instant::now();

        debug!(
            "🎯 [handle_ordering1_request] Node {} 处理请求: tx_id={}, hash={}",
            node_id,
            transaction.id,
            &tx_hash[0..8]
        );

        let should_respond = if state.ordering1_responses.contains_key(&tx_hash) {
            false
        } else {
            state.transaction_store.insert(tx_hash.clone(), transaction);
            // warn!("⚠️ [首次Ordering1] Node {} 记录新交易: hash = {}", node_id, &tx_hash[0..8]);
            state
                .ordering1_responses
                .insert(tx_hash.clone(), Vec::new());
            state.ordering1_count.insert(tx_hash.clone(), 0);
            true
        };

        let check_duration = processing_start.elapsed();
        if check_duration > tokio::time::Duration::from_millis(1) {
            debug!(
                "⚠️ [检查耗时] Node {} Ordering1检查耗时: {:?}",
                node_id, check_duration
            );
        }

        if !should_respond {
            debug!(
                "🔄 [handle_ordering1_request] Node {} 已响应过: {}",
                node_id,
                &tx_hash[0..8]
            );
            return;
        }

        let timestamp_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        let tx_hash_clone = tx_hash.clone();
        let response = PompeMessage::Ordering1Response {
            tx_hash,
            timestamp_us,
            node_id,
            initiator_node_id,
        };

        let network_clone = Arc::clone(network);
        let tx_hash_for_async = tx_hash_clone.clone();
        // tokio::spawn(async move {
        if let Err(e) = network_clone
            .send_to_node(initiator_node_id, response)
            .await
        {
            error!("❌ [handle_ordering1_request] 异步发送失败: {}", e);
        }
        info!(
            "📤 [handle_ordering1_request] Node {} 发送Ordering1响应给 Node {}: hash = {}",
            node_id, initiator_node_id, tx_hash_for_async
        );
        // });

        let total_duration = processing_start.elapsed();
        if total_duration > tokio::time::Duration::from_millis(5) {
            debug!(
                "⚠️ [性能] Node {} handle_ordering1_request总耗时: {:?}, hash = {}",
                node_id, total_duration, tx_hash_clone
            );
        } else {
            debug!(
                "✅ [性能] Node {} handle_ordering1_request处理完成: {:?}, hash = {}",
                node_id, total_duration, tx_hash_clone
            );
        }
    }

    async fn handle_ordering1_response_lockfree(
        node_id: usize,
        state: &Arc<PompeAppStateAdversary>,
        nfaulty: usize,
        network: &Arc<crate::pompe_network::PompeNetwork>,
        broadcast_tx: &mpsc::Sender<PompeMessage>,
        _sender_id: usize,
        tx_hash: String,
        timestamp_us: u64,
        sender_node_id: usize,
        initiator_node_id: usize,
    ) {
        let processing_start = std::time::Instant::now();

        if node_id != initiator_node_id {
            return;
        }

        if state.completed_ordering1.contains_key(&tx_hash) {
            return;
        }

        debug!(
            "🌟 [handle_ordering1_response] Node {} 收到时间戳: {}",
            node_id,
            &tx_hash[0..8]
        );

        let should_proceed = {
            if state.completed_ordering1.contains_key(&tx_hash) {
                return;
            }

            let mut timestamps = state
                .ordering1_responses
                .get(&tx_hash)
                .map(|ref_val| ref_val.clone())
                .unwrap_or_else(Vec::new);

            if timestamps.contains(&timestamp_us) {
                return;
            }

            timestamps.push(timestamp_us);
            let current_count = timestamps.len();

            state
                .ordering1_responses
                .insert(tx_hash.clone(), timestamps.clone());
            state.ordering1_count.insert(tx_hash.clone(), current_count);

            let required = 2 * nfaulty + 1;

            if current_count >= required {
                let mut timestamps_sorted = timestamps;
                timestamps_sorted.sort();
                let median = timestamps_sorted[nfaulty];

                state.completed_ordering1.insert(tx_hash.clone(), ());
                state.ordering1_responses.remove(&tx_hash);
                state.ordering1_count.remove(&tx_hash);

                Some(median)
            } else {
                None
            }
        };

        let processing_duration = processing_start.elapsed();
        if processing_duration > tokio::time::Duration::from_millis(2) {
            debug!("⚠️ [处理性能] Node {} handle_ordering1_response 处理耗时: {:?}, 来自 Node {}, hash = {}", node_id, processing_duration, sender_node_id, tx_hash);
        } else {
            debug!("✅ [处理性能] Node {} handle_ordering1_response 处理完成: {:?}, 来自 Node {}, hash = {}", node_id, processing_duration, sender_node_id, tx_hash);
        }

        // warn!("😈 [Adversary] Node {} holds Ordering2 Request: hash = {}", node_id, &tx_hash[0..8]);

        // if let Some(median) = should_proceed {
        //     let msg = PompeMessage::Ordering2Request {
        //         tx_hash: tx_hash.clone(),
        //         median_timestamp: median,
        //         initiator_node_id: initiator_node_id,
        //     };

        //     let log_start = std::time::Instant::now();
        //     // 使用专用广播通道，避免阻塞
        //     if let Err(e) = broadcast_tx.send(msg).await {
        //         warn!("⚠️ [handle_ordering1_response] 广播队列背压/关闭: {}", e);
        //     }
        //     let log_duration = log_start.elapsed();
        //     debug!("⏱️ [性能] PompeManager 广播通道发送耗时: {:?}, hash = {}", log_duration, tx_hash);
        // }
    }

    // 是 handle 完 ordering 1 response 之后call的
    async fn handle_ordering2_request_lockfree(
        node_id: usize,
        state: &Arc<PompeAppStateAdversary>,
        network: &Arc<crate::pompe_network::PompeNetwork>,
        lockfree_adapter: &Option<Arc<LockFreeHotStuffAdapter>>,
        config: &PompeConfig,
        _sender_id: usize,
        tx_hash: String,
        median_timestamp: u64,
        initiator_node_id: usize,
        event_tx: &tokio::sync::broadcast::Sender<SystemEvent>,
        is_leader: Arc<AtomicBool>,
    ) {
        let processing_start = std::time::Instant::now();

        debug!(
            "🚀 [Ordering2-2-LockFree] Node {} 处理请求: {}",
            node_id,
            &tx_hash[0..8]
        );

        let current_stable_point = state
            .stable_point
            .load(std::sync::atomic::Ordering::Relaxed);

        // Sanity check: the median timestamp should not regress past the stable point
        // dummy check
        if median_timestamp < 0 {
            // if median_timestamp < current_stable_point {
            error!("❌ [Ordering2-Stable检查] Node {} 网络异常检测: median_timestamp({}) < stable_point({})", 
                node_id, median_timestamp, current_stable_point);

            let error_response = PompeMessage::Ordering2Response {
                tx_hash,
                timestamp: 0,
                node_id,
            };

            let network_clone = Arc::clone(network);
            // tokio::spawn(async move {
            if let Err(e) = network_clone
                .send_to_node(initiator_node_id, error_response)
                .await
            {
                error!("❌ [Ordering2-错误响应] 发送失败: {}", e);
            }
            // });

            return;
        }
        info!(
            "✅ [Ordering2-2-LockFree] Node {} 检查点处理完成: stable_point = {}",
            node_id, current_stable_point
        );

        let transaction = match state.transaction_store.get(&tx_hash) {
            Some(tx_ref) => tx_ref.clone(),
            None => {
                // warn!("⚠️ [Ordering2-2-LockFree] Node {} 找不到交易: {}", node_id, &tx_hash[0..8]);
                return;
            }
        };

        let tx_id = transaction.id;
        {
            let mut commit_set = state.commit_set.write().unwrap();
            commit_set.push((transaction, median_timestamp));
            drop(commit_set);

            *state.consensus_ready.write().unwrap() = true;
            // Free per-tx state now that it is in the commit pipeline
            state.transaction_store.remove(&tx_hash);
            state.transaction_initiators.remove(&tx_hash);
        }

        let processing_duration = processing_start.elapsed();
        if processing_duration > tokio::time::Duration::from_millis(1) {
            debug!(
                "⚠️ [处理耗时] Node {} Ordering2处理耗时: {:?}, tx_id={}, hash={}",
                node_id, processing_duration, tx_id, tx_hash
            );
        } else {
            debug!(
                "✅ [处理耗时] Node {} Ordering2处理耗时: {:?}, tx_id={}, hash={}",
                node_id, processing_duration, tx_id, tx_hash
            );
        }

        // if tx_id % 10 == 0 {
        let _ = event_tx.send(SystemEvent::PompeOrdering1Completed { tx_id });
        debug!(
            "📡 [Pompe] Node {} 发送 Ordering1 完成事件: tx_id={}",
            node_id, tx_id
        );
        // }

        let response = PompeMessage::Ordering2Response {
            tx_hash,
            timestamp: median_timestamp,
            node_id,
        };

        let network_clone = Arc::clone(network);
        tokio::spawn(async move {
            if let Err(e) = network_clone
                .send_to_node(initiator_node_id, response)
                .await
            {
                error!("❌ [Ordering2-2-LockFree] 异步发送失败: {}", e);
            }
        });

        // let state_clone = Arc::clone(state);
        // let lockfree_adapter_clone = lockfree_adapter.clone();
        // let config_clone = config.clone();
        // let network_clone_for_flush = Arc::clone(network);
        if is_leader.load(Ordering::SeqCst) {
            // Self::check_and_output_to_hotstuff_lockfree(
            //     node_id, &state_clone, &lockfree_adapter_clone, &config_clone, &network_clone_for_flush, is_leader.clone()
            // ).await;
        }
    }

    async fn check_and_output_to_hotstuff_lockfree(
        node_id: usize,
        state: &Arc<PompeAppStateAdversary>,
        lockfree_adapter: &Option<Arc<LockFreeHotStuffAdapter>>,
        config: &PompeConfig,
        network: &Arc<crate::pompe_network::PompeNetwork>,
        is_leader: Arc<AtomicBool>,
    ) {
        // 非 leader 直接返回
        warn!("check_and_output_to_hotstuff_lockfree is triggered!");
        if !is_leader.load(Ordering::SeqCst) {
            return;
        }
        let check_start = std::time::Instant::now();

        let commit_set_len = {
            let commit_set = state.commit_set.read().unwrap();
            commit_set.len()
        };

        let consensus_ready = *state.consensus_ready.read().unwrap();

        if commit_set_len == 0 || !consensus_ready {
            return;
        }

        let current_time_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        let ordered_txs = {
            let mut last_batch_clock = state.exec_last_batch_clock.write().unwrap();

            if *last_batch_clock == 0 {
                *last_batch_clock = current_time_us;
                return;
            }

            let time_elapsed = current_time_us - *last_batch_clock;
            let required_wait = config.stable_period_ms * 1000; // 50ms

            if time_elapsed >= required_wait {
                *last_batch_clock = current_time_us;
                drop(last_batch_clock);

                let mut commit_set = state.commit_set.write().unwrap();

                if commit_set.is_empty() {
                    return;
                }

                commit_set.sort_by_key(|&(_, timestamp)| timestamp);

                // 批次剪尾: 截止点 = 最新时间戳 - liveness_delta
                let delta_us = (config.liveness_delta_ms.saturating_mul(1000)) as u64;
                let batch_end_ts = commit_set
                    .last()
                    .map(|&(_, ts)| ts.saturating_sub(delta_us))
                    .unwrap_or(0);

                let mut cut_idx = 0usize;
                while cut_idx < commit_set.len() {
                    if commit_set[cut_idx].1 > batch_end_ts {
                        break;
                    }
                    cut_idx += 1;
                }

                if cut_idx == 0 {
                    // 没有足够稳定的交易，等待下次周期 flush
                    *state.consensus_ready.write().unwrap() = true;
                    return;
                }

                // 更新 stable_point
                let latest_ts = commit_set[cut_idx - 1].1;
                let old_stable_point = state
                    .stable_point
                    .fetch_max(latest_ts, std::sync::atomic::Ordering::Relaxed);
                info!(
                    "📊 [稳定点] Node {} 更新stable_point: {} -> {}",
                    node_id, old_stable_point, latest_ts
                );

                let txs: Vec<String> = commit_set
                    .iter()
                    .take(cut_idx)
                    .map(|(tx, timestamp)| tx.to_hotstuff_format(*timestamp))
                    .collect();

                // 移除已输出部分
                commit_set.drain(0..cut_idx);
                drop(commit_set);
                *state.consensus_ready.write().unwrap() = false;

                txs
            } else {
                // 未到稳定期：安排一次定时刷新
                let remaining_us = required_wait - time_elapsed;
                if !state
                    .flusher_scheduled
                    .swap(true, std::sync::atomic::Ordering::SeqCst)
                {
                    let state_clone = Arc::clone(state);
                    let lockfree_adapter_clone = lockfree_adapter.clone();
                    let config_clone = config.clone();
                    let network_clone = Arc::clone(network);
                    let leader_flag = is_leader.clone();
                    info!(
                        "⏳ [Flusher] Node {} 安排定时刷新，剩余 {:?}us",
                        node_id, remaining_us
                    );
                    tokio::spawn(async move {
                        tokio::time::sleep(tokio::time::Duration::from_micros(remaining_us)).await;
                        // 到点执行一次刷新
                        Self::flush_commit_set_to_hotstuff(
                            node_id,
                            &state_clone,
                            &lockfree_adapter_clone,
                            &config_clone,
                            Some(network_clone),
                            leader_flag.clone(),
                        )
                        .await;
                        state_clone
                            .flusher_scheduled
                            .store(false, std::sync::atomic::Ordering::SeqCst);
                    });
                }
                Vec::new()
            }
        };

        let processing_duration = check_start.elapsed();
        if processing_duration > tokio::time::Duration::from_millis(2) {
            debug!(
                "⚠️ [输出耗时] Node {} 输出检查耗时: {:?}",
                node_id, processing_duration
            );
        }

        if !ordered_txs.is_empty() {
            // 修改：所有节点均可注入到本地 HotStuff 队列，避免非 leader 产生空块
            if let Some(ref adapter) = lockfree_adapter {
                let cnt = ordered_txs.len();
                adapter.push_batch(ordered_txs.clone());
                info!(
                    "⚡ [输出] Node {} 注入 {} 个已排序交易到 HotStuff 队列",
                    node_id, cnt
                );
            } else {
                warn!(
                    "⚠️ [输出] Node {} 无锁适配器未设置，丢失 {} 个交易",
                    node_id,
                    ordered_txs.len()
                );
            }
        }
    }

    async fn flush_commit_set_to_hotstuff(
        node_id: usize,
        state: &Arc<PompeAppStateAdversary>,
        lockfree_adapter: &Option<Arc<LockFreeHotStuffAdapter>>,
        config: &PompeConfig,
        network: Option<Arc<crate::pompe_network::PompeNetwork>>,
        is_leader: Arc<AtomicBool>,
    ) {
        if !is_leader.load(Ordering::SeqCst) {
            return;
        }
        let now_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        let mut last_batch_clock = state.exec_last_batch_clock.write().unwrap();
        *last_batch_clock = now_us;
        drop(last_batch_clock);

        let mut commit_set = state.commit_set.write().unwrap();
        if commit_set.is_empty() {
            return;
        }
        commit_set.sort_by_key(|&(_, ts)| ts);
        if let Some(&(_, latest_ts)) = commit_set.last() {
            let old = state
                .stable_point
                .fetch_max(latest_ts, std::sync::atomic::Ordering::Relaxed);
            info!(
                "📊 [Flusher] Node {} 刷新 stable_point: {} -> {} ({} 条)",
                node_id,
                old,
                latest_ts,
                commit_set.len()
            );
        }
        let txs: Vec<String> = commit_set
            .iter()
            .map(|(tx, ts)| tx.to_hotstuff_format(*ts))
            .collect();
        commit_set.clear();
        drop(commit_set);
        *state.consensus_ready.write().unwrap() = false;

        // 修改：所有节点均注入本地 HotStuff 队列，减少空块概率
        if let Some(ref adapter) = lockfree_adapter {
            let count = txs.len();
            adapter.push_batch(txs.clone());
            info!("⚡ [定时输出] Node {} 刷新输出 {} 个交易", node_id, count);
        } else {
            warn!(
                "⚠️ [定时输出] Node {} 无锁适配器未设置，丢失 {} 个交易",
                node_id,
                txs.len()
            );
        }
    }

    pub fn get_detailed_stats(&self) -> (usize, usize, usize, bool, u64, usize, usize) {
        let batch_count = self.state.batch_received.len();
        let ordering1_count = self.state.ordering1_responses.len();
        let transaction_store_len = self.state.transaction_store.len();
        let transaction_initiators_len = self.state.transaction_initiators.len();

        let commit_set_len = {
            let commit_set = self.state.commit_set.read().unwrap();
            commit_set.len()
        };
        let consensus_ready = *self.state.consensus_ready.read().unwrap();
        let exec_last_batch_clock = *self.state.exec_last_batch_clock.read().unwrap();

        (
            batch_count,
            ordering1_count,
            commit_set_len,
            consensus_ready,
            exec_last_batch_clock,
            transaction_store_len,
            transaction_initiators_len,
        )
    }

    pub fn is_enabled(&self) -> bool {
        self.config.enable
    }

    pub fn clone(&self) -> Self {
        Self {
            node_id: self.node_id,
            config: self.config.clone(),
            state: Arc::clone(&self.state),
            nfaulty: self.nfaulty,
            all_node_ids: self.all_node_ids.clone(),
            current_view: Arc::clone(&self.current_view),
            is_current_leader: Arc::clone(&self.is_current_leader),
            ordering1_tx: self.ordering1_tx.clone(),
            ordering1_rx: Arc::clone(&self.ordering1_rx),
            ordering2_tx: self.ordering2_tx.clone(),
            ordering2_rx: Arc::clone(&self.ordering2_rx),
            general_tx: self.general_tx.clone(),
            general_rx: Arc::clone(&self.general_rx),
            broadcast_tx: self.broadcast_tx.clone(),
            broadcast_rx: Arc::clone(&self.broadcast_rx),
            network: self.network.as_ref().map(|n| Arc::clone(n)),
            lockfree_adapter: self.lockfree_adapter.clone(),
            event_tx: self.event_tx.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_pompe_transaction_parsing() {
        let raw_tx = "123:alice->bob:100";
        let tx = PompeTransaction::from_raw_string(raw_tx, "client_1".to_string());

        assert!(tx.is_some());
        let tx = tx.unwrap();
        assert_eq!(tx.id, 123);
        assert_eq!(tx.from, "alice");
        assert_eq!(tx.to, "bob");
        assert_eq!(tx.amount, 100);
    }

    #[test]
    fn test_transaction_hash() {
        let tx = PompeTransaction {
            id: 1,
            from: "alice".to_string(),
            to: "bob".to_string(),
            amount: 100,
            client_id: "test".to_string(),
            timestamp: 0,
            nonce: 0,
        };
        let hash = tx.hash();
        assert!(!hash.is_empty());
        assert_eq!(hash.len(), 64);
    }

    #[test]
    fn test_hotstuff_format() {
        let tx = PompeTransaction {
            id: 1,
            from: "alice".to_string(),
            to: "bob".to_string(),
            amount: 100,
            client_id: "test".to_string(),
            timestamp: 0,
            nonce: 0,
        };
        let formatted = tx.to_hotstuff_format(1234567890);
        assert!(formatted.starts_with("pompe:1234567890:1:alice->bob:100"));
    }
}
