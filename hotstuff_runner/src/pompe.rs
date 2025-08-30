// hotstuff_runner/src/pompe.rs
//! 完全无锁化的Pompe BFT实现 - 支持crossbeam无锁队列

use std::collections::{HashMap, VecDeque, BTreeMap};
use std::sync::{Arc, RwLock};
use dashmap::DashMap;
use std::time::{SystemTime, UNIX_EPOCH, Duration, Instant};
use serde::{Serialize, Deserialize};
use tokio::sync::mpsc;
use hotstuff_rs::types::crypto_primitives::VerifyingKey;
use ed25519_dalek::SigningKey;
use tracing::{info, warn, error, debug};
use sha2::{Sha256, Digest};
use std::net::SocketAddr;
use crate::{pompe_network::PompeNetwork, tcp_network::TcpNetwork};
use crossbeam::queue::SegQueue;
use crossbeam::channel::{unbounded, Sender, Receiver};
use crate::event::SystemEvent; 

#[derive(Serialize, Deserialize, Clone, Debug, Hash, PartialEq, Eq)]
pub struct PompeTransaction {
    pub id: u64,
    pub from: String,
    pub to: String,
    pub amount: u64,
    pub client_id: String,
    pub timestamp: u64,
    pub nonce: u64,
}

impl PompeTransaction {
    pub fn hash(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(format!("{}:{}:{}:{}", self.id, self.from, self.to, self.amount));
        format!("{:x}", hasher.finalize())
    }

    pub fn from_raw_string(raw: &str, client_id: String) -> Option<Self> {
        let parts: Vec<&str> = raw.split(':').collect();
        
        debug!("🔍 [解析] 输入: '{}', 分割结果: {:?}", raw, parts);
        
        if parts.len() >= 6 && parts[0] == "pompe" {
            debug!("🔍 [解析] 跳过已排序的pompe交易: {}", raw);
            return None;
        }
        
        if parts.len() == 3 {
            if let Ok(id) = parts[0].parse::<u64>() {
                let from_to_amount = parts[1];
                let amount_str = parts[2];
                
                if let Ok(amount) = amount_str.parse::<u64>() {
                    if let Some(arrow_pos) = from_to_amount.find("->") {
                        let from = from_to_amount[..arrow_pos].to_string();
                        let to = from_to_amount[arrow_pos + 2..].to_string();
                        
                        return Some(Self {
                            id,
                            from,
                            to,
                            amount,
                            client_id,
                            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
                            nonce: 0,
                        });
                    }
                }
            }
        }
        else if parts.len() == 4 {
            if let (Ok(id), Ok(amount)) = (parts[0].parse::<u64>(), parts[3].parse::<u64>()) {
                let from = parts[1].to_string();
                let to = parts[2].to_string();
                
                return Some(Self {
                    id,
                    from,
                    to,
                    amount,
                    client_id,
                    timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
                    nonce: 0,
                });
            }
        }
        
        error!("❌ [解析] 无法解析交易格式: {} (parts: {:?})", raw, parts);
        None
    }

    pub fn to_hotstuff_format(&self, ordering_timestamp: u64) -> String {
        format!("pompe:{}:{}:{}->{}:{}", 
                ordering_timestamp, self.id, self.from, self.to, self.amount)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum PompeMessage {
    Ordering1Request {
        tx_hash: String,
        transaction: PompeTransaction,
        batch_size: usize,
        initiator_node_id: usize,
    },
    Ordering1Response {
        tx_hash: String,
        timestamp_us: u64,
        node_id: usize,
        initiator_node_id: usize,
    },
    Ordering2Request {
        tx_hash: String,
        median_timestamp: u64,
        initiator_node_id: usize,
    },
    Ordering2Response {
        tx_hash: String,
        timestamp: u64,
        node_id: usize,
    },
}

#[derive(Debug, Clone)]
pub struct PompeConfig {
    pub enable: bool,
    pub batch_size: usize,
    pub stable_period_ms: u64,
    pub leader_node_id: usize,
}

impl Default for PompeConfig {
    fn default() -> Self {
        Self {
            enable: true,
            batch_size: 1,
            stable_period_ms: 50,
            leader_node_id: 1,
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
            .unwrap_or(10),
        stable_period_ms: env::var("POMPE_STABLE_PERIOD_MS")
            .unwrap_or_else(|_| "1000".to_string())
            .parse()
            .unwrap_or(1000),
        leader_node_id: env::var("POMPE_LEADER_NODE_ID")
            .unwrap_or_else(|_| "1".to_string())
            .parse()
            .unwrap_or(1),
    }
}

#[derive(Debug)]
struct PompeAppState {
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
    stable_point: Arc<RwLock<u64>>,
}

impl PompeAppState {
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
            stable_point: Arc::new(RwLock::new(0)),
        }
    }
}

#[derive(Debug)]
pub struct LockFreeHotStuffAdapter {
    external_queue: Option<Arc<SegQueue<String>>>,
}

impl LockFreeHotStuffAdapter {
    pub fn new() -> Self {
        Self {
            external_queue: None,
        }
    }
    
    pub fn connect_to_queue(&mut self, queue: Arc<SegQueue<String>>) {
        self.external_queue = Some(queue);
        debug!("📈 [无锁适配器] 连接到外部HotStuff队列");
    }
    
    pub fn push(&self, item: String) {
        if let Some(ref queue) = self.external_queue {
            queue.push(item);
            debug!("📈 [无锁适配器] 添加交易到HotStuff队列");
        } else {
            warn!("⚠️ [无锁适配器] 外部队列未连接，丢失交易: {}", 
                  if item.len() > 50 { &item[0..50] } else { &item });
        }
    }
    
    pub fn push_batch(&self, items: Vec<String>) {
        let items_count = items.len();
        if let Some(ref queue) = self.external_queue {
            for item in items {
                queue.push(item);
            }
            debug!("📈 [无锁适配器] 批量添加 {} 个交易到HotStuff队列", items_count);
        } else {
            warn!("⚠️ [无锁适配器] 外部队列未连接，丢失 {} 个交易", items_count);
        }
    }
}

pub struct PompeManager {
    node_id: usize,
    config: PompeConfig,
    state: Arc<PompeAppState>,
    nfaulty: usize,
    
    ordering1_tx: Sender<(usize, PompeMessage)>,
    ordering1_rx: Receiver<(usize, PompeMessage)>,
    
    ordering2_tx: Sender<(usize, PompeMessage)>,
    ordering2_rx: Receiver<(usize, PompeMessage)>,
    
    general_tx: Sender<(usize, PompeMessage)>,
    general_rx: Receiver<(usize, PompeMessage)>,
    
    // 新增：专用广播通道
    broadcast_tx: mpsc::UnboundedSender<PompeMessage>,
    broadcast_rx: Arc<tokio::sync::Mutex<Option<mpsc::UnboundedReceiver<PompeMessage>>>>,
    
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
            debug!("🧹 [清理] Node {} 清理 {} 个已完成交易记录", 
                  self.node_id, 500);
        }
        
        let orphan_ordering1 = self.state.ordering1_responses.len();
        if orphan_ordering1 > 500 {
            self.state.ordering1_responses.clear();
            self.state.ordering1_count.clear();
            warn!("🧹 [清理] Node {} 清理 {} 个孤儿ordering1状态", 
                  self.node_id, orphan_ordering1);
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
        _tcp_network: TcpNetwork,
        event_tx: tokio::sync::broadcast::Sender<SystemEvent>,
    ) -> Self {
        let node_num = all_node_ids.len();
        let nfaulty = (node_num - 1) / 3;
        let (general_tx, general_rx) = unbounded();
        
        info!("🚀 创建完整网络支持的Pompe管理器，节点 {}, f={}", node_id, nfaulty);
        info!("🔍 节点列表: {:?}", all_node_ids);

        let (ord1_tx, ord1_rx) = unbounded();
        let (ord2_tx, ord2_rx) = unbounded();
        let (broadcast_tx, broadcast_rx) = mpsc::unbounded_channel();
        
        let network = Arc::new(PompeNetwork::new(node_id, all_node_ids));
        
        Self {
            node_id,
            config,
            state: Arc::new(PompeAppState::new()),
            nfaulty,
            ordering1_tx: ord1_tx,
            ordering1_rx: ord1_rx,
            ordering2_tx: ord2_tx,
            ordering2_rx: ord2_rx,
            general_tx,
            general_rx,
            broadcast_tx,
            broadcast_rx: Arc::new(tokio::sync::Mutex::new(Some(broadcast_rx))),
            network: Some(network),
            lockfree_adapter: None,
            event_tx, 
        }
    }

    pub fn set_lockfree_adapter(&mut self, adapter: Arc<LockFreeHotStuffAdapter>) {
        self.lockfree_adapter = Some(adapter);
        info!("✅ [完全无锁设置] Node {} 设置无锁HotStuff适配器", self.node_id);
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
            info!("  - 当前节点在网络中: {}", network.peer_node_ids.contains(&self.node_id));
        } else {
            warn!("  - ⚠️ 网络未配置！");
        }
    }

    pub async fn process_raw_transaction(&self, raw_tx: &str) -> Result<(), String> {
        if !self.config.enable {
            return Ok(());
        }

        if let Some(transaction) = PompeTransaction::from_raw_string(raw_tx, format!("client_{}", self.node_id)) {
            let tx_hash = transaction.hash();
            
            debug!("📥 [Ordering1] Node {} 处理交易: {} -> Hash: {}", 
                self.node_id, raw_tx, &tx_hash[0..8]);
            
            self.state.transaction_store.insert(tx_hash.clone(), transaction.clone());
            
            let current_count = self.state.batch_received
                .entry(tx_hash.clone())
                .and_modify(|count| *count += 1)
                .or_insert(1)
                .clone();
            
            debug!("📊 [Ordering1] Node {} 批次计数: {} -> {}/{}", 
                self.node_id, &tx_hash[0..8], current_count, self.config.batch_size);
            
            if current_count == self.config.batch_size {
                self.state.transaction_initiators.insert(tx_hash.clone(), self.node_id);
                debug!("📋 [发起者记录] Node {} 记录为交易 {} 的发起者", 
                    self.node_id, &tx_hash[0..8]);
                
                // 修复：调用正确的方法
                self.exec_ordering1(tx_hash, transaction).await?;
            } else {
                debug!("🔄 [Ordering1] Node {} 已有其他节点发起此交易的ordering", self.node_id);
            }
        }
        
        Ok(())
    }

    async fn exec_ordering1(&self, tx_hash: String, transaction: PompeTransaction) -> Result<(), String> {
        debug!("🚀 [Ordering1-exec] Node {} 发起ordering1阶段: {}", self.node_id, &tx_hash[0..8]);

        let broadcast_start = std::time::Instant::now();
        
        if let Some(ref network) = self.network {
            let request = PompeMessage::Ordering1Request {
                tx_hash: tx_hash.clone(),
                transaction: transaction.clone(),
                batch_size: self.config.batch_size,
                initiator_node_id: self.node_id, 
            };
            
            // 使用专用广播通道，避免阻塞
            let _ = self.broadcast_tx.send(request);

            let broadcast_duration = broadcast_start.elapsed();
            debug!("⏱️ [Ordering1-exec] Node {} 广播耗时: {:?}", self.node_id, broadcast_duration);
        }
        
        Ok(())
    }

    pub fn get_ordered_transactions(&self) -> Vec<String> {
        let commit_set_size = {
            let commit_set = self.state.commit_set.read().unwrap();
            commit_set.len()
        };
        
        if commit_set_size == 0 {
            debug!("🔍 [输出检查] Node {} 提交集为空", self.node_id);
            return Vec::new();
        }
        
        let consensus_ready = *self.state.consensus_ready.read().unwrap();
        if !consensus_ready {
            debug!("🔍 [输出] Node {} consensus未就绪", self.node_id);
            return Vec::new();
        }
        
        let current_time_us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        
        let ordered_txs = {
            let mut last_batch_clock = self.state.exec_last_batch_clock.write().unwrap();
            
            if *last_batch_clock == 0 {
                debug!("🔍 [输出] Node {} 初始化执行时间戳", self.node_id);
                *last_batch_clock = current_time_us;
                return Vec::new();
            }
            
            let time_elapsed = current_time_us - *last_batch_clock;
            let required_wait = self.config.stable_period_ms * 1000;
            
            if time_elapsed < required_wait {
                debug!("🔍 [输出] Node {} 还需等待 {}μs", 
                    self.node_id, required_wait - time_elapsed);
                return Vec::new();
            }
            
            *last_batch_clock = current_time_us;
            drop(last_batch_clock);
            
            let mut commit_set = self.state.commit_set.write().unwrap();
            
            let batch_size = std::cmp::min(commit_set.len(), 50);
            
            debug!("🚀 [输出] Node {} 分批输出 {}/{} 个交易", 
                self.node_id, batch_size, commit_set.len());
            
            commit_set.sort_by_key(|&(_, timestamp)| timestamp);
            
            let ordered_txs: Vec<String> = commit_set
                .iter()
                .take(batch_size)
                .map(|(tx, timestamp)| {
                    let formatted = tx.to_hotstuff_format(*timestamp);
                    info!("📤 [输出] Node {} 排序交易: {} -> {}", 
                        self.node_id, tx.id, formatted);
                    formatted
                })
                .collect();

            if let Some(&(_, latest_timestamp)) = commit_set.last() {
                let mut stable_point = self.state.stable_point.write().unwrap();
                let old_stable_point = *stable_point;
                *stable_point = latest_timestamp;
                drop(stable_point);
                
                debug!("📊 [稳定点] Node {} 更新stable_point: {} -> {}", 
                    self.node_id, old_stable_point, latest_timestamp);
            }
            
            commit_set.drain(0..batch_size);
            
            if commit_set.is_empty() {
                drop(commit_set);
                *self.state.consensus_ready.write().unwrap() = false;
                debug!("✅ [输出完成] Node {} 所有交易已输出，重置consensus_ready", self.node_id);
            } else {
                info!("⏳ [输出继续] Node {} 还有 {} 个交易等待下次输出", 
                    self.node_id, commit_set.len());
            }
            
            ordered_txs
        };
        
        info!("✅ [输出] Node {} 本次输出 {} 个交易", self.node_id, ordered_txs.len());
        
        ordered_txs
    }

    pub async fn start_network_message_loop(&self) -> Result<(), String> {
        if let Some(ref network) = self.network {
            info!("🚀 Node {} 启动Pompe网络", self.node_id);
            
            if let Err(e) = network.start_server().await {
                return Err(format!("启动Pompe服务器失败: {}", e));
            }
            
            // 启动专用广播处理器
            let broadcast_rx = {
                let mut rx_guard = self.broadcast_rx.lock().await;
                rx_guard.take()
            };
            
            if let Some(mut rx) = broadcast_rx {
                let network_for_broadcast = Arc::clone(network);
                tokio::spawn(async move {
                    info!("📡 启动专用广播处理器");
                    while let Some(msg) = rx.recv().await {
                        if let Err(e) = network_for_broadcast.broadcast(msg).await {
                            error!("❌ 专用广播失败: {}", e);
                        }
                    }
                });
            }
            
            let network_clone = Arc::clone(network);
            let node_id = self.node_id;
            let ordering1_tx = self.ordering1_tx.clone();
            let ordering2_tx = self.ordering2_tx.clone();
            let general_tx = self.general_tx.clone();
            
            tokio::spawn(async move {
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
                                
                                if let Err(e) = ordering1_tx.send((sender_id, message)) {
                                    error!("❌ Ordering1队列发送失败: {}", e);
                                }
                            }
                            
                            PompeMessage::Ordering2Request { .. } | 
                            PompeMessage::Ordering2Response { .. } => {
                                ordering2_count += 1;
                                debug!("📨 [分发器] Node {} 分发Ordering2消息: {:?} (总计: O1={}, O2={}, 总={})", 
                                    node_id, std::mem::discriminant(&message), ordering1_count, ordering2_count, total_messages);
                                
                                if let Err(e) = ordering2_tx.send((sender_id, message)) {
                                    error!("❌ Ordering2队列发送失败: {}", e);
                                }
                            }
                            
                            _ => {
                                if let Err(e) = general_tx.send((sender_id, message)) {
                                    error!("❌ 通用队列发送失败: {}", e);
                                }
                            }
                        }
                    }
                }
            });

            self.start_ordering1_processor().await;
            self.start_ordering2_processor().await;
        }
        
        Ok(())
    }

    async fn start_ordering1_processor(&self) {
        let ordering1_rx = self.ordering1_rx.clone();
        let state = Arc::clone(&self.state);
        let network = self.network.clone();
        let node_id = self.node_id;
        let nfaulty = self.nfaulty;
        let broadcast_tx = self.broadcast_tx.clone();
        
        tokio::spawn(async move {
            info!("🔄 Node {} 无锁Ordering1处理器启动", node_id);
            
            loop {
                let message_opt = ordering1_rx.try_recv().ok();
                
                if let Some((sender_id, message)) = message_opt {
                    match message {
                        PompeMessage::Ordering1Request { tx_hash, transaction, batch_size, initiator_node_id } => {
                            let tx_id=transaction.id;
                            info!("**** 收到Ordering1请求: {}, hash = {}", tx_id,tx_hash);
                            if let Some(ref net) = network {
                                Self::handle_ordering1_request_lockfree(
                                    node_id, &state, &net,
                                    sender_id, tx_hash, transaction, batch_size, initiator_node_id
                                ).await;
                            }
                        }
                        PompeMessage::Ordering1Response { tx_hash, timestamp_us, node_id: sender_node_id, initiator_node_id } => {
                            if let Some(ref net) = network {
                                Self::handle_ordering1_response_lockfree(
                                    node_id, &state, nfaulty, &net, &broadcast_tx,
                                    sender_id, tx_hash, timestamp_us, sender_node_id, initiator_node_id
                                ).await;
                            }
                        }
                        _ => {}
                    }
                }
            }
        });
    }

    async fn start_ordering2_processor(&self) {
        let ordering2_rx = self.ordering2_rx.clone();
        let state = Arc::clone(&self.state);
        let network = self.network.clone();
        let node_id = self.node_id;
        let lockfree_adapter = self.lockfree_adapter.clone();
        let config = self.config.clone();
        let event_tx = self.event_tx.clone();
        
        tokio::spawn(async move {
            info!("🔄 Node {} 无锁Ordering2处理器启动", node_id);
            
            loop {
                if let Ok((sender_id, message)) = ordering2_rx.try_recv() {
                    match message {
                        PompeMessage::Ordering2Request { tx_hash, median_timestamp, initiator_node_id } => {
                            if let Some(ref net) = network {
                                Self::handle_ordering2_request_lockfree(
                                    node_id, &state, &net, &lockfree_adapter, &config,
                                    sender_id, tx_hash, median_timestamp, initiator_node_id, &event_tx
                                ).await;
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
        state: &Arc<PompeAppState>,
        network: &Arc<crate::pompe_network::PompeNetwork>,
        _sender_id: usize,
        tx_hash: String,
        transaction: PompeTransaction,
        _batch_size: usize,
        initiator_node_id: usize,
    ) {
        let processing_start = std::time::Instant::now();
        
        debug!("🎯 [Ordering1-2-LockFree] Node {} 处理请求: {}", node_id, &tx_hash[0..8]);
        
        let should_respond = if state.ordering1_responses.contains_key(&tx_hash) {
            false
        } else {
            state.transaction_store.insert(tx_hash.clone(), transaction);
            state.ordering1_responses.insert(tx_hash.clone(), Vec::new());
            state.ordering1_count.insert(tx_hash.clone(), 0);
            true
        };
        
        let check_duration = processing_start.elapsed();
        if check_duration > tokio::time::Duration::from_millis(1) {
            warn!("⚠️ [检查耗时] Node {} Ordering1检查耗时: {:?}", node_id, check_duration);
        }
        
        if !should_respond {
            debug!("🔄 [Ordering1-2-LockFree] Node {} 已响应过: {}", node_id, &tx_hash[0..8]);
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
        tokio::spawn(async move {
            if let Err(e) = network_clone.send_to_node(initiator_node_id, response).await {
                error!("❌ [Ordering1-2-LockFree] 异步发送失败: {}", e);
            }
        });
        
        let total_duration = processing_start.elapsed();
        if total_duration > tokio::time::Duration::from_millis(5) {
            warn!("⚠️ [性能] Node {} Ordering1-1 request总耗时: {:?}, hash = {}", node_id, total_duration, tx_hash_clone);
        } else {
            debug!("✅ [性能] Node {} Ordering1-1 request处理完成: {:?}, hash = {}", node_id, total_duration, tx_hash_clone);
        }
    }

    async fn handle_ordering1_response_lockfree(
        node_id: usize,
        state: &Arc<PompeAppState>,
        nfaulty: usize,
        network: &Arc<crate::pompe_network::PompeNetwork>,
        broadcast_tx: &mpsc::UnboundedSender<PompeMessage>,
        _sender_id: usize,
        tx_hash: String,
        timestamp_us: u64,
        sender_node_id: usize,
        initiator_node_id: usize
    ) {
        let processing_start = std::time::Instant::now();
        
        if node_id != initiator_node_id {
            return;
        }
        
        if state.completed_ordering1.contains_key(&tx_hash) {
            return;
        }
        
        debug!("🌟 [Ordering1-3-LockFree] Node {} 收到时间戳: {}", node_id, &tx_hash[0..8]);
        
        let should_proceed = {
            if state.completed_ordering1.contains_key(&tx_hash) {
                return;
            }
            
            let mut timestamps = state.ordering1_responses
                .get(&tx_hash)
                .map(|ref_val| ref_val.clone())
                .unwrap_or_else(Vec::new);
            
            if timestamps.contains(&timestamp_us) {
                return;
            }
            
            timestamps.push(timestamp_us);
            let current_count = timestamps.len();
            
            state.ordering1_responses.insert(tx_hash.clone(), timestamps.clone());
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
            warn!("⚠️ [处理性能] Node {} Ordering1-2 response 处理耗时: {:?}, hash = {}", node_id, processing_duration, tx_hash);
        } else {
            debug!("✅ [处理性能] Node {} Ordering1-2 response 处理完成: {:?}, hash = {}", node_id, processing_duration, tx_hash);
        }

        if let Some(median) = should_proceed {
            let msg = PompeMessage::Ordering2Request {
                tx_hash: tx_hash.clone(),
                median_timestamp: median,
                initiator_node_id: initiator_node_id,
            };
            
            // 使用专用广播通道，避免阻塞
            let _ = broadcast_tx.send(msg);
        }
    }

    async fn handle_ordering2_request_lockfree(
        node_id: usize,
        state: &Arc<PompeAppState>,
        network: &Arc<crate::pompe_network::PompeNetwork>,
        lockfree_adapter: &Option<Arc<LockFreeHotStuffAdapter>>,
        config: &PompeConfig,
        _sender_id: usize,
        tx_hash: String,
        median_timestamp: u64,
        initiator_node_id: usize,
        event_tx: &tokio::sync::broadcast::Sender<SystemEvent>, 
    ) {
        let processing_start = std::time::Instant::now();
        
        debug!("🚀 [Ordering2-2-LockFree] Node {} 处理请求: {}", node_id, &tx_hash[0..8]);

        let current_stable_point = {
            let stable_point = state.stable_point.read().unwrap();
            *stable_point
        };
        
        if median_timestamp < current_stable_point {
            error!("❌ [Ordering2-Stable检查] Node {} 网络异常检测: median_timestamp({}) < stable_point({})", 
                node_id, median_timestamp, current_stable_point);
            
            let error_response = PompeMessage::Ordering2Response {
                tx_hash,
                timestamp: 0,
                node_id,
            };
            
            let network_clone = Arc::clone(network);
            tokio::spawn(async move {
                if let Err(e) = network_clone.send_to_node(initiator_node_id, error_response).await {
                    error!("❌ [Ordering2-错误响应] 发送失败: {}", e);
                }
            });
            
            return;
        }

        let transaction = match state.transaction_store.get(&tx_hash) {
            Some(tx_ref) => tx_ref.clone(),
            None => {
                warn!("⚠️ [Ordering2-2-LockFree] Node {} 找不到交易: {}", node_id, &tx_hash[0..8]);
                return;
            }
        };

        let tx_id = transaction.id;
        {
            let mut commit_set = state.commit_set.write().unwrap();
            commit_set.push((transaction, median_timestamp));
            drop(commit_set);
            
            *state.consensus_ready.write().unwrap() = true;
        }

        let processing_duration = processing_start.elapsed();
        if processing_duration > tokio::time::Duration::from_millis(1) {
            warn!("⚠️ [处理耗时] Node {} Ordering2处理耗时: {:?}, tx_id={}", node_id, processing_duration, tx_id);
        } else {
            info!("✅ [处理耗时] Node {} Ordering2处理耗时: {:?}, tx_id={}", node_id, processing_duration, tx_id);
        }

        if tx_id % 100 == 0 {
            let _ = event_tx.send(SystemEvent::PompeOrdering1Completed {
                tx_id
            });
            debug!("📡 [Pompe] Node {} 发送 Ordering1 完成事件: tx_id={}", 
                node_id, tx_id);
        }

        let response = PompeMessage::Ordering2Response {
            tx_hash,
            timestamp: median_timestamp,
            node_id,
        };

        let network_clone = Arc::clone(network);
        tokio::spawn(async move {
            if let Err(e) = network_clone.send_to_node(initiator_node_id, response).await {
                error!("❌ [Ordering2-2-LockFree] 异步发送失败: {}", e);
            }
        });
        
        let state_clone = Arc::clone(state);
        let lockfree_adapter_clone = lockfree_adapter.clone();
        let config_clone = config.clone();
        tokio::spawn(async move {
            Self::check_and_output_to_hotstuff_lockfree(node_id, &state_clone, &lockfree_adapter_clone, &config_clone).await;
        });
    }

    async fn check_and_output_to_hotstuff_lockfree(
        node_id: usize,
        state: &Arc<PompeAppState>,
        lockfree_adapter: &Option<Arc<LockFreeHotStuffAdapter>>,
        config: &PompeConfig,
    ) {
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
            let required_wait = config.stable_period_ms * 1000;
            
            if time_elapsed >= required_wait {
                *last_batch_clock = current_time_us;
                drop(last_batch_clock);
                
                let mut commit_set = state.commit_set.write().unwrap();
                
                if commit_set.is_empty() {
                    return;
                }
                
                commit_set.sort_by_key(|&(_, timestamp)| timestamp);
                
                let txs: Vec<String> = commit_set
                    .iter()
                    .map(|(tx, timestamp)| tx.to_hotstuff_format(*timestamp))
                    .collect();
                
                commit_set.clear();
                drop(commit_set);
                
                *state.consensus_ready.write().unwrap() = false;
                
                txs
            } else {
                Vec::new()
            }
        };

        let processing_duration = check_start.elapsed();
        if processing_duration > tokio::time::Duration::from_millis(2) {
            warn!("⚠️ [输出耗时] Node {} 输出检查耗时: {:?}", node_id, processing_duration);
        }
        
        if !ordered_txs.is_empty() {
            if let Some(ref adapter) = lockfree_adapter {
                adapter.push_batch(ordered_txs.clone());
                info!("⚡ [完全无锁输出] Node {} 无锁输出 {} 个交易", 
                    node_id, ordered_txs.len());
            } else {
                warn!("⚠️ [无锁输出] Node {} 无锁适配器未设置，丢失 {} 个交易", 
                    node_id, ordered_txs.len());
            }
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
            ordering1_tx: self.ordering1_tx.clone(),
            ordering1_rx: self.ordering1_rx.clone(),
            ordering2_tx: self.ordering2_tx.clone(),
            ordering2_rx: self.ordering2_rx.clone(),
            general_tx: self.general_tx.clone(),
            general_rx: self.general_rx.clone(),
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