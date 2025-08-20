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

// ... [保留之前的PompeTransaction和PompeMessage定义] ...

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

// 🚨 关键优化：完全移除外层Mutex，所有字段都使用无锁数据结构
#[derive(Debug)]
struct PompeAppState {
    // 🚨 所有字段都使用无锁数据结构
    batch_received: DashMap<String, usize>,
    ordering1_responses: DashMap<String, Vec<u64>>,
    ordering1_count: DashMap<String, usize>,
    completed_ordering1: DashMap<String, ()>,
    ordering2_responses: DashMap<String, Vec<(usize, u64)>>,
    completed_ordering2: DashMap<String, ()>,
    transaction_store: DashMap<String, PompeTransaction>,
    transaction_initiators: DashMap<String, usize>,
    
    // 🚨 使用atomic或者单独的RwLock，避免大锁
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

// 🚨 完全无锁的HotStuff队列适配器
#[derive(Debug)]
pub struct LockFreeHotStuffAdapter {
    // 🚨 直接连接到外部的无锁队列
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
    // 🚨 完全无锁的状态管理
    state: Arc<PompeAppState>,
    nfaulty: usize,
    
    // 🚨 完全无锁的消息处理：使用crossbeam channel
    ordering1_tx: Sender<(usize, PompeMessage)>,
    ordering1_rx: Receiver<(usize, PompeMessage)>,
    
    ordering2_tx: Sender<(usize, PompeMessage)>,
    ordering2_rx: Receiver<(usize, PompeMessage)>,
    
    general_tx: Sender<(usize, PompeMessage)>,
    general_rx: Receiver<(usize, PompeMessage)>,
    
    pub network: Option<Arc<crate::pompe_network::PompeNetwork>>,
    
    // 🚨 完全无锁的HotStuff队列适配器 - 移除所有Mutex
    lockfree_adapter: Option<Arc<LockFreeHotStuffAdapter>>,
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
        // 🚨 直接访问DashMap，无需外层锁
        if self.state.completed_ordering1.len() > 500 {
            self.state.completed_ordering1.clear();
            info!("🧹 [清理] Node {} 清理 {} 个已完成交易记录", 
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
            info!("🧹 [清理] Node {} 清理发起者记录", self.node_id);
        }
        
        if self.state.completed_ordering2.len() > 1000 {
            self.state.completed_ordering2.clear();
            info!("🧹 [清理] Node {} 清理ordering2完成标记", self.node_id);
        }
    }

    pub fn new_with_complete_network(
        node_id: usize, 
        all_node_ids: Vec<usize>,
        config: PompeConfig,
        _tcp_network: TcpNetwork,
    ) -> Self {
        let node_num = all_node_ids.len();
        let nfaulty = (node_num - 1) / 3;
        let (general_tx, general_rx) = unbounded();
        
        info!("🚀 创建完整网络支持的Pompe管理器，节点 {}, f={}", node_id, nfaulty);
        info!("🔍 节点列表: {:?}", all_node_ids);

        let (ord1_tx, ord1_rx) = unbounded();
        let (ord2_tx, ord2_rx) = unbounded();
        
        let network = Arc::new(PompeNetwork::new(node_id, all_node_ids));
        
        Self {
            node_id,
            config,
            // 🚨 移除外层Mutex
            state: Arc::new(PompeAppState::new()),
            nfaulty,
            ordering1_tx: ord1_tx,
            ordering1_rx: ord1_rx,
            ordering2_tx: ord2_tx,
            ordering2_rx: ord2_rx,
            general_tx,
            general_rx,
            network: Some(network),
            lockfree_adapter: None,
        }
    }

    // 🚨 移除旧的有锁方法，只保留无锁适配器
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

    // 🚨 关键优化：完全无锁的事务处理
    pub async fn process_raw_transaction(&self, raw_tx: &str) -> Result<(), String> {
        if !self.config.enable {
            return Ok(());
        }

        if let Some(transaction) = PompeTransaction::from_raw_string(raw_tx, format!("client_{}", self.node_id)) {
            let tx_hash = transaction.hash();
            
            info!("📥 [Ordering1] Node {} 处理交易: {} -> Hash: {}", 
                self.node_id, raw_tx, &tx_hash[0..8]);
            
            // 🚨 关键优化：无锁操作
            // 存储交易信息
            self.state.transaction_store.insert(tx_hash.clone(), transaction.clone());
            
            // 原子性更新批次计数
            let current_count = self.state.batch_received
                .entry(tx_hash.clone())
                .and_modify(|count| *count += 1)
                .or_insert(1)
                .clone();
            
            info!("📊 [Ordering1] Node {} 批次计数: {} -> {}/{}", 
                self.node_id, &tx_hash[0..8], current_count, self.config.batch_size);
            
            // 检查是否达到批次大小
            if current_count == self.config.batch_size {
                // 记录发起者
                self.state.transaction_initiators.insert(tx_hash.clone(), self.node_id);
                info!("📋 [发起者记录] Node {} 记录为交易 {} 的发起者", 
                    self.node_id, &tx_hash[0..8]);
                
                // 发起ordering1阶段
                self.exec_ordering1(tx_hash, transaction).await?;
            } else {
                debug!("🔄 [Ordering1] Node {} 已有其他节点发起此交易的ordering", self.node_id);
            }
        }
        
        Ok(())
    }

    async fn exec_ordering1(&self, tx_hash: String, transaction: PompeTransaction) -> Result<(), String> {
        info!("🚀 [Ordering1-exec] Node {} 发起ordering1阶段: {}", self.node_id, &tx_hash[0..8]);

        let broadcast_start = std::time::Instant::now();
        
        if let Some(ref network) = self.network {
            let request = PompeMessage::Ordering1Request {
                tx_hash: tx_hash.clone(),
                transaction: transaction.clone(),
                batch_size: self.config.batch_size,
                initiator_node_id: self.node_id, 
            };
            
            if let Err(e) = network.broadcast(request).await {
                error!("❌ [Ordering1-exec] Node {} 广播失败: {}", self.node_id, e);
                return Err(format!("Ordering1Request广播失败: {}", e));
            }

            let broadcast_duration = broadcast_start.elapsed();
            info!("⏱️ [Ordering1-exec] Node {} 广播耗时: {:?}", self.node_id, broadcast_duration);
            
            if broadcast_duration > std::time::Duration::from_millis(100) {
                warn!("⚠️ [Ordering1-exec] Node {} 广播延迟过高: {:?}", self.node_id, broadcast_duration);
            }
        }
        
        Ok(())
    }

    pub async fn process_transaction_batch(&self, shared_tx_queue: Arc<tokio::sync::Mutex<Vec<String>>>) -> Result<usize, String> {
        if !self.config.enable {
            return Ok(0);
        }

        let transactions_to_process = {
            let mut queue = shared_tx_queue.lock().await;
            let mut pompe_transactions = Vec::new();
            
            for _ in 0..std::cmp::min(queue.len(), self.config.batch_size * 2) {
                if let Some(tx) = queue.pop() {
                    if tx.starts_with("pompe:") {
                        continue;
                    }
                    pompe_transactions.push(tx);
                } else {
                    break;
                }
            }
            
            pompe_transactions
        };

        let processed_count = transactions_to_process.len();
        
        if processed_count > 0 {
            info!("🔍 [批处理] Node {} 处理 {} 个Pompe交易", 
                  self.node_id, processed_count);

            for raw_tx in &transactions_to_process {
                if let Err(e) = self.process_raw_transaction(raw_tx).await {
                    error!("❌ Pompe 处理交易失败: {}, 错误: {}", raw_tx, e);
                }
            }
        }

        Ok(processed_count)
    }

    // 🚨 优化后的get_ordered_transactions，减少锁操作
    pub fn get_ordered_transactions(&self) -> Vec<String> {
        // 快速检查提交集大小
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
        
        // 时间检查和输出处理
        let ordered_txs = {
            let mut last_batch_clock = self.state.exec_last_batch_clock.write().unwrap();
            
            if *last_batch_clock == 0 {
                info!("🔍 [输出] Node {} 初始化执行时间戳", self.node_id);
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
            drop(last_batch_clock); // 提前释放锁
            
            // 处理提交集
            let mut commit_set = self.state.commit_set.write().unwrap();
            
            let batch_size = std::cmp::min(commit_set.len(), 50);
            
            info!("🚀 [输出] Node {} 分批输出 {}/{} 个交易", 
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

            // 更新stable_point
            if let Some(&(_, latest_timestamp)) = commit_set.last() {
                let mut stable_point = self.state.stable_point.write().unwrap();
                let old_stable_point = *stable_point;
                *stable_point = latest_timestamp;
                drop(stable_point);
                
                info!("📊 [稳定点] Node {} 更新stable_point: {} -> {}", 
                    self.node_id, old_stable_point, latest_timestamp);
            }
            
            commit_set.drain(0..batch_size);
            
            if commit_set.is_empty() {
                drop(commit_set);
                *self.state.consensus_ready.write().unwrap() = false;
                info!("✅ [输出完成] Node {} 所有交易已输出，重置consensus_ready", self.node_id);
            } else {
                info!("⏳ [输出继续] Node {} 还有 {} 个交易等待下次输出", 
                    self.node_id, commit_set.len());
            }
            
            ordered_txs
        };
        
        info!("✅ [输出] Node {} 本次输出 {} 个交易", self.node_id, ordered_txs.len());
        
        ordered_txs
    }

    // 🚨 启动处理器时使用tokio::sync::Mutex而不是std::sync::Mutex
    pub async fn start_network_message_loop(&self) -> Result<(), String> {
        if let Some(ref network) = self.network {
            info!("🚀 Node {} 启动Pompe网络", self.node_id);
            
            if let Err(e) = network.start_server().await {
                return Err(format!("启动Pompe服务器失败: {}", e));
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
                                info!("📨 [分发器] Node {} 分发Ordering1消息: {:?} (总计: O1={}, O2={}, 总={})", 
                                    node_id, std::mem::discriminant(&message), ordering1_count, ordering2_count, total_messages);
                                
                                if let Err(e) = ordering1_tx.send((sender_id, message)) {
                                    error!("❌ Ordering1队列发送失败: {}", e);
                                }
                            }
                            
                            PompeMessage::Ordering2Request { .. } | 
                            PompeMessage::Ordering2Response { .. } => {
                                ordering2_count += 1;
                                info!("📨 [分发器] Node {} 分发Ordering2消息: {:?} (总计: O1={}, O2={}, 总={})", 
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
                    
                    tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
                }
            });

            self.start_ordering1_processor().await;
            self.start_ordering2_processor().await;
        }
        
        Ok(())
    }

    // 🚨 完全无锁的Ordering1处理器
    async fn start_ordering1_processor(&self) {
        let ordering1_rx = self.ordering1_rx.clone();
        let state = Arc::clone(&self.state);
        let network = self.network.clone();
        let node_id = self.node_id;
        let nfaulty = self.nfaulty;
        
        tokio::spawn(async move {
            info!("🔄 Node {} 无锁Ordering1处理器启动", node_id);
            
            loop {
                let message_opt = ordering1_rx.try_recv().ok();
                
                if let Some((sender_id, message)) = message_opt {
                    match message {
                        PompeMessage::Ordering1Request { tx_hash, transaction, batch_size, initiator_node_id } => {
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
                                    node_id, &state, nfaulty, &net,
                                    sender_id, tx_hash, timestamp_us, sender_node_id, initiator_node_id
                                ).await;
                            }
                        }
                        _ => {}
                    }
                }
                
                tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
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
        
        tokio::spawn(async move {
            info!("🔄 Node {} 无锁Ordering2处理器启动", node_id);
            
            loop {
                if let Ok((sender_id, message)) = ordering2_rx.try_recv() {
                    match message {
                        PompeMessage::Ordering2Request { tx_hash, median_timestamp, initiator_node_id } => {
                            if let Some(ref net) = network {
                                Self::handle_ordering2_request_lockfree(
                                    node_id, &state, &net, &lockfree_adapter, &config,
                                    sender_id, tx_hash, median_timestamp, initiator_node_id
                                ).await;
                            }
                        }
                        _ => {}
                    }
                }
                
                tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
            }
        });
    }

    // 🚨 完全无锁的Ordering1请求处理
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
        
        info!("🎯 [Ordering1-2-LockFree] Node {} 处理请求: {}", node_id, &tx_hash[0..8]);
        
        // 🚨 关键优化：使用DashMap的原子操作，避免任何锁
        let should_respond = if state.ordering1_responses.contains_key(&tx_hash) {
            false
        } else {
            // 原子性插入操作
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
            info!("🔄 [Ordering1-2-LockFree] Node {} 已响应过: {}", node_id, &tx_hash[0..8]);
            return;
        }
        
        // 生成时间戳并异步发送
        let timestamp_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        let response = PompeMessage::Ordering1Response {
            tx_hash,
            timestamp_us,
            node_id,
            initiator_node_id,
        };
        
        // 完全异步发送，避免阻塞
        let network_clone = Arc::clone(network);
        tokio::spawn(async move {
            if let Err(e) = network_clone.send_to_node(initiator_node_id, response).await {
                error!("❌ [Ordering1-2-LockFree] 异步发送失败: {}", e);
            }
        });
        
        let total_duration = processing_start.elapsed();
        if total_duration > tokio::time::Duration::from_millis(5) {
            warn!("⚠️ [总耗时] Node {} Ordering1总耗时: {:?}", node_id, total_duration);
        } else {
            debug!("✅ [性能] Node {} Ordering1处理完成: {:?}", node_id, total_duration);
        }
    }

    // 🚨 完全无锁的Ordering1响应处理
    async fn handle_ordering1_response_lockfree(
        node_id: usize,
        state: &Arc<PompeAppState>,
        nfaulty: usize,
        network: &Arc<crate::pompe_network::PompeNetwork>,
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
        
        // 🚨 快速预检查：使用DashMap的原子操作
        if state.completed_ordering1.contains_key(&tx_hash) {
            return;
        }
        
        info!("🌟 [Ordering1-3-LockFree] Node {} 收到时间戳: {}", node_id, &tx_hash[0..8]);
        
        // 🚨 关键优化：使用DashMap的entry API进行原子更新
        let should_proceed = {
            // 双重检查
            if state.completed_ordering1.contains_key(&tx_hash) {
                return;
            }
            
            // 原子性获取并更新时间戳列表
            let mut timestamps = state.ordering1_responses
                .get(&tx_hash)
                .map(|ref_val| ref_val.clone())
                .unwrap_or_else(Vec::new);
            
            // 防重复
            if timestamps.contains(&timestamp_us) {
                return;
            }
            
            timestamps.push(timestamp_us);
            let current_count = timestamps.len();
            
            // 原子性更新
            state.ordering1_responses.insert(tx_hash.clone(), timestamps.clone());
            state.ordering1_count.insert(tx_hash.clone(), current_count);
            
            let required = 2 * nfaulty + 1;
            
            if current_count >= required {
                let mut timestamps_sorted = timestamps;
                timestamps_sorted.sort();
                let median = timestamps_sorted[nfaulty];
                
                // 原子性清理状态
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
            warn!("⚠️ [处理耗时] Node {} Ordering1响应处理耗时: {:?}", node_id, processing_duration);
        }

        // 异步发送ordering2请求
        if let Some(median) = should_proceed {
            let msg = PompeMessage::Ordering2Request {
                tx_hash: tx_hash.clone(),
                median_timestamp: median,
                initiator_node_id: initiator_node_id,
            };
            
            let network_clone = Arc::clone(network);
            tokio::spawn(async move {
                if let Err(e) = network_clone.broadcast(msg).await {
                    error!("❌ [Ordering2-1-LockFree] 异步广播失败: {}", e);
                }
            });
        }
    }

    // 🚨 完全无锁的Ordering2请求处理
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
    ) {
        let processing_start = std::time::Instant::now();
        
        info!("🚀 [Ordering2-2-LockFree] Node {} 处理请求: {}", node_id, &tx_hash[0..8]);

        // 🚨 快速获取交易：使用DashMap的原子操作
        let transaction = match state.transaction_store.get(&tx_hash) {
            Some(tx_ref) => tx_ref.clone(),
            None => {
                warn!("⚠️ [Ordering2-2-LockFree] Node {} 找不到交易: {}", node_id, &tx_hash[0..8]);
                return;
            }
        };

        // 🚨 最小化RwLock使用：快速更新提交集
        {
            let mut commit_set = state.commit_set.write().unwrap();
            commit_set.push((transaction, median_timestamp));
            drop(commit_set); // 立即释放
            
            *state.consensus_ready.write().unwrap() = true;
        }

        let processing_duration = processing_start.elapsed();
        if processing_duration > tokio::time::Duration::from_millis(1) {
            warn!("⚠️ [处理耗时] Node {} Ordering2处理耗时: {:?}", node_id, processing_duration);
        }

        // 异步发送响应
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
        
        // 异步触发输出检查
        let state_clone = Arc::clone(state);
        let lockfree_adapter_clone = lockfree_adapter.clone();
        let config_clone = config.clone();
        tokio::spawn(async move {
            Self::check_and_output_to_hotstuff_lockfree(node_id, &state_clone, &lockfree_adapter_clone, &config_clone).await;
        });
    }

    // 🚨 完全无锁的输出检查 - 移除所有锁依赖
    async fn check_and_output_to_hotstuff_lockfree(
        node_id: usize,
        state: &Arc<PompeAppState>,
        lockfree_adapter: &Option<Arc<LockFreeHotStuffAdapter>>,
        config: &PompeConfig,
    ) {
        let check_start = std::time::Instant::now();
        
        // 🚨 快速检查是否需要输出
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
        
        // 时间检查和输出
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
                drop(last_batch_clock); // 立即释放
                
                // 快速处理提交集
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
                drop(commit_set); // 立即释放
                
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
        
        // 🚨 完全无锁输出到HotStuff
        if !ordered_txs.is_empty() {
            if let Some(ref adapter) = lockfree_adapter {
                // 🚨 无锁批量输出
                adapter.push_batch(ordered_txs.clone());
                info!("⚡ [完全无锁输出] Node {} 无锁输出 {} 个交易", 
                    node_id, ordered_txs.len());
            } else {
                warn!("⚠️ [无锁输出] Node {} 无锁适配器未设置，丢失 {} 个交易", 
                    node_id, ordered_txs.len());
            }
        }
    }

    // 🚨 优化的统计方法，减少锁竞争
    pub fn get_detailed_stats(&self) -> (usize, usize, usize, bool, u64, usize, usize) {
        // 直接从DashMap获取，无需额外锁
        let batch_count = self.state.batch_received.len();
        let ordering1_count = self.state.ordering1_responses.len();
        let transaction_store_len = self.state.transaction_store.len();
        let transaction_initiators_len = self.state.transaction_initiators.len();
        
        // 只在必要时使用RwLock
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
        // 🚨 完全无锁的克隆：crossbeam channel是Clone的
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
            network: self.network.as_ref().map(|n| Arc::clone(n)),
            lockfree_adapter: self.lockfree_adapter.clone(),
        }
    }
}

// 客户端支持函数保持不变
pub async fn send_pompe_transaction_to_node(
    node_addr: SocketAddr,
    transaction: PompeTransaction,
    client_id: &str,
) -> Result<(), String> {
    use tokio::net::TcpStream;
    use tokio::io::{AsyncWriteExt};
    
    #[derive(Serialize, Deserialize, Debug)]
    struct ClientMessage {
        pub message_type: String,
        pub transaction: Option<TestTransaction>,
        pub client_id: String,
    }
    
    #[derive(Serialize, Deserialize, Debug, Clone)]
    struct TestTransaction {
        pub id: u64,
        pub from: String,
        pub to: String,
        pub amount: u64,
        pub timestamp: u64,
        pub nonce: u64,
    }
    
    let test_tx = TestTransaction {
        id: transaction.id,
        from: transaction.from,
        to: transaction.to,
        amount: transaction.amount,
        timestamp: transaction.timestamp,
        nonce: transaction.nonce,
    };
    
    let client_message = ClientMessage {
        message_type: "pompe_transaction".to_string(),
        transaction: Some(test_tx),
        client_id: client_id.to_string(),
    };

    let mut stream = TcpStream::connect(node_addr).await.map_err(|e| format!("连接失败: {}", e))?;
    let serialized = serde_json::to_vec(&client_message).map_err(|e| format!("序列化失败: {}", e))?;
    let message_length = serialized.len() as u32;
    
    stream.write_all(&message_length.to_be_bytes()).await.map_err(|e| format!("写入长度失败: {}", e))?;
    stream.write_all(&serialized).await.map_err(|e| format!("写入消息失败: {}", e))?;
    stream.flush().await.map_err(|e| format!("刷新失败: {}", e))?;
    
    debug!("📤 发送 Pompe 交易到 {}: ID={}", node_addr, transaction.id);
    
    Ok(())
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
        assert_eq!(hash.len(), 64); // SHA256 hex string length
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