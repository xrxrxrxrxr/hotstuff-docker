// hotstuff_runner/src/pompe.rs
//! 基于C++实现的完整Pompe BFT修复版本

use std::collections::{HashMap, VecDeque, BTreeMap};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH, Duration, Instant};
use serde::{Serialize, Deserialize};
use tokio::sync::mpsc;
use hotstuff_rs::types::crypto_primitives::VerifyingKey;
use ed25519_dalek::SigningKey;
use tracing::{info, warn, error, debug};
use sha2::{Sha256, Digest};
use std::net::SocketAddr;
use crate::{pompe_network::PompeNetwork, tcp_network::TcpNetwork};

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
    pub fn from_raw_string(raw: &str, client_id: String) -> Option<Self> {
        let parts: Vec<&str> = raw.split(':').collect();
        
        debug!("🔍 [解析] 输入: '{}', 分割结果: {:?}", raw, parts);
        
        // 跳过已经处理过的pompe交易
        if parts.len() >= 6 && parts[0] == "pompe" {
            debug!("🔍 [解析] 跳过已排序的pompe交易: {}", raw);
            return None;
        }
        
        // 处理 "id:from->to:amount" 格式
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
        // 处理 "id:from:to:amount" 格式
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

    pub fn hash(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(format!("{}:{}:{}:{}", self.id, self.from, self.to, self.amount));
        format!("{:x}", hasher.finalize())
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
    },
    Ordering1Response {
        tx_hash: String,
        timestamp_us: u64,
        node_id: usize,
    },
    Ordering2Request {
        tx_hash: String,
        median_timestamp: u64,
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

// 🚨 关键：完全按照C++实现的状态结构
#[derive(Debug)]
struct PompeAppState {
    // C++: std::unordered_map<const uint256_t, int> batch_received;
    batch_received: HashMap<String, usize>,
    
    // C++: std::vector<std::pair<std::pair<uint256_t, uint64_t>, NetAddr>> commit_set;
    commit_set: Vec<(PompeTransaction, u64)>,
    
    // C++: uint64_t exec_last_batch_clock;
    exec_last_batch_clock: u64,
    
    // C++: bool consensus_ready;
    consensus_ready: bool,
    
    // 用于收集ordering1响应的临时存储
    ordering1_responses: HashMap<String, Vec<u64>>,
    ordering1_count: HashMap<String, usize>,

    ordering2_responses: HashMap<String, Vec<(usize, u64)>>, // (node_id, timestamp)   
    // 用于存储交易信息
    transaction_store: HashMap<String, PompeTransaction>,
    stable_point: u64,
}

impl PompeAppState {
    fn new() -> Self {
        Self {
            batch_received: HashMap::new(),
            commit_set: Vec::new(),
            exec_last_batch_clock: 0,
            consensus_ready: false,
            ordering1_responses: HashMap::new(),
            ordering1_count: HashMap::new(),
            ordering2_responses: HashMap::new(),
            transaction_store: HashMap::new(),
            stable_point: 0,
        }
    }
}

pub struct PompeManager {
    node_id: usize,
    config: PompeConfig,
    state: Arc<Mutex<PompeAppState>>,
    nfaulty: usize,
    
    message_tx: mpsc::UnboundedSender<(usize, PompeMessage)>,
    message_rx: Arc<Mutex<mpsc::UnboundedReceiver<(usize, PompeMessage)>>>,
    network: Option<Arc<crate::pompe_network::PompeNetwork>>,
}

impl PompeManager {
    pub fn new_with_complete_network(
        node_id: usize, 
        all_node_ids: Vec<usize>,
        config: PompeConfig,
        _tcp_network: TcpNetwork,
    ) -> Self {
        let node_num = all_node_ids.len();
        let nfaulty = (node_num - 1) / 3;
        let (tx, rx) = mpsc::unbounded_channel();
        
        info!("🚀 创建完整网络支持的Pompe管理器，节点 {}, f={}", node_id, nfaulty);
        info!("🔍 节点列表: {:?}", all_node_ids);
        
        let network = Arc::new(PompeNetwork::new(node_id, all_node_ids));
        
        Self {
            node_id,
            config,
            state: Arc::new(Mutex::new(PompeAppState::new())),
            nfaulty,
            message_tx: tx,
            message_rx: Arc::new(Mutex::new(rx)),
            network: Some(network),
        }
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

    // 🚨 关键修复：完全按照C++的client_ordering1_request_cmd_handler逻辑
    pub async fn process_raw_transaction(&self, raw_tx: &str) -> Result<(), String> {
        if !self.config.enable {
            return Ok(());
        }

        if let Some(transaction) = PompeTransaction::from_raw_string(raw_tx, format!("client_{}", self.node_id)) {
            let tx_hash = transaction.hash();
            
            info!("📥 [Ordering1] Node {} 处理交易: {} -> Hash: {}", 
                  self.node_id, raw_tx, &tx_hash[0..8]);
            
            // 🚨 C++逻辑：batch_received[cmd_hash]++
            let should_proceed = {
                let mut state = self.state.lock().unwrap();
                
                // 存储交易信息
                state.transaction_store.insert(tx_hash.clone(), transaction.clone());
                
                // 更新批次计数
                let current_count = state.batch_received.entry(tx_hash.clone()).or_insert(0);
                *current_count += 1;
                
                info!("📊 [Ordering1] Node {} 批次计数: {} -> {}/{}", 
                      self.node_id, &tx_hash[0..8], current_count, self.config.batch_size);
                
                // 🚨 C++逻辑：if (batch_received[cmd_hash] < clnt_blk_size) return;
                *current_count >= self.config.batch_size
            };
            
            if !should_proceed {
                debug!("🔄 [Ordering1] Node {} 批次未满，等待更多交易", self.node_id);
                return Ok(());
            }
            
            // 🚨 执行ordering1阶段：生成时间戳并广播
            self.exec_ordering1(tx_hash, transaction).await?;
        }
        
        Ok(())
    }

    // 🚨 新增：按照C++的exec_ordering1逻辑
    async fn exec_ordering1(&self, tx_hash: String, transaction: PompeTransaction) -> Result<(), String> {
        info!("🚀 [Ordering1] Node {} 开始ordering1阶段: {}", self.node_id, &tx_hash[0..8]);
        
        // 🚨 修复：发送Ordering1Request给所有节点（包括自己）
        if let Some(ref network) = self.network {
            let request = PompeMessage::Ordering1Request {
                tx_hash: tx_hash.clone(),
                transaction: transaction.clone(),
                batch_size: self.config.batch_size,
            };
            
            info!("📡 [Ordering1] Node {} 广播Ordering1Request: {}", 
                self.node_id, &tx_hash[0..8]);
            
            if let Err(e) = network.broadcast(request).await {
                error!("❌ [Ordering1] Node {} 广播失败: {}", self.node_id, e);
                return Err(format!("Ordering1Request广播失败: {}", e));
            }
        }
        
        Ok(())
    }

    pub async fn process_transaction_batch(&self, shared_tx_queue: Arc<Mutex<Vec<String>>>) -> Result<usize, String> {
        if !self.config.enable {
            return Ok(0);
        }

        let transactions_to_process = {
            let mut queue = shared_tx_queue.lock().unwrap();
            let mut pompe_transactions = Vec::new();
            let mut other_transactions = Vec::new();
            
            for _ in 0..std::cmp::min(queue.len(), self.config.batch_size * 2) {
                if let Some(tx) = queue.pop() {
                    if tx.starts_with("standard:") || tx.starts_with("pompe:") {
                        other_transactions.push(tx);
                    } else {
                        pompe_transactions.push(tx);
                    }
                } else {
                    break;
                }
            }
            
            for tx in other_transactions.into_iter().rev() {
                queue.insert(0, tx);
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

    // 🚨 关键修复：按照C++的时间窗口逻辑获取排序交易
    pub fn get_ordered_transactions(&self) -> Vec<String> {
        let mut state = self.state.lock().unwrap();
        
        if state.commit_set.is_empty() {
            return Vec::new();
        }
        
        // 🚨 C++逻辑：only a single leader starts the consensus phase
        if self.node_id != self.config.leader_node_id {
            debug!("🔍 [输出] Node {} 非领导者，跳过输出 (领导者: {})", 
                   self.node_id, self.config.leader_node_id);
            return Vec::new();
        }
        
        // 🚨 C++逻辑：检查是否consensus_ready
        if !state.consensus_ready {
            debug!("🔍 [输出] Node {} consensus未就绪", self.node_id);
            return Vec::new();
        }
        
        // 🚨 C++逻辑：时间窗口检查
        let current_time_us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        
        if state.exec_last_batch_clock == 0 {
            info!("🔍 [输出] Node {} 初始化执行时间戳", self.node_id);
            state.exec_last_batch_clock = current_time_us;
            return Vec::new();
        }
        
        let time_elapsed = current_time_us - state.exec_last_batch_clock;
        let required_wait = self.config.stable_period_ms * 1000; // 转换为微秒
        
        // 🚨 C++逻辑：if (exec_last_batch_clock + stable_period * 1000 < curr_clock_us)
        if time_elapsed < required_wait {
            debug!("🔍 [输出] Node {} 还需等待 {}μs", 
                   self.node_id, required_wait - time_elapsed);
            return Vec::new();
        }
        
        info!("🚀 [输出] Node {} 时间窗口到达，处理 {} 个交易", 
              self.node_id, state.commit_set.len());
        
        // 🚨 C++逻辑：按时间戳排序commit_set
        state.commit_set.sort_by_key(|&(_, timestamp)| timestamp);

        // 批量输出，不要一个一个输出 Fix P
        let batch_size = std::cmp::min(state.commit_set.len(), 50); // 一次最多50个
        
        if batch_size < 10 && time_elapsed < required_wait {
            return Vec::new(); // 积累更多交易再输出
        }
        
        let ordered_txs: Vec<String> = state.commit_set
            .iter()
            .take(batch_size)
            .map(|(tx, timestamp)| {
                let formatted = tx.to_hotstuff_format(*timestamp);
                info!("📤 [输出] 排序交易: {} -> {}", tx.id, formatted);
                formatted
            })
            .collect();
        
        if !ordered_txs.is_empty() {
            // 🚨 更新stable_point为最新处理的时间戳
            if let Some(&(_, latest_timestamp)) = state.commit_set.last() {
                state.stable_point = latest_timestamp;
                info!("📊 [稳定点] Node {} 更新stable_point: {}", 
                    self.node_id, latest_timestamp);
            }
        }
        // 🚨 C++逻辑：清空commit_set并更新时间戳
        // state.commit_set.clear();
        // 只移除已输出的交易 Fix P
        state.commit_set.drain(0..batch_size);  
        state.exec_last_batch_clock = current_time_us;
        state.consensus_ready = false; // 重置状态
        
        info!("✅ [输出] Node {} 输出 {} 个排序交易", 
              self.node_id, ordered_txs.len());
        
        ordered_txs
    }

    pub async fn start_network_message_loop(&self) -> Result<(), String> {
        if let Some(ref network) = self.network {
            info!("🚀 Node {} 启动Pompe网络", self.node_id);
            
            if let Err(e) = network.start_server().await {
                return Err(format!("启动Pompe服务器失败: {}", e));
            }
            
            let network_clone = Arc::clone(network);
            let node_id = self.node_id;
            let state = Arc::clone(&self.state);
            let nfaulty = self.nfaulty;
            
            tokio::spawn(async move {
                info!("🌐 Node {} Pompe消息接收循环启动", node_id);
                
                loop {
                    if let Some((sender_id, message)) = network_clone.recv().await {
                        debug!("📬 [消息接收] Node {} 收到来自节点 {} 的消息", node_id, sender_id);
                        
                        match message {
                            PompeMessage::Ordering1Request { tx_hash, transaction, batch_size } => {
                                info!("🎯 [网络请求] Node {} 处理来自节点 {} 的Ordering1请求", 
                                    node_id, sender_id);
                                
                                Self::handle_network_ordering1_request(
                                    node_id, &state, &network_clone,
                                    sender_id, tx_hash, transaction, batch_size
                                ).await;
                            }
                            PompeMessage::Ordering1Response { tx_hash, timestamp_us, node_id: sender_node_id } => {
                                Self::handle_network_ordering1_response(
                                    node_id, &state, nfaulty, &network_clone,
                                    sender_id, tx_hash, timestamp_us, sender_node_id
                                ).await;
                            }
                            PompeMessage::Ordering2Request { tx_hash, median_timestamp } => {
                                Self::handle_network_ordering2_request(
                                    node_id, &state, &network_clone,
                                    sender_id, tx_hash, median_timestamp
                                ).await;
                            }
                            PompeMessage::Ordering2Response { tx_hash, timestamp, node_id: sender_node_id } => {
                                info!("✅ [网络响应] Node {} 处理来自节点 {} 的Ordering2响应", 
                                    node_id, sender_node_id);
                                
                                Self::handle_network_ordering2_response(
                                    node_id, &state, nfaulty,
                                    sender_id, tx_hash, timestamp, sender_node_id
                                ).await;
                            }
                            _ => {
                                debug!("🔍 Node {} 收到其他类型Pompe消息", node_id);
                            }
                        }
                    }
                    
                    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                }
            });
        }
        
        Ok(())
    }

    // 3. 新增：处理Ordering1Request的函数
    async fn handle_network_ordering1_request(
        node_id: usize,
        state: &Arc<Mutex<PompeAppState>>,
        network: &Arc<crate::pompe_network::PompeNetwork>,
        _sender_id: usize,
        tx_hash: String,
        transaction: PompeTransaction,
        _batch_size: usize
    ) {
        info!("🎯 [Ordering1请求] Node {} 处理Ordering1请求: {}", 
            node_id, &tx_hash[0..8]);
        
        // 存储交易信息
        {
            let mut state_guard = state.lock().unwrap();
            state_guard.transaction_store.insert(tx_hash.clone(), transaction);
        }
        
        // 生成时间戳并回复
        let timestamp_us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        
        let response = PompeMessage::Ordering1Response {
            tx_hash,
            timestamp_us,
            node_id,
        };
        
        info!("📤 [Ordering1响应] Node {} 回复时间戳: {}", node_id, timestamp_us);
        
        if let Err(e) = network.broadcast(response).await {
            error!("❌ [Ordering1响应] Node {} 回复失败: {}", node_id, e);
        }
    }

    // 🚨 关键修复：按照C++逻辑处理ordering1响应
    async fn handle_network_ordering1_response(
        node_id: usize,
        state: &Arc<Mutex<PompeAppState>>,
        nfaulty: usize,
        network: &Arc<crate::pompe_network::PompeNetwork>,
        _sender_id: usize,
        tx_hash: String,
        timestamp_us: u64,
        sender_node_id: usize
    ) {
        info!("🌟 [Ordering1响应] Node {} 收到来自节点 {} 的时间戳: {} -> {}", 
              node_id, sender_node_id, &tx_hash[0..8], timestamp_us);
        
        let should_proceed = {
            let mut state_guard = state.lock().unwrap();
            
            // 收集时间戳
            state_guard.ordering1_responses.entry(tx_hash.clone()).or_insert_with(Vec::new).push(timestamp_us);
            
            let count = state_guard.ordering1_count.entry(tx_hash.clone()).or_insert(0);
            *count += 1;
            
            let required = 2 * nfaulty + 1;
            
            info!("📊 [Ordering1响应] Node {} 收集进度: {}/{} 交易: {}", 
                  node_id, count, required, &tx_hash[0..8]);
            
            if *count >= required {
                info!("🎉 [Ordering1响应] Node {} 收集完成，计算中位数", node_id);
                
                if let Some(timestamps) = state_guard.ordering1_responses.get(&tx_hash) {
                    let mut timestamps_sorted = timestamps.clone();
                    timestamps_sorted.sort();
                    
                    if timestamps_sorted.len() >= required {
                        let median = timestamps_sorted[nfaulty];
                        
                        info!("🎯 [Ordering1响应] Node {} 中位数时间戳: {} 交易: {}", 
                              node_id, median, &tx_hash[0..8]);
                        
                        // 清理状态
                        state_guard.ordering1_responses.remove(&tx_hash);
                        state_guard.ordering1_count.remove(&tx_hash);
                        
                        Some(median)
                    } else {
                        error!("❌ [Ordering1响应] Node {} 时间戳数量不足", node_id);
                        None
                    }
                } else {
                    error!("❌ [Ordering1响应] Node {} 找不到时间戳数据", node_id);
                    None
                }
            } else {
                None
            }
        };

        // 🚨 发送ordering2请求
        if let Some(median) = should_proceed {
            let msg = PompeMessage::Ordering2Request {
                tx_hash: tx_hash.clone(),
                median_timestamp: median,
            };
            
            info!("🚀 [Ordering2请求] Node {} 广播Ordering2请求，交易: {}，中位数: {}", 
                  node_id, &tx_hash[0..8], median);
            
            if let Err(e) = network.broadcast(msg).await {
                error!("❌ [Ordering2请求] Node {} 广播失败: {}", node_id, e);
            }
        }
    }

    async fn handle_network_ordering2_request(
        node_id: usize,
        state: &Arc<Mutex<PompeAppState>>,
        network: &Arc<crate::pompe_network::PompeNetwork>,
        _sender_id: usize,
        tx_hash: String,
        median_timestamp: u64
    ) {
        info!("🚀 [Ordering2请求] Node {} 处理Ordering2请求: {}, 时间戳: {}", 
            node_id, &tx_hash[0..8], median_timestamp);

        // 🚨 C++逻辑：检查timestamp是否大于stable_point
        let should_accept = {
            let mut state_guard = state.lock().unwrap();
            
            // C++: if (timestamp < stable_point) { assert(false); }
            if median_timestamp < state_guard.stable_point {
                error!("❌ [时间戳验证] Node {} 网络异常检测 - 时间戳({}) < 稳定点({})", 
                    node_id, median_timestamp, state_guard.stable_point);
                error!("❌ [时间戳验证] 请考虑增加stable-point配置参数");
                false
            } else {
                info!("✅ [时间戳验证] Node {} 接受: timestamp({}) >= stable_point({})", 
                    node_id, median_timestamp, state_guard.stable_point);
                
                // C++: commit_set.push_back(...)
                if let Some(transaction) = state_guard.transaction_store.get(&tx_hash) {
                    let commit_entry = (transaction.clone(), median_timestamp);
                    state_guard.commit_set.push(commit_entry);
                    state_guard.consensus_ready = true;
                    
                    info!("📝 [Ordering2请求] Node {} 添加到提交集，大小: {}", 
                        node_id, state_guard.commit_set.len());
                    true
                } else {
                    warn!("⚠️ [Ordering2请求] Node {} 找不到交易: {}", node_id, &tx_hash[0..8]);
                    false
                }
            }
        };

        // C++: exec_ordering2(...) - 总是发送响应
        if should_accept {
            let response = PompeMessage::Ordering2Response {
                tx_hash,
                timestamp: median_timestamp,
                node_id,
            };

            info!("📡 [Ordering2响应] Node {} 发送响应", node_id);
            
            if let Err(e) = network.broadcast(response).await {
                error!("❌ [Ordering2响应] Node {} 广播失败: {}", node_id, e);
            }
        }
        // 注意：C++中即使时间戳检查失败也会crash，不会拒绝发送响应
    }

    // 2. 新增：处理Ordering2Response的函数
    async fn handle_network_ordering2_response(
        node_id: usize,
        state: &Arc<Mutex<PompeAppState>>,
        nfaulty: usize,
        _sender_id: usize,
        tx_hash: String,
        timestamp: u64,
        sender_node_id: usize
    ) {
        info!("✅ [Ordering2响应] Node {} 收到来自节点 {} 的确认: {} -> {}", 
            node_id, sender_node_id, &tx_hash[0..8], timestamp);
        
        let should_trigger_consensus = {
            let mut state_guard = state.lock().unwrap();
            
            // 收集ordering2响应
            let responses = state_guard.ordering2_responses.entry(tx_hash.clone()).or_insert_with(Vec::new);
            responses.push((sender_node_id, timestamp));
            
            let required = 2 * nfaulty + 1;
            
            info!("📊 [Ordering2响应] Node {} 收集进度: {}/{} 交易: {}", 
                node_id, responses.len(), required, &tx_hash[0..8]);
            
            if responses.len() >= required {
                info!("🎉 [Ordering2响应] Node {} 收集完成，可以触发共识", node_id);
                
                // 清理状态
                state_guard.ordering2_responses.remove(&tx_hash);
                true
            } else {
                false
            }
        };
        
        if should_trigger_consensus {
            // 🚨 这里可以触发共识阶段或者设置consensus_ready
            info!("🚀 [共识触发] Node {} 交易 {} 准备进入共识", node_id, &tx_hash[0..8]);
        }
    }

    pub fn get_detailed_stats(&self) -> (usize, usize, usize, bool, u64) {
        let state = self.state.lock().unwrap();
        (
            state.batch_received.len(),
            state.ordering1_responses.len(),
            state.commit_set.len(),
            state.consensus_ready,
            state.exec_last_batch_clock,
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
            message_tx: self.message_tx.clone(),
            message_rx: Arc::clone(&self.message_rx),
            network: self.network.as_ref().map(|n| Arc::clone(n)),
        }
    }
}

// 客户端支持函数
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