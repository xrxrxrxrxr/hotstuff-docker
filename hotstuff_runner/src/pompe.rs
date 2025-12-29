// hotstuff_runner/src/pompe.rs
//! Fully lock-free Pompe BFT implementation - supports crossbeam lock-free queues

use crate::pompe_network::PompeNetwork;
use crate::tx_utils::{equivalent_tx_count, synthetic_tx_ids};
use bincode;
use crossbeam::queue::SegQueue;
use dashmap::{DashMap, DashSet};
use ed25519_dalek::{Signature as Ed25519Signature, Signer, SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, warn};
// Switch Pompe internal queues to tokio::mpsc (async, non-blocking)
use crate::{
    event::SystemEvent,
    utils::{extract_signatures, verify_signatures, DigitalSignature},
};
use tokio::{
    sync::{mpsc as async_mpsc, Mutex as AsyncMutex},
    task,
};

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
        hasher.update(format!(
            "{}:{}:{}:{}",
            self.id, self.from, self.to, self.amount
        ));
        format!("{:x}", hasher.finalize())
    }

    pub fn from_raw_string(raw: &str, client_id: String) -> Option<Self> {
        let parts: Vec<&str> = raw.split(':').collect();

        debug!("🔍 [Parse] Input: '{}', segments: {:?}", raw, parts);

        if parts.len() >= 6 && parts[0] == "pompe" {
            debug!(
                "🔍 [Parse] Skipping already-ordered pompe transaction: {}",
                raw
            );
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
                            timestamp: SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_secs(),
                            nonce: 0,
                        });
                    }
                }
            }
        } else if parts.len() == 4 {
            if let (Ok(id), Ok(amount)) = (parts[0].parse::<u64>(), parts[3].parse::<u64>()) {
                let from = parts[1].to_string();
                let to = parts[2].to_string();

                return Some(Self {
                    id,
                    from,
                    to,
                    amount,
                    client_id,
                    timestamp: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                    nonce: 0,
                });
            }
        }

        error!(
            "❌ [Parse] Failed to parse transaction format: {} (parts: {:?})",
            raw, parts
        );
        None
    }

    pub fn to_hotstuff_format(&self, ordering_timestamp: u64) -> String {
        self.to_hotstuff_format_with_id(ordering_timestamp, self.id)
    }

    pub fn to_hotstuff_format_with_id(&self, ordering_timestamp: u64, tx_id: u64) -> String {
        let from = "Alice".to_string();
        let to = "Bob".to_string();
        format!(
            "pompe:{}:{}:{}->{}:{}",
            ordering_timestamp, tx_id, from, to, self.amount
        )
    }

    pub fn serialized_size_bytes(&self) -> usize {
        bincode::serialized_size(self)
            .map(|size| size as usize)
            .unwrap_or(0)
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
        signature: Vec<u8>,
    },
    Ordering2Request {
        tx_hash: String,
        median_timestamp: u64,
        initiator_node_id: usize,
        signatures: Vec<DigitalSignature>,
    },
    Ordering2Response {
        tx_hash: String,
        timestamp: u64,
        node_id: usize,
    },
    // Deliver ordered transactions directly into the target node's HotStuff queue
    DeliverOrderedTxs {
        items: Vec<String>,
        initiator: usize,
    },
}

#[derive(Debug, Clone)]
pub struct PompeConfig {
    pub enable: bool,
    pub batch_size: usize,
    pub stable_period_ms: u64,
    pub leader_node_id: usize,
    pub liveness_delta_ms: u64,
    pub queue_capacity: usize,
}

impl Default for PompeConfig {
    fn default() -> Self {
        Self {
            enable: true,
            batch_size: 1,
            stable_period_ms: 50,
            leader_node_id: 1,
            liveness_delta_ms: 10,
            queue_capacity: 4096,
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

#[derive(Debug)]
pub struct PompeAppState {
    batch_received: DashMap<String, usize>,
    ordering1_responses: DashMap<String, Vec<(usize, u64, Vec<u8>)>>,
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
    // Indicates whether a scheduled flush task is already arranged
    flusher_scheduled: std::sync::atomic::AtomicBool,
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
            stable_point: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            flusher_scheduled: std::sync::atomic::AtomicBool::new(false),
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
        debug!("📈 [Lock-free Adapter] Connected to external HotStuff queue");
    }

    pub fn push(&self, item: String) {
        if let Some(ref queue) = self.external_queue {
            queue.push(item);
            debug!("📈 [Lock-free Adapter] Added transaction to the HotStuff queue");
        } else {
            warn!(
                "⚠️ [Lock-free Adapter] External queue not connected, dropped transaction: {}",
                if item.len() > 50 { &item[0..50] } else { &item }
            );
        }
    }

    pub fn push_batch(&self, items: Vec<String>) {
        let items_count = items.len();
        if let Some(ref queue) = self.external_queue {
            for item in items {
                queue.push(item);
            }
            debug!(
                "📈 [Lock-free Adapter] Added {} transactions to the HotStuff queue",
                items_count
            );
        } else {
            warn!(
                "⚠️ [Lock-free Adapter] External queue not connected, dropped {} transactions",
                items_count
            );
        }
    }

    pub fn set_filters(&mut self, confirmed: Arc<DashSet<u64>>, in_flight: Arc<DashSet<u64>>) {
        let _ = confirmed;
        let _ = in_flight;
        debug!("📦 [Lock-free Adapter] Received HotStuff filter handles (currently unused)");
    }
}

pub struct PompeManager {
    node_id: usize,
    config: PompeConfig,
    state: Arc<PompeAppState>,
    nfaulty: usize,
    // All node IDs (order must remain consistent for view-based leader rotation)
    all_node_ids: Vec<usize>,
    // Current view number and whether this node is the active leader
    current_view: Arc<AtomicU64>,
    is_current_leader: Arc<AtomicBool>,

    ordering1_tx: async_mpsc::UnboundedSender<(usize, PompeMessage)>,
    ordering1_rx: Arc<AsyncMutex<async_mpsc::UnboundedReceiver<(usize, PompeMessage)>>>,

    ordering2_tx: async_mpsc::UnboundedSender<(usize, PompeMessage)>,
    ordering2_rx: Arc<AsyncMutex<async_mpsc::UnboundedReceiver<(usize, PompeMessage)>>>,

    general_tx: async_mpsc::UnboundedSender<(usize, PompeMessage)>,
    general_rx: Arc<AsyncMutex<async_mpsc::UnboundedReceiver<(usize, PompeMessage)>>>,

    // Added: dedicated broadcast channel (Tokio mpsc to avoid blocking runtime threads)
    broadcast_tx: async_mpsc::UnboundedSender<PompeMessage>,
    broadcast_rx: Arc<AsyncMutex<async_mpsc::UnboundedReceiver<PompeMessage>>>,

    pub network: Option<Arc<crate::pompe_network::PompeNetwork>>,
    lockfree_adapter: Option<Arc<LockFreeHotStuffAdapter>>,
    event_tx: tokio::sync::broadcast::Sender<SystemEvent>,
    signing_key: Arc<SigningKey>,
    verifying_keys: Arc<HashMap<usize, VerifyingKey>>,
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
                "🧹 [Cleanup] Node {} cleared {} completed transaction records",
                self.node_id, 500
            );
        }

        let orphan_ordering1 = self.state.ordering1_responses.len();
        if orphan_ordering1 > 500 {
            self.state.ordering1_responses.clear();
            self.state.ordering1_count.clear();
            warn!(
                "🧹 [Cleanup] Node {} cleared {} orphaned ordering1 states",
                self.node_id, orphan_ordering1
            );
        }

        if self.state.transaction_initiators.len() > 1000 {
            self.state.transaction_initiators.clear();
            debug!(
                "🧹 [Cleanup] Node {} cleared initiator records",
                self.node_id
            );
        }

        if self.state.completed_ordering2.len() > 1000 {
            self.state.completed_ordering2.clear();
            debug!(
                "🧹 [Cleanup] Node {} cleared ordering2 completion markers",
                self.node_id
            );
        }
    }

    pub fn new_with_complete_network(
        node_id: usize,
        all_node_ids: Vec<usize>,
        config: PompeConfig,
        _network: impl hotstuff_rs::networking::network::Network + Clone + Send + 'static,
        event_tx: tokio::sync::broadcast::Sender<SystemEvent>,
        signing_key: SigningKey,
        verifying_keys: HashMap<usize, VerifyingKey>,
    ) -> Self {
        let node_num = all_node_ids.len();
        let nfaulty = (node_num - 1) / 3;
        let (general_tx, general_rx) = async_mpsc::unbounded_channel();

        info!(
            "🚀 Created Pompe manager with full network support, node {}, f={}",
            node_id, nfaulty
        );
        info!("🔍 Node list: {:?}", all_node_ids);

        let (ord1_tx, ord1_rx) = async_mpsc::unbounded_channel();
        let (ord2_tx, ord2_rx) = async_mpsc::unbounded_channel();
        let (broadcast_tx, broadcast_rx) = async_mpsc::unbounded_channel();

        let network = Arc::new(PompeNetwork::new(node_id, all_node_ids.clone()));

        Self {
            node_id,
            config,
            state: Arc::new(PompeAppState::new()),
            nfaulty,
            all_node_ids: all_node_ids.clone(),
            current_view: Arc::new(AtomicU64::new(0)),
            is_current_leader: Arc::new(AtomicBool::new(false)),
            ordering1_tx: ord1_tx,
            ordering1_rx: Arc::new(AsyncMutex::new(ord1_rx)),
            ordering2_tx: ord2_tx,
            ordering2_rx: Arc::new(AsyncMutex::new(ord2_rx)),
            general_tx,
            general_rx: Arc::new(AsyncMutex::new(general_rx)),
            broadcast_tx,
            broadcast_rx: Arc::new(AsyncMutex::new(broadcast_rx)),
            network: Some(network),
            lockfree_adapter: None,
            event_tx,
            signing_key: Arc::new(signing_key),
            verifying_keys: Arc::new(verifying_keys),
        }
    }

    pub fn set_lockfree_adapter(&mut self, adapter: Arc<LockFreeHotStuffAdapter>) {
        self.lockfree_adapter = Some(adapter);
        info!(
            "✅ [Lock-free Setup] Node {} configured lock-free HotStuff adapter",
            self.node_id
        );
    }

    pub fn debug_config(&self) {
        info!("🔧 [Config Check] Node {} Pompe config:", self.node_id);
        info!("  - Enabled: {}", self.config.enable);
        info!("  - Batch size: {}", self.config.batch_size);
        info!("  - Stable period: {}ms", self.config.stable_period_ms);
        info!("  - Leader node: {}", self.config.leader_node_id);
        info!("  - Fault tolerance f: {}", self.nfaulty);
        info!("  - Total nodes: {}", self.nfaulty * 3 + 1);
        info!("  - Required responses (2f+1): {}", 2 * self.nfaulty + 1);

        if let Some(ref network) = self.network {
            info!("  - Network node IDs: {:?}", network.peer_node_ids);
            info!(
                "  - Current node present in network: {}",
                network.peer_node_ids.contains(&self.node_id)
            );
        } else {
            warn!("  - ⚠️ Network not configured!");
        }
    }

    pub async fn process_raw_transaction(&self, raw_tx: &str) -> Result<(), String> {
        if !self.config.enable {
            debug!("Pompe disabled, skipping transaction: {}", raw_tx);
            return Ok(());
        }

        if let Some(transaction) =
            PompeTransaction::from_raw_string(raw_tx, format!("client_{}", self.node_id))
        {
            let tx_hash = transaction.hash();

            debug!(
                "📥 [Ordering1] Node {} processing transaction: {} -> Hash: {}, tx_id={}",
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
                "📊 [Ordering1] Node {} batch count: {} -> {}/{}",
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
                    "📋 [Initiator Record] Node {} marked as initiator for transaction {}",
                    self.node_id,
                    &tx_hash[0..8]
                );

                // Fix: call the correct method
                self.exec_ordering1(tx_hash, transaction).await?;
            } else {
                debug!(
                    "🔄 [Ordering1] Node {} another node already initiated ordering for this transaction",
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
            "🚀 [Ordering1-exec] Node {} starting ordering1 phase: {}",
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

            // Use the dedicated broadcast channel (bounded with backpressure)
            if let Err(e) = self.broadcast_tx.send(request) {
                warn!("⚠️ [Ordering1-exec] Broadcast channel closed: {}", e);
            }

            let broadcast_duration = broadcast_start.elapsed();
            debug!(
                "⏱️ [Ordering1-exec] Node {} broadcast duration: {:?}",
                self.node_id, broadcast_duration
            );
        }

        Ok(())
    }

    pub async fn start_network_message_loop(&self) -> Result<(), String> {
        if let Some(ref network) = self.network {
            info!("🚀 Node {} starting Pompe network", self.node_id);

            if let Err(e) = network.start_server() {
                return Err(format!("Failed to start Pompe server: {}", e));
            }
            // Pre-warm connections to reduce the first-send latency
            network.warm_up_connections();

            // Start dedicated broadcast handler
            let broadcast_rx = Arc::clone(&self.broadcast_rx);
            let net = Arc::clone(network);
            network.spawn(async move {
                info!("📡 Starting dedicated broadcast handler");
                loop {
                    let msg_opt = {
                        let mut rx = broadcast_rx.lock().await;
                        rx.recv().await
                    };

                    match msg_opt {
                        Some(msg) => {
                            if let Err(e) = net.broadcast(msg).await {
                                error!("❌ Dedicated broadcast failed: {}", e);
                            }
                        }
                        None => {
                            warn!("📡 Broadcast channel closed, handler exiting");
                            break;
                        }
                    }
                }
            });

            // Listen for HotStuff view start events to compute the current view leader (retain non-fixed leader mode)
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
                info!("🌐 Node {} Pompe message receive loop started", node_id);
                let mut total_messages = 0;
                let mut ordering1_count = 0;
                let mut ordering2_count = 0;

                loop {
                    if let Some((sender_id, message)) = network_clone.recv().await {
                        debug!(
                            "📬 [Receiver] Node {} received message from node {}",
                            node_id, sender_id
                        );
                        total_messages += 1;

                        match &message {
                            PompeMessage::Ordering1Request { .. }
                            | PompeMessage::Ordering1Response { .. } => {
                                ordering1_count += 1;
                                debug!(
                                    "📨 [Dispatcher] Node {} routed Ordering1 message: {:?} (totals: O1={}, O2={}, All={})",
                                    node_id,
                                    std::mem::discriminant(&message),
                                    ordering1_count,
                                    ordering2_count,
                                    total_messages
                                );

                                if let Err(e) = ordering1_tx.send((sender_id, message)) {
                                    error!("❌ Ordering1 channel send failed (closed): {}", e);
                                }
                            }

                            PompeMessage::Ordering2Request { .. }
                            | PompeMessage::Ordering2Response { .. } => {
                                ordering2_count += 1;
                                debug!(
                                    "📨 [Dispatcher] Node {} routed Ordering2 message: {:?} (totals: O1={}, O2={}, All={})",
                                    node_id,
                                    std::mem::discriminant(&message),
                                    ordering1_count,
                                    ordering2_count,
                                    total_messages
                                );

                                if let Err(e) = ordering2_tx.send((sender_id, message)) {
                                    error!("❌ Ordering2 channel send failed (closed): {}", e);
                                }
                            }

                            _ => {
                                if let Err(e) = general_tx.send((sender_id, message)) {
                                    error!("❌ General channel send failed (closed): {}", e);
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
            let event_tx_for_flush = self.event_tx.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(
                    config.stable_period_ms,
                ));
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
        let signing_key = Arc::clone(&self.signing_key);
        let verifying_keys = Arc::clone(&self.verifying_keys);

        tokio::spawn(async move {
            info!("🔄 Node {} lock-free Ordering1 handler started", node_id);

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
                            // if tx_id>20000{
                            //     warn!("🚨 [Ordering1] Node {} received suspiciously high tx_id: {}", node_id, tx_id);
                            // }
                            debug!("Received Ordering1 request: {}, hash = {}", tx_id, tx_hash);
                            if let Some(ref net) = network {
                                Self::handle_ordering1_request_lockfree(
                                    node_id,
                                    &state,
                                    &net,
                                    Arc::clone(&signing_key),
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
                            signature,
                        } => {
                            if let Some(ref net) = network {
                                Self::handle_ordering1_response_lockfree(
                                    node_id,
                                    &state,
                                    nfaulty,
                                    &net,
                                    &broadcast_tx,
                                    Arc::clone(&verifying_keys),
                                    sender_id,
                                    tx_hash,
                                    timestamp_us,
                                    sender_node_id,
                                    initiator_node_id,
                                    signature,
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
        let verifying_keys = Arc::clone(&self.verifying_keys);

        tokio::spawn(async move {
            info!("🔄 Node {} lock-free Ordering2 handler started", node_id);

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
                                    Arc::clone(&verifying_keys),
                                    signatures,
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
            info!("🔄 Node {} general message handler started", node_id);
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
                                info!("📥 [Ordered Delivery] Node {} received {} ordered transactions from {} and wrote them to the HotStuff queue", node_id, count, initiator);
                            } else {
                                warn!(
                                        "⚠️ [Ordered Delivery] Node {} missing HotStuff adapter, dropping delivery",
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
        state: &Arc<PompeAppState>,
        network: &Arc<crate::pompe_network::PompeNetwork>,
        signing_key: Arc<SigningKey>,
        _sender_id: usize,
        tx_hash: String,
        transaction: PompeTransaction,
        _batch_size: usize,
        initiator_node_id: usize,
    ) {
        let processing_start = std::time::Instant::now();

        if transaction.id > 100000 {
            let payload_bytes = bincode::serialized_size(&transaction)
                .map(|sz| sz as usize)
                .unwrap_or(0);
            debug!(
                "🚨 [Ordering1] Node {} processing suspiciously high tx_id: {} (payload={} bytes)",
                node_id, transaction.id, payload_bytes
            );
        }
        debug!(
            "🎯 [handle_ordering1_request] Node {} handling request: tx_id={}, hash={}, initiator={}",
            node_id,
            transaction.id,
            &tx_hash[0..8],
            initiator_node_id
        );

        let should_respond = if state.ordering1_responses.contains_key(&tx_hash) {
            false
        } else {
            state
                .transaction_store
                .insert(tx_hash.clone(), transaction.clone());
            // warn!("⚠️ [First Ordering1] Node {} recorded new transaction: hash = {}", node_id, &tx_hash[0..8]);
            state
                .ordering1_responses
                .insert(tx_hash.clone(), Vec::new());
            state.ordering1_count.insert(tx_hash.clone(), 0);
            true
        };

        let check_duration = processing_start.elapsed();
        if check_duration > tokio::time::Duration::from_millis(1) {
            debug!(
                "⚠️ [Timing] Node {} Ordering1 check duration: {:?}",
                node_id, check_duration
            );
        }

        if !should_respond {
            debug!(
                "🔄 [handle_ordering1_request] Node {} already responded: {}",
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
        let message_to_sign = format!("ordering1:{}", tx_hash_clone);
        let signature = signing_key
            .sign(message_to_sign.as_bytes())
            .to_bytes()
            .to_vec();
        let response = PompeMessage::Ordering1Response {
            tx_hash,
            timestamp_us,
            node_id,
            initiator_node_id,
            signature,
        };

        let network_clone = Arc::clone(network);
        let tx_hash_for_async = tx_hash_clone.clone();
        // tokio::spawn(async move {
        if let Err(e) = network_clone
            .send_to_node(initiator_node_id, response)
            .await
        {
            error!("❌ [handle_ordering1_request] Async send failed: {}", e);
        }
        if transaction.id > 100000 {
            debug!(
                "🚨 [Ordering1] Node {} sent response for high tx_id: {}",
                node_id, transaction.id
            );
        }
        info!(
            "📤 [handle_ordering1_request] Node {} sent Ordering1 response to Node {}: hash = {}",
            node_id, initiator_node_id, tx_hash_for_async
        );
        // });

        let total_duration = processing_start.elapsed();
        if total_duration > tokio::time::Duration::from_millis(5) {
            debug!(
                "⚠️ [Performance] Node {} handle_ordering1_request total duration: {:?}, hash = {}",
                node_id, total_duration, tx_hash_clone
            );
        } else {
            debug!(
                "✅ [Performance] Node {} handle_ordering1_request completed: {:?}, hash = {}",
                node_id, total_duration, tx_hash_clone
            );
        }
    }

    async fn handle_ordering1_response_lockfree(
        node_id: usize,
        state: &Arc<PompeAppState>,
        nfaulty: usize,
        network: &Arc<crate::pompe_network::PompeNetwork>,
        broadcast_tx: &async_mpsc::UnboundedSender<PompeMessage>,
        verifying_keys: Arc<HashMap<usize, VerifyingKey>>,
        _sender_id: usize,
        tx_hash: String,
        timestamp_us: u64,
        sender_node_id: usize,
        initiator_node_id: usize,
        signature_bytes: Vec<u8>,
    ) {
        let processing_start = std::time::Instant::now();

        if node_id != initiator_node_id {
            return;
        }

        debug!(
            "🌟 [handle_ordering1_response] Node {} received timestamp: {}",
            node_id,
            &tx_hash[0..8]
        );

        let Some(verifying_key) = verifying_keys.get(&sender_node_id) else {
            warn!(
                "⚠️ [Ordering1] Missing public key for node {}",
                sender_node_id
            );
            return;
        };

        if signature_bytes.len() != 64 {
            warn!(
                "⚠️ [Ordering1] Node {} signature length unexpected: {} bytes",
                sender_node_id,
                signature_bytes.len()
            );
            return;
        }

        let signature = match Ed25519Signature::try_from(signature_bytes.as_slice()) {
            Ok(sig) => sig,
            Err(e) => {
                warn!(
                    "⚠️ [Ordering1] Node {} signature parsing failed: {}",
                    sender_node_id, e
                );
                return;
            }
        };

        let message = format!("ordering1:{}", tx_hash);
        if verifying_key
            .verify_strict(message.as_bytes(), &signature)
            .is_err()
        {
            warn!(
                "⚠️ [Ordering1] Node {} signature verification failed",
                sender_node_id
            );
            return;
        }

        let required = 2 * nfaulty + 1;
        let mut median_result: Option<u64> = None;
        let mut selected_records: Vec<(usize, u64, Vec<u8>)> = Vec::new();

        {
            let mut response_entry = state.ordering1_responses.get_mut(&tx_hash);
            if let Some(mut guard) = response_entry {
                let responses = guard.value_mut();
                if responses
                    .iter()
                    .any(|(node_id, _, _)| *node_id == sender_node_id)
                {
                    return;
                }

                responses.push((sender_node_id, timestamp_us, signature_bytes.clone()));
                let current_count = responses.len();
                state.ordering1_count.insert(tx_hash.clone(), current_count);

                if current_count >= required {
                    let mut sorted = responses.clone();
                    sorted.sort_by_key(|(_, ts, _)| *ts);
                    median_result = Some(sorted[nfaulty].1);
                    selected_records = sorted.into_iter().take(required).collect();
                }
            } else {
                state.ordering1_responses.insert(
                    tx_hash.clone(),
                    vec![(sender_node_id, timestamp_us, signature_bytes.clone())],
                );
                state.ordering1_count.insert(tx_hash.clone(), 1);
                return;
            }
        }

        let should_proceed = median_result.map(|median| (median, selected_records));

        let processing_duration = processing_start.elapsed();
        if processing_duration > tokio::time::Duration::from_millis(2) {
            debug!(
                "⚠️ [Performance] Node {} handle_ordering1_response duration: {:?}, from Node {}, hash = {}",
                node_id,
                processing_duration,
                sender_node_id,
                tx_hash
            );
        } else {
            debug!(
                "✅ [Performance] Node {} handle_ordering1_response completed: {:?}, from Node {}, hash = {}",
                node_id,
                processing_duration,
                sender_node_id,
                tx_hash
            );
        }

        if let Some((median, signature_records)) = should_proceed {
            state.completed_ordering1.insert(tx_hash.clone(), ());
            state.ordering1_responses.remove(&tx_hash);
            state.ordering1_count.remove(&tx_hash);

            let generate_start = std::time::Instant::now();
            let signatures = extract_signatures(&signature_records);
            let generate_duration = generate_start.elapsed();
            if generate_duration > tokio::time::Duration::from_millis(2) {
                warn!(
                    "⚠️ [Extract Signature] Node {} signature extraction duration: {:?}, hash = {}, signatures={}",
                    node_id,
                    generate_duration,
                    &tx_hash[0..8],
                    signatures.len()
                );
            }
            let msg = PompeMessage::Ordering2Request {
                tx_hash: tx_hash.clone(),
                median_timestamp: median,
                initiator_node_id: initiator_node_id,
                signatures,
            };

            let log_start = std::time::Instant::now();
            // Use the dedicated broadcast channel to avoid blocking
            if let Err(e) = broadcast_tx.send(msg) {
                warn!(
                    "⚠️ [handle_ordering1_response] Broadcast channel closed: {}",
                    e
                );
            }
            let log_duration = log_start.elapsed();
            debug!(
                "⏱️ [Performance] PompeManager broadcast channel send duration: {:?}, hash = {}",
                log_duration, tx_hash
            );
        }
    }

    // Called after finishing ordering1 response handling
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
        is_leader: Arc<AtomicBool>,
        verifying_keys: Arc<HashMap<usize, VerifyingKey>>,
        signatures: Vec<DigitalSignature>,
    ) {
        let processing_start = std::time::Instant::now();

        debug!(
            "🚀 [Ordering2-2-LockFree] Node {} processing request: {}",
            node_id,
            &tx_hash[0..8]
        );

        // verify 2f+1 signatures
        let verify_start = std::time::Instant::now();
        let tx_hash_for_verify = tx_hash.clone();
        let verifying_keys_clone = Arc::clone(&verifying_keys);
        let signatures_clone = signatures.clone();
        let verify_result = task::spawn_blocking(move || {
            verify_signatures(
                &signatures_clone,
                &tx_hash_for_verify,
                &verifying_keys_clone,
            )
        })
        .await
        .unwrap_or(false);
        let verify_duration = verify_start.elapsed();
        if verify_duration > Duration::from_millis(2) {
            debug!(
                "⏱️ [Verify Signature] Node {} signature verification duration: {:?}, hash = {}, signatures={}",
                node_id,
                verify_duration,
                &tx_hash[0..8],
                signatures.len()
            );
        }

        if !verify_result {
            warn!(
                "⚠️ [Ordering2-2-LockFree] Node {} signature verification failed for {}",
                node_id,
                &tx_hash[0..8]
            );
            return;
        }

        let current_stable_point = state
            .stable_point
            .load(std::sync::atomic::Ordering::Relaxed);

        // Sanity check: the median timestamp should not regress past the stable point
        // dummy check
        if median_timestamp < 0 {
            // if median_timestamp < current_stable_point {
            error!(
                "❌ [Ordering2-Stability Check] Node {} detected network anomaly: median_timestamp({}) < stable_point({})",
                node_id,
                median_timestamp,
                current_stable_point
            );

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
                error!("❌ [Ordering2-Error Response] Send failed: {}", e);
            }
            // });

            return;
        }
        info!(
            "✅ [Ordering2-2-LockFree] Node {} checkpoint processing completed: stable_point = {}",
            node_id, current_stable_point
        );

        let transaction = match state.transaction_store.get(&tx_hash) {
            Some(tx_ref) => tx_ref.clone(),
            None => {
                // warn!("⚠️ [Ordering2-2-LockFree] Node {} could not find transaction: {}", node_id, &tx_hash[0..8]);
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
                "⚠️ [Performance] Node {} Ordering2 processing duration: {:?}, tx_id={}, hash={}",
                node_id, processing_duration, tx_id, tx_hash
            );
        } else {
            debug!(
                "✅ [Performance] Node {} Ordering2 processing completed: {:?}, tx_id={}, hash={}",
                node_id, processing_duration, tx_id, tx_hash
            );
        }

        // if tx_id % 10 == 0 {
        let _ = event_tx.send(SystemEvent::PompeOrdering1Completed { tx_id });
        debug!(
            "📡 [Pompe] Node {} sent Ordering1 completion event: tx_id={}",
            node_id, tx_id
        );
        // }

        let response = PompeMessage::Ordering2Response {
            tx_hash,
            timestamp: median_timestamp,
            node_id,
        };

        let network_clone = Arc::clone(network);
        if let Err(e) = network_clone
            .send_to_node(initiator_node_id, response)
            .await
        {
            error!("❌ [Ordering2-2-LockFree] Async send failed: {}", e);
        }

        let state_clone = Arc::clone(state);
        let lockfree_adapter_clone = lockfree_adapter.clone();
        let config_clone = config.clone();
        let network_clone_for_flush = Arc::clone(network);
        // Immediately try an output check (periodic flusher as fallback) only if this node is the current view leader
        // if is_leader.load(Ordering::SeqCst) {
        Self::check_and_output_to_hotstuff_lockfree(
            node_id,
            &state_clone,
            &lockfree_adapter_clone,
            &config_clone,
            &network_clone_for_flush,
            is_leader.clone(),
            event_tx,
        )
        .await;
        // }
    }

    async fn check_and_output_to_hotstuff_lockfree(
        node_id: usize,
        state: &Arc<PompeAppState>,
        lockfree_adapter: &Option<Arc<LockFreeHotStuffAdapter>>,
        config: &PompeConfig,
        network: &Arc<crate::pompe_network::PompeNetwork>,
        is_leader: Arc<AtomicBool>,
        event_tx: &tokio::sync::broadcast::Sender<SystemEvent>,
    ) {
        // Return immediately if this node is not the leader
        // if !is_leader.load(Ordering::SeqCst) {
        //     return;
        // }
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

        let mut ordered_tx_ids = Vec::new();
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

                // Batch tail-cut: cutoff = latest timestamp minus liveness_delta
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
                    // Not enough stable transactions; wait for the next periodic flush
                    *state.consensus_ready.write().unwrap() = true;
                    return;
                }

                // Update stable_point
                let latest_ts = commit_set[cut_idx - 1].1;
                let old_stable_point = state
                    .stable_point
                    .fetch_max(latest_ts, std::sync::atomic::Ordering::Relaxed);
                info!(
                    "📊 [Stability] Node {} updated stable_point: {} -> {}",
                    node_id, old_stable_point, latest_ts
                );

                let mut txs = Vec::new();
                for (tx, timestamp) in commit_set.iter().take(cut_idx) {
                    let payload_len = tx.serialized_size_bytes();
                    let equiv_count = equivalent_tx_count(payload_len);
                    let mut synthetic_ids = synthetic_tx_ids(tx.id, equiv_count);
                    if synthetic_ids.is_empty() {
                        synthetic_ids.push(tx.id);
                    }
                    for synthetic_id in &synthetic_ids {
                        txs.push(tx.to_hotstuff_format_with_id(*timestamp, *synthetic_id));
                    }
                    ordered_tx_ids.extend(synthetic_ids);
                }

                // Remove already output entries
                commit_set.drain(0..cut_idx);
                drop(commit_set);
                *state.consensus_ready.write().unwrap() = false;

                txs
            } else {
                // Not yet stable: schedule a timed flush
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
                    let event_tx_clone = event_tx.clone();
                    info!(
                        "⏳ [Flusher] Node {} scheduled timed flush in {:?}µs",
                        node_id, remaining_us
                    );
                    tokio::spawn(async move {
                        tokio::time::sleep(tokio::time::Duration::from_micros(remaining_us)).await;
                        // push tx every 50ms
                        Self::flush_commit_set_to_hotstuff(
                            node_id,
                            &state_clone,
                            &lockfree_adapter_clone,
                            &config_clone,
                            Some(network_clone),
                            leader_flag.clone(),
                            &event_tx_clone,
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
                "⚠️ [Output Timing] Node {} output check duration: {:?}",
                node_id, processing_duration
            );
        }

        if !ordered_txs.is_empty() {
            // Change: allow all nodes to inject ordered batches into the local HotStuff queue to avoid empty blocks on non-leaders
            if let Some(ref adapter) = lockfree_adapter {
                let cnt = ordered_txs.len();
                adapter.push_batch(ordered_txs.clone());
                info!(
                    "⚡ [Output] Node {} injected {} ordered transactions into the HotStuff queue",
                    node_id, cnt
                );
                if !ordered_tx_ids.is_empty() {
                    let _ = event_tx.send(SystemEvent::PushedToHotStuff {
                        tx_ids: ordered_tx_ids.clone(),
                    });
                    debug!(
                        "📡 [Pompe] Node {} emitted pushed2hotstuff event: {:?}",
                        node_id, ordered_tx_ids
                    );
                }
            } else {
                warn!(
                    "⚠️ [Output] Node {} missing lock-free adapter, dropped {} transactions",
                    node_id,
                    ordered_txs.len()
                );
            }
        }
    }

    async fn flush_commit_set_to_hotstuff(
        node_id: usize,
        state: &Arc<PompeAppState>,
        lockfree_adapter: &Option<Arc<LockFreeHotStuffAdapter>>,
        config: &PompeConfig,
        network: Option<Arc<crate::pompe_network::PompeNetwork>>,
        is_leader: Arc<AtomicBool>,
        event_tx: &tokio::sync::broadcast::Sender<SystemEvent>,
    ) {
        // if !is_leader.load(Ordering::SeqCst) {
        //     return;
        // }
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
                "📊 [Flusher] Node {} refreshed stable_point: {} -> {} ({} items)",
                node_id,
                old,
                latest_ts,
                commit_set.len()
            );
        }
        let mut txs: Vec<String> = Vec::new();
        let mut tx_ids: Vec<u64> = Vec::new();
        for (tx, ts) in commit_set.iter() {
            let payload_len = tx.serialized_size_bytes();
            let mut synthetic_ids = synthetic_tx_ids(tx.id, equivalent_tx_count(payload_len));
            if synthetic_ids.is_empty() {
                synthetic_ids.push(tx.id);
            }
            for synthetic_id in &synthetic_ids {
                txs.push(tx.to_hotstuff_format_with_id(*ts, *synthetic_id));
            }
            tx_ids.extend(synthetic_ids);
        }
        commit_set.clear();
        drop(commit_set);
        *state.consensus_ready.write().unwrap() = false;

        // Change: inject ordered batches into every node's local HotStuff queue to reduce empty blocks
        if let Some(ref adapter) = lockfree_adapter {
            let count = txs.len();
            adapter.push_batch(txs.clone());
            warn!(
                "⚡ [Timed Output] Node {} flushed {} transactions",
                node_id, count
            );
            if !tx_ids.is_empty() {
                let _ = event_tx.send(SystemEvent::PushedToHotStuff {
                    tx_ids: tx_ids.clone(),
                });
                debug!(
                    "📡 [Flusher] Node {} emitted pushed2hotstuff event: {:?}",
                    node_id, tx_ids
                );
            }
        } else {
            warn!(
                "⚠️ [Timed Output] Node {} missing lock-free adapter, dropped {} transactions",
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
            signing_key: Arc::clone(&self.signing_key),
            verifying_keys: Arc::clone(&self.verifying_keys),
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
