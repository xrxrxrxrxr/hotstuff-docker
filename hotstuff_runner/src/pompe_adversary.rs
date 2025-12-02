// hotstuff_runner/src/pompe.rs
//! Fully lock-free Pompe BFT implementation with crossbeam lock-free queues

use crate::flood_limiter::FloodLimiter;
use crate::pompe_network::PompeNetwork;
use crossbeam::queue::SegQueue;
use dashmap::DashMap;
use ed25519_dalek::SigningKey;
use hex;
use hotstuff_rs::types::crypto_primitives::VerifyingKey;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
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
use crate::tx_utils::{equivalent_tx_count, synthetic_tx_ids};
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
    // Whether the periodic flush task has been scheduled
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
    // List of all nodes (order matters for view-based leader rotation)
    all_node_ids: Vec<usize>,
    // Current view number and whether this node is the leader for that view
    current_view: Arc<AtomicU64>,
    is_current_leader: Arc<AtomicBool>,

    ordering1_tx: async_mpsc::Sender<(usize, PompeMessage)>,
    ordering1_rx: Arc<tokio::sync::Mutex<async_mpsc::Receiver<(usize, PompeMessage)>>>,

    ordering2_tx: async_mpsc::Sender<(usize, PompeMessage)>,
    ordering2_rx: Arc<tokio::sync::Mutex<async_mpsc::Receiver<(usize, PompeMessage)>>>,

    general_tx: async_mpsc::Sender<(usize, PompeMessage)>,
    general_rx: Arc<tokio::sync::Mutex<async_mpsc::Receiver<(usize, PompeMessage)>>>,

    // Dedicated broadcast channel (Tokio mpsc) to avoid blocking the runtime
    broadcast_tx: async_mpsc::Sender<PompeMessage>,
    broadcast_rx: Arc<tokio::sync::Mutex<Option<async_mpsc::Receiver<PompeMessage>>>>,
    broadcast_limiter: Arc<FloodLimiter>,

    pub network: Option<Arc<crate::pompe_network::PompeNetwork>>,
    lockfree_adapter: Option<Arc<LockFreeHotStuffAdapter>>,
    event_tx: tokio::sync::broadcast::Sender<SystemEvent>,
    signing_key: Arc<SigningKey>,
    verifying_keys: Arc<HashMap<usize, VerifyingKey>>,
}

impl PompeManager {
    fn attack_only_mode() -> bool {
        static FLAG: OnceLock<bool> = OnceLock::new();
        *FLAG.get_or_init(|| {
            std::env::var("POMPE_ATTACK_ONLY")
                .map(|v| {
                    matches!(
                        v.trim().to_ascii_lowercase().as_str(),
                        "1" | "true" | "yes" | "on"
                    )
                })
                .unwrap_or(true)
        })
    }

    pub fn attack_only_enabled() -> bool {
        Self::attack_only_mode()
    }

    fn build_attack_broadcast_limiter(config: &PompeConfig) -> FloodLimiter {
        if !Self::attack_only_mode() {
            return FloodLimiter::unlimited("pompe-broadcast");
        }

        fn env_f64(key: &str) -> Option<f64> {
            std::env::var(key).ok()?.trim().parse::<f64>().ok()
        }

        let default_rate = (config.queue_capacity.max(1) as f64) * 2.0;
        let default_burst = config.queue_capacity.max(1) as f64;

        let rate = env_f64("POMPE_ATTACK_BROADCAST_RATE").unwrap_or(default_rate);
        let burst = env_f64("POMPE_ATTACK_BROADCAST_BURST").unwrap_or(default_burst);

        FloodLimiter::new("pompe-broadcast", rate, burst)
    }

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
                "[cleanup] node {} purged {} completed transaction records",
                self.node_id, 500
            );
        }

        let orphan_ordering1 = self.state.ordering1_responses.len();
        if orphan_ordering1 > 500 {
            self.state.ordering1_responses.clear();
            self.state.ordering1_count.clear();
            warn!(
                "[cleanup] node {} removed {} orphaned ordering1 states",
                self.node_id, orphan_ordering1
            );
        }

        if self.state.transaction_initiators.len() > 1000 {
            self.state.transaction_initiators.clear();
            debug!("[cleanup] node {} cleared initiator records", self.node_id);
        }

        if self.state.completed_ordering2.len() > 1000 {
            self.state.completed_ordering2.clear();
            debug!(
                "[cleanup] node {} reset ordering2 completion markers",
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
        let (general_tx, general_rx) = async_mpsc::channel(config.queue_capacity);

        info!(
            "Creating Pompe manager with full network support, node {}, f={}",
            node_id, nfaulty
        );
        info!("Peer node list: {:?}", all_node_ids);

        let (ord1_tx, ord1_rx) = async_mpsc::channel(config.queue_capacity);
        let (ord2_tx, ord2_rx) = async_mpsc::channel(config.queue_capacity);
        let (broadcast_tx, broadcast_rx) = async_mpsc::channel(config.queue_capacity);

        let network = Arc::new(PompeNetwork::new(node_id, all_node_ids.clone()));
        let signing_key = Arc::new(signing_key);
        let verifying_keys = Arc::new(verifying_keys);
        let broadcast_limiter = Arc::new(Self::build_attack_broadcast_limiter(&config));

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
            broadcast_limiter,
            network: Some(network),
            lockfree_adapter: None,
            event_tx,
            signing_key,
            verifying_keys,
        }
    }

    pub fn set_lockfree_adapter(&mut self, adapter: Arc<LockFreeHotStuffAdapter>) {
        self.lockfree_adapter = Some(adapter);
        info!(
            "[lock-free setup] node {} installed lock-free HotStuff adapter",
            self.node_id
        );
    }

    pub fn debug_config(&self) {
        info!("[config] node {} Pompe settings:", self.node_id);
        info!("  - enabled: {}", self.config.enable);
        info!("  - batch size: {}", self.config.batch_size);
        info!("  - stable period: {} ms", self.config.stable_period_ms);
        info!("  - leader node: {}", self.config.leader_node_id);
        info!("  - fault tolerance f: {}", self.nfaulty);
        info!("  - total nodes: {}", self.nfaulty * 3 + 1);
        info!("  - required responses (2f+1): {}", 2 * self.nfaulty + 1);
        info!("  - verifying key entries: {}", self.verifying_keys.len());
        let signing_preview = hex::encode(&self.signing_key.to_bytes()[..4]);
        info!(
            "  - signing key configured (preview: {}...)",
            signing_preview
        );

        if let Some(ref network) = self.network {
            info!("  - network peers: {:?}", network.peer_node_ids);
            info!(
                "  - current node participates in network: {}",
                network.peer_node_ids.contains(&self.node_id)
            );
        } else {
            warn!("  - network not configured!");
        }
    }

    // Function to process a raw transaction string and call Ordering1
    pub async fn process_raw_transaction(&self, raw_tx: &str) -> Result<(), String> {
        if !self.config.enable {
            debug!("Pompe disabled; skipping transaction: {}", raw_tx);
            return Ok(());
        }

        if let Some(transaction) =
            PompeTransaction::from_raw_string(raw_tx, format!("client_{}", self.node_id))
        {
            if Self::attack_only_mode() {
                let tx_hash = transaction.hash();
                self.exec_ordering1(tx_hash, transaction).await?;
                return Ok(());
            }

            let tx_hash = transaction.hash();

            debug!(
                "[ordering1] node {} processing tx: {} -> hash {}, tx_id={}",
                self.node_id,
                raw_tx,
                &tx_hash[0..8],
                transaction.id
            );

            // self.state
            //     .transaction_store
            //     .insert(tx_hash.clone(), transaction.clone());

            // let current_count = self
            //     .state
            //     .batch_received
            //     .entry(tx_hash.clone())
            //     .and_modify(|count| *count += 1)
            //     .or_insert(1)
            //     .clone();

            // debug!(
            //     "[ordering1] node {} batch count: {} -> {}/{}",
            //     self.node_id,
            //     &tx_hash[0..8],
            //     current_count,
            //     self.config.batch_size
            // );

            self.exec_ordering1(tx_hash, transaction).await?;
            // if current_count == self.config.batch_size {
            //     self.state
            //         .transaction_initiators
            //         .insert(tx_hash.clone(), self.node_id);
            //     debug!(
            //         "[initiator] node {} marked as initiator for tx {}",
            //         self.node_id,
            //         &tx_hash[0..8]
            //     );

            //     // Fix: invoke the correct method
            //     self.exec_ordering1(tx_hash, transaction).await?;
            // } else {
            //     debug!(
            //         "[ordering1] node {} detected another initiator for this transaction",
            //         self.node_id
            //     );
            // }
        }

        Ok(())
    }

    async fn exec_ordering1(
        &self,
        tx_hash: String,
        transaction: PompeTransaction,
    ) -> Result<(), String> {
        let attack_only = Self::attack_only_mode();
        if !attack_only {
            debug!(
                "[ordering1-exec] node {} starting ordering1 for {}",
                self.node_id,
                &tx_hash[0..8]
            );
        }

        let broadcast_start = std::time::Instant::now();

        if self.network.is_some() {
            let request = PompeMessage::Ordering1Request {
                tx_hash: tx_hash.clone(),
                transaction: transaction.clone(),
                batch_size: self.config.batch_size,
                initiator_node_id: self.node_id,
            };

            // Use the dedicated broadcast channel (bounded, backpressure-aware)
            if attack_only {
                if let Err(e) = self.enqueue_attack_broadcast(request) {
                    warn!("[ordering1-exec] attack broadcast send failed: {}", e);
                }
            } else if let Err(e) = self.broadcast_tx.send(request).await {
                warn!("[ordering1-exec] broadcast queue full or closed: {}", e);
            }

            if !attack_only {
                let broadcast_duration = broadcast_start.elapsed();
                debug!(
                    "[ordering1-exec] node {} broadcast duration: {:?}",
                    self.node_id, broadcast_duration
                );
            }
        }

        Ok(())
    }

    fn enqueue_attack_broadcast(&self, request: PompeMessage) -> Result<(), String> {
        if !self.broadcast_limiter.allow(1.0) {
            return Ok(());
        }

        match self.broadcast_tx.try_send(request) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_msg)) => {
                self.broadcast_limiter.note_drop();
                Ok(())
            }
            Err(TrySendError::Closed(_msg)) => Err("Pompe broadcast channel closed".to_string()),
        }
    }

    pub async fn start_network_message_loop(&self) -> Result<(), String> {
        if let Some(ref network) = self.network {
            info!("Node {} starting Pompe network", self.node_id);

            if let Err(e) = network.start_server() {
                return Err(format!("Failed to start Pompe server: {}", e));
            }
            // Warm up connections to reduce first-send latency
            network.warm_up_connections();

            // Start the dedicated broadcast worker
            let broadcast_rx = {
                let mut rx_guard = self.broadcast_rx.lock().await;
                rx_guard.take()
            };

            if let Some(mut rx) = broadcast_rx {
                let net = Arc::clone(network);
                network.spawn(async move {
                    info!("Starting dedicated broadcast worker");
                    while let Some(msg) = rx.recv().await {
                        if let Err(e) = net.broadcast_skip_self(msg).await {
                            error!("Dedicated broadcast failed: {}", e);
                        }
                    }
                });
            }

            if Self::attack_only_mode() {
                info!(
                    "Node {} attack-only: inbound Pompe message loop disabled",
                    self.node_id
                );
                return Ok(());
            }

            // Listen for HotStuff view start events to compute the current leader (supports rotating leader mode)
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
                info!("Node {} started Pompe message receive loop", node_id);
                let mut total_messages = 0;
                let mut ordering1_count = 0;
                let mut ordering2_count = 0;

                loop {
                    if let Some((sender_id, message)) = network_clone.recv().await {
                        debug!("[receiver] node {} received message from node {}", node_id, sender_id);
                        total_messages += 1;

                        match &message {
                            PompeMessage::Ordering1Request { .. } |
                            PompeMessage::Ordering1Response { .. } => {
                                ordering1_count += 1;
                                debug!(
                                    "[dispatcher] node {} forwarded ordering1 message {:?} (totals: O1={}, O2={}, total={})",
                                    node_id,
                                    std::mem::discriminant(&message),
                                    ordering1_count,
                                    ordering2_count,
                                    total_messages
                                );

                                if let Err(e) = ordering1_tx.send((sender_id, message)).await {
                                    error!("Failed to enqueue into ordering1 queue (backpressure/closed): {}", e);
                                }
                            }

                            PompeMessage::Ordering2Request { .. } |
                            PompeMessage::Ordering2Response { .. } => {
                                ordering2_count += 1;
                                debug!(
                                    "[dispatcher] node {} forwarded ordering2 message {:?} (totals: O1={}, O2={}, total={})",
                                    node_id,
                                    std::mem::discriminant(&message),
                                    ordering1_count,
                                    ordering2_count,
                                    total_messages
                                );

                                if let Err(e) = ordering2_tx.send((sender_id, message)).await {
                                    error!("Failed to enqueue into ordering2 queue (backpressure/closed): {}", e);
                                }
                            }

                            _ => {
                                if let Err(e) = general_tx.send((sender_id, message)).await {
                                    error!("Failed to enqueue into general queue (backpressure/closed): {}", e);
                                }
                            }
                        }
                    }
                }
            });

            if Self::attack_only_mode() {
                info!(
                    "Node {} attack-only: skipping ordering/general processors and cleanup",
                    self.node_id
                );
                return Ok(());
            }

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
                    // Reuse the same logic as the validation path
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
        if Self::attack_only_mode() {
            info!(
                "Node {} attack-only: skipping ordering1 processor",
                self.node_id
            );
            return;
        }
        let ordering1_rx = Arc::clone(&self.ordering1_rx);
        let state = Arc::clone(&self.state);
        let network = self.network.clone();
        let node_id = self.node_id;
        let nfaulty = self.nfaulty;
        let broadcast_tx = self.broadcast_tx.clone();

        tokio::spawn(async move {
            info!("Node {} started lock-free ordering1 handler", node_id);

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
                            debug!("Received ordering1 request: {}, hash {}", tx_id, tx_hash);
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
                            signature,
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
        if Self::attack_only_mode() {
            info!(
                "Node {} attack-only: skipping ordering2 processor",
                self.node_id
            );
            return;
        }
        let ordering2_rx = Arc::clone(&self.ordering2_rx);
        let state = Arc::clone(&self.state);
        let network = self.network.clone();
        let node_id = self.node_id;
        let lockfree_adapter = self.lockfree_adapter.clone();
        let config = self.config.clone();
        let event_tx = self.event_tx.clone();
        let is_leader_flag_for_o2 = self.is_current_leader.clone();

        tokio::spawn(async move {
            info!("Node {} started lock-free ordering2 handler", node_id);

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
        if Self::attack_only_mode() {
            info!(
                "Node {} attack-only: skipping general message processor",
                self.node_id
            );
            return;
        }
        let general_rx = Arc::clone(&self.general_rx);
        let lockfree_adapter = self.lockfree_adapter.clone();
        let node_id = self.node_id;
        tokio::spawn(async move {
            info!("Node {} started general message handler", node_id);
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
                                info!(
                                    "[ordered-delivery] node {} received {} ordered transactions from {} and pushed to HotStuff queue",
                                    node_id,
                                    count,
                                    initiator
                                );
                            } else {
                                warn!(
                                    "[ordered-delivery] node {} has no HotStuff adapter; dropping delivery",
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
            "[handle_ordering1_request] node {} handling request: tx_id={}, hash={}",
            node_id,
            transaction.id,
            &tx_hash[0..8]
        );

        let should_respond = if state.ordering1_responses.contains_key(&tx_hash) {
            false
        } else {
            // state.transaction_store.insert(tx_hash.clone(), transaction);
            // warn!("[ordering1-first] node {} recorded new transaction: hash {}", node_id, &tx_hash[0..8]);
            // state
            //     .ordering1_responses
            //     .insert(tx_hash.clone(), Vec::new());
            // state.ordering1_count.insert(tx_hash.clone(), 0);
            true
        };

        let check_duration = processing_start.elapsed();
        if check_duration > tokio::time::Duration::from_millis(1) {
            debug!(
                "[timing] node {} ordering1 check duration: {:?}",
                node_id, check_duration
            );
        }

        if !should_respond {
            debug!(
                "[handle_ordering1_request] node {} already responded: {}",
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
            signature: vec![0u8; 64],
        };

        let network_clone = Arc::clone(network);
        let tx_hash_for_async = tx_hash_clone.clone();
        // tokio::spawn(async move {
        if let Err(e) = network_clone
            .send_to_node(initiator_node_id, response)
            .await
        {
            error!("[handle_ordering1_request] async send failed: {}", e);
        }
        info!(
            "[handle_ordering1_request] node {} sent ordering1 response to node {}: hash {}",
            node_id, initiator_node_id, tx_hash_for_async
        );
        // });

        if Self::attack_only_mode() {
            return;
        }

        let total_duration = processing_start.elapsed();
        if total_duration > tokio::time::Duration::from_millis(5) {
            debug!(
                "[performance] node {} handle_ordering1_request duration: {:?}, hash {}",
                node_id, total_duration, tx_hash_clone
            );
        } else {
            debug!(
                "[performance] node {} handle_ordering1_request completed in {:?}, hash {}",
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
        _signature_bytes: Vec<u8>,
    ) {
        let processing_start = std::time::Instant::now();

        if Self::attack_only_mode() {
            return;
        }

        if node_id != initiator_node_id {
            return;
        }

        if state.completed_ordering1.contains_key(&tx_hash) {
            return;
        }

        debug!(
            "[handle_ordering1_response] node {} received timestamp for {}",
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
            debug!(
                "[performance] node {} handle_ordering1_response duration: {:?}, from node {}, hash {}",
                node_id,
                processing_duration,
                sender_node_id,
                tx_hash
            );
        } else {
            debug!(
                "[performance] node {} handle_ordering1_response completed in {:?}, from node {}, hash {}",
                node_id,
                processing_duration,
                sender_node_id,
                tx_hash
            );
        }

        // warn!("😈 [Adversary] Node {} holds Ordering2 Request: hash = {}", node_id, &tx_hash[0..8]);

        // if let Some(median) = should_proceed {
        //     let msg = PompeMessage::Ordering2Request {
        //         tx_hash: tx_hash.clone(),
        //         median_timestamp: median,
        //         initiator_node_id: initiator_node_id,
        //     };

        //     let log_start = std::time::Instant::now();
        //     // Use the broadcast channel to avoid blocking
        //     if let Err(e) = broadcast_tx.send(msg).await {
        //         warn!("[handle_ordering1_response] broadcast queue backpressure/closed: {}", e);
        //     }
        //     let log_duration = log_start.elapsed();
        //     debug!("[performance] PompeManager broadcast send duration: {:?}, hash {}", log_duration, tx_hash);
        // }
    }

    // Called after handling ordering1 responses
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
            "[ordering2-lockfree] node {} processing request: {}",
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
            error!(
                "[ordering2-stable-check] node {} detected anomaly: median_timestamp({}) < stable_point({})",
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
                error!("[ordering2-error-response] send failed: {}", e);
            }
            // });

            return;
        }
        info!(
            "[ordering2-lockfree] node {} checkpoint processed: stable_point = {}",
            node_id, current_stable_point
        );

        let transaction = match state.transaction_store.get(&tx_hash) {
            Some(tx_ref) => tx_ref.clone(),
            None => {
                // warn!("[ordering2-lockfree] node {} could not find transaction: {}", node_id, &tx_hash[0..8]);
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
                "[timing] node {} ordering2 processing duration: {:?}, tx_id={}, hash={}",
                node_id, processing_duration, tx_id, tx_hash
            );
        } else {
            debug!(
                "[timing] node {} ordering2 processing completed in {:?}, tx_id={}, hash={}",
                node_id, processing_duration, tx_id, tx_hash
            );
        }

        if !Self::attack_only_mode() {
            let _ = event_tx.send(SystemEvent::PompeOrdering1Completed { tx_id });
            debug!(
                "[pompe] node {} emitted ordering1 completion event: tx_id={}",
                node_id, tx_id
            );
        }
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
                error!("[ordering2-lockfree] async send failed: {}", e);
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
        // Non-leader nodes return early
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

                // Trim batch tail: cutoff = latest_timestamp - liveness_delta
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
                    // Not enough stable transactions; wait for the next flush cycle
                    *state.consensus_ready.write().unwrap() = true;
                    return;
                }

                // Update the stable point
                let latest_ts = commit_set[cut_idx - 1].1;
                let old_stable_point = state
                    .stable_point
                    .fetch_max(latest_ts, std::sync::atomic::Ordering::Relaxed);
                info!(
                    "[stable-point] node {} updated stable_point: {} -> {}",
                    node_id, old_stable_point, latest_ts
                );

                let mut txs: Vec<String> = Vec::new();
                for (tx, timestamp) in commit_set.iter().take(cut_idx) {
                    let mut synthetic_ids =
                        synthetic_tx_ids(tx.id, equivalent_tx_count(tx.serialized_size_bytes()));
                    if synthetic_ids.is_empty() {
                        synthetic_ids.push(tx.id);
                    }
                    for synthetic_id in &synthetic_ids {
                        txs.push(tx.to_hotstuff_format_with_id(*timestamp, *synthetic_id));
                    }
                }

                // Remove already emitted entries
                commit_set.drain(0..cut_idx);
                drop(commit_set);
                *state.consensus_ready.write().unwrap() = false;

                txs
            } else {
                // Not yet stable: schedule another timed flush
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
                        "[flusher] node {} scheduled timed flush in {:?}µs",
                        node_id, remaining_us
                    );
                    tokio::spawn(async move {
                        tokio::time::sleep(tokio::time::Duration::from_micros(remaining_us)).await;
                        // Run flush when the timer fires
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
                "[timing] node {} output check duration: {:?}",
                node_id, processing_duration
            );
        }

        if !ordered_txs.is_empty() {
            // Update: allow all nodes to inject into the local HotStuff queue to avoid empty blocks on non-leaders
            if let Some(ref adapter) = lockfree_adapter {
                let cnt = ordered_txs.len();
                adapter.push_batch(ordered_txs.clone());
                info!(
                    "[output] node {} injected {} ordered transactions into the HotStuff queue",
                    node_id, cnt
                );
            } else {
                warn!(
                    "[output] node {} missing lock-free adapter; dropping {} transactions",
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
        if Self::attack_only_mode() {
            return;
        }
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
                "[flusher] node {} refreshed stable_point: {} -> {} ({} entries)",
                node_id,
                old,
                latest_ts,
                commit_set.len()
            );
        }
        let mut txs: Vec<String> = Vec::new();
        for (tx, ts) in commit_set.iter() {
            let mut synthetic_ids =
                synthetic_tx_ids(tx.id, equivalent_tx_count(tx.serialized_size_bytes()));
            if synthetic_ids.is_empty() {
                synthetic_ids.push(tx.id);
            }
            for synthetic_id in &synthetic_ids {
                txs.push(tx.to_hotstuff_format_with_id(*ts, *synthetic_id));
            }
        }
        commit_set.clear();
        drop(commit_set);
        *state.consensus_ready.write().unwrap() = false;

        // Change: all nodes inject into the local HotStuff queue to reduce empty blocks
        if let Some(ref adapter) = lockfree_adapter {
            let count = txs.len();
            adapter.push_batch(txs.clone());
            info!(
                "[scheduled-output] node {} flushed {} transactions",
                node_id, count
            );
        } else {
            warn!(
                "[scheduled-output] node {} missing lock-free adapter; dropping {} transactions",
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
            broadcast_limiter: Arc::clone(&self.broadcast_limiter),
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
