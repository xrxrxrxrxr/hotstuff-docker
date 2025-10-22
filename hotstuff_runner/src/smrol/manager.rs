use crate::event::SystemEvent;
use crate::smrol::{
    adapter::SmrolHotStuffAdapter,
    consensus::{Consensus, TransactionEntry},
    crypto::derive_threshold_keys,
    finalization::OutputFinalization,
    message::{SmrolMessage, SmrolTransaction},
    network::SmrolTcpNetwork,
    pnfifo::{FinalMsg, PnfifoBc, ProposalMsg, VoteMsg},
    sequencing::TransactionSequencing,
    ModuleMessage,
};
use ed25519_dalek::{SigningKey, VerifyingKey};
use hex;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::{broadcast, mpsc as async_mpsc, Mutex as AsyncMutex, RwLock};
use tokio::time::interval;
use tracing::{debug, error, info, warn};

#[derive(Debug, Clone)]
pub struct SmrolConfig {
    pub enable: bool,
    pub f: usize,
    pub capital_k: usize,
    pub epoch_timeout_ms: u64,
    pub pnfifo_threshold: usize,
}

impl Default for SmrolConfig {
    fn default() -> Self {
        Self {
            enable: true,
            f: 1,
            // k: 3, // 🔥🔥 文中 k=O(n)
            capital_k: 1, // 🔥🔥 修改点
            epoch_timeout_ms: 100,
            pnfifo_threshold: 3,
        }
    }
}

pub struct SmrolManager {
    pub node_id: usize,
    pub config: SmrolConfig,
    pub n: usize,

    pub network: Arc<SmrolTcpNetwork>,
    pub consensus: Arc<RwLock<Consensus>>,
    pub sequencing: Arc<TransactionSequencing>,
    pub finalization: Arc<RwLock<OutputFinalization>>,
    hotstuff_adapter: OnceLock<Arc<SmrolHotStuffAdapter>>,
    pub event_tx: broadcast::Sender<SystemEvent>,

    pub signing_key: SigningKey,
    pub verifying_keys: HashMap<usize, VerifyingKey>,
    broadcast_tx: async_mpsc::UnboundedSender<SmrolTransaction>,
    pnfifo_proposal_tx: async_mpsc::UnboundedSender<(usize, ProposalMsg)>,
    pnfifo_vote_tx: async_mpsc::UnboundedSender<(usize, VoteMsg)>,
    pnfifo_final_tx: async_mpsc::UnboundedSender<(usize, FinalMsg)>,
    sequencing_inbound_tx: async_mpsc::UnboundedSender<ModuleMessage>,
    sequencing_output_rx: Arc<AsyncMutex<async_mpsc::UnboundedReceiver<TransactionEntry>>>,
    consensus_proposal_tx: async_mpsc::UnboundedSender<ModuleMessage>,
    consensus_proposal_rx: Arc<AsyncMutex<async_mpsc::UnboundedReceiver<ModuleMessage>>>,
    consensus_vote_tx: async_mpsc::UnboundedSender<ModuleMessage>,
    consensus_vote_rx: Arc<AsyncMutex<async_mpsc::UnboundedReceiver<ModuleMessage>>>,
}

impl SmrolManager {
    pub async fn new(
        node_id: usize,
        config: SmrolConfig,
        signing_key: SigningKey,
        verifying_keys: HashMap<usize, VerifyingKey>,
        peer_addrs: HashMap<usize, SocketAddr>,
        event_tx: broadcast::Sender<SystemEvent>,
    ) -> Result<Self, String> {
        let n = verifying_keys.len();

        let (threshold_share, threshold_public) =
            derive_threshold_keys(node_id, config.f, &verifying_keys)
                .map_err(|e| format!("derive threshold keys failed: {}", e))?;

        let pnfifo = Arc::new(
            PnfifoBc::new(
                node_id,
                n,
                signing_key.clone(),
                verifying_keys.clone(),
                peer_addrs,
            )
            .await?,
        );

        // start PNFIFO network listener
        pnfifo.start().await?;
        info!("✅ [SMROL] PNFIFO自动启动完成");

        // 启动PNFIFO定期清理任务，防止内存泄漏和延迟累积
        let pnfifo_for_cleanup = Arc::clone(&pnfifo);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));
            loop {
                interval.tick().await;
                pnfifo_for_cleanup.cleanup_old_slots(100).await; // 保留最近100个slot
            }
        });

        let pnfifo_proposal_tx = pnfifo.proposal_sender();
        let pnfifo_vote_tx = pnfifo.vote_sender();
        let pnfifo_final_tx = pnfifo.final_sender();

        let network = pnfifo.network();
        network.warm_up_connections();

        let finalization = Arc::new(RwLock::new(OutputFinalization::new(
            node_id,
            config.f,
            Arc::clone(&network),
            signing_key.clone(),
        )));

        let (sequenced_entry_tx, sequenced_entry_rx_raw) =
            async_mpsc::unbounded_channel::<TransactionEntry>();
        let sequencing_output_rx = Arc::new(AsyncMutex::new(sequenced_entry_rx_raw));

        let sequencing = TransactionSequencing::new(
            node_id,
            n,
            config.f,
            config.pnfifo_threshold,
            Arc::clone(&network),
            Arc::clone(&pnfifo),
            threshold_share,
            threshold_public.clone(),
            signing_key.clone(),
            verifying_keys.clone(),
            Arc::clone(&finalization),
            sequenced_entry_tx,
        );

        let consensus = Consensus::new(
            node_id,
            n,
            config.f,
            Arc::clone(&network),
            signing_key.clone(),
            verifying_keys.clone(),
            Arc::clone(&finalization),
            event_tx.clone(),
        );

        let sequencing_arc = Arc::new(sequencing);
        sequencing_arc.start_processing();
        let sequencing_inbound_tx = sequencing_arc.inbound_sender();
        let consensus_arc = Arc::new(RwLock::new(consensus));

        let broadcast_workers = Self::smrol_broadcast_worker_count();
        let (broadcast_tx, broadcast_rx_raw) = async_mpsc::unbounded_channel::<SmrolTransaction>();
        let broadcast_rx = Arc::new(AsyncMutex::new(broadcast_rx_raw));

        for worker_id in 0..broadcast_workers {
            let sequencing_for_worker = Arc::clone(&sequencing_arc);
            let rx = Arc::clone(&broadcast_rx);
            tokio::spawn(async move {
                info!("[SMROL] Broadcast worker {} started", worker_id);

                loop {
                    let transaction = {
                        let mut guard = rx.lock().await;
                        guard.recv().await
                    };

                    let Some(transaction) = transaction else {
                        warn!(
                            "⚠️ [SMROL] Broadcast worker {} exited (channel closed)",
                            worker_id
                        );
                        break;
                    };

                    let tx_id = transaction.id;
                    let result = sequencing_for_worker.smrol_broadcast(transaction).await;

                    match result {
                        Ok(_) => debug!(
                            "✅ [SMROL] Broadcast worker {} dispatched tx_id={} to sequencing",
                            worker_id, tx_id
                        ),
                        Err(e) => error!(
                            "[SMROL] Broadcast worker {} sequencing broadcast failed: {}",
                            worker_id, e
                        ),
                    }
                }
            });
        }

        let (consensus_proposal_tx, consensus_proposal_rx_raw) =
            async_mpsc::unbounded_channel::<ModuleMessage>();
        let consensus_proposal_rx = Arc::new(AsyncMutex::new(consensus_proposal_rx_raw));
        let (consensus_vote_tx, consensus_vote_rx_raw) =
            async_mpsc::unbounded_channel::<ModuleMessage>();
        let consensus_vote_rx = Arc::new(AsyncMutex::new(consensus_vote_rx_raw));

        Ok(Self {
            node_id,
            config,
            n,
            network,
            consensus: consensus_arc,
            sequencing: sequencing_arc,
            finalization,
            hotstuff_adapter: OnceLock::new(),
            event_tx,
            signing_key,
            verifying_keys,
            broadcast_tx,
            pnfifo_proposal_tx,
            pnfifo_vote_tx,
            pnfifo_final_tx,
            sequencing_inbound_tx,
            sequencing_output_rx,
            consensus_proposal_tx,
            consensus_proposal_rx,
            consensus_vote_tx,
            consensus_vote_rx,
        })
    }

    pub async fn set_hotstuff_adapter(&self, adapter: Arc<SmrolHotStuffAdapter>) {
        {
            let mut consensus = self.consensus.write().await;
            consensus.set_hotstuff_adapter(adapter.clone());
        }
        if self.hotstuff_adapter.set(adapter).is_err() {
            warn!("⚠️ [SMROL] HotStuff adapter already connected");
        } else {
            info!("🔗 [SMROL] Connected to HotStuff adapter");
        }
    }

    // public interface to process incoming SMROL transactions
    pub async fn process_smrol_transaction(
        &self,
        transaction: SmrolTransaction,
    ) -> Result<(), String> {
        debug!(
            "📥 [SMROL] Processing transaction: {}:{}->{}:{}",
            transaction.id, transaction.from, transaction.to, transaction.amount
        );

        if let Err(e) = self.broadcast_tx.send(transaction) {
            return Err(format!("SMROL broadcast queue send failed: {}", e));
        }

        tokio::task::yield_now().await;

        Ok(())
    }

    fn spawn_processors(self: &Arc<Self>) {
        self.spawn_sequencing_output_processor();
        self.spawn_consensus_proposal_processor();
        self.spawn_consensus_vote_processor();
    }

    fn spawn_sequencing_output_processor(self: &Arc<Self>) {
        let output_rx = Arc::clone(&self.sequencing_output_rx);
        let manager = Arc::clone(self);
        tokio::spawn(async move {
            info!(
                "🔄 [SMROL] Node {} started sequencing output processor",
                manager.node_id
            );

            while let Some(entry) = {
                let mut rx = output_rx.lock().await;
                rx.recv().await
            } {
                if let Err(e) = manager.add_sequenced_transaction(entry).await {
                    warn!("⚠️ [SMROL] Failed to handle sequenced entry: {}", e);
                }
            }

            warn!("⚠️ [SMROL] Sequencing output processor exiting (channel closed)");
        });
    }

    fn spawn_consensus_proposal_processor(self: &Arc<Self>) {
        let proposal_rx = Arc::clone(&self.consensus_proposal_rx);
        let manager = Arc::clone(self);
        tokio::spawn(async move {
            info!(
                "🔄 [SMROL] Node {} started consensus proposal processor",
                manager.node_id
            );

            while let Some((sender_id, message)) = {
                let mut rx = proposal_rx.lock().await;
                rx.recv().await
            } {
                if let SmrolMessage::ConsensusProposal { .. } = &message {
                    if let Err(e) = manager.process_consensus_message(sender_id, message).await {
                        warn!("⚠️ [SMROL] Consensus proposal handling failed: {}", e);
                    }
                } else {
                    warn!(
                        "⚠️ [SMROL] Unexpected message in consensus-proposal queue: {:?}",
                        message
                    );
                }
            }

            warn!("⚠️ [SMROL] Consensus proposal processor exiting (channel closed)");
        });
    }

    fn spawn_consensus_vote_processor(self: &Arc<Self>) {
        let vote_rx = Arc::clone(&self.consensus_vote_rx);
        let manager = Arc::clone(self);
        tokio::spawn(async move {
            info!(
                "🔄 [SMROL] Node {} started consensus vote processor",
                manager.node_id
            );

            while let Some((sender_id, message)) = {
                let mut rx = vote_rx.lock().await;
                rx.recv().await
            } {
                if let SmrolMessage::ConsensusVote { .. } = &message {
                    if let Err(e) = manager.process_consensus_message(sender_id, message).await {
                        warn!("⚠️ [SMROL] Consensus vote handling failed: {}", e);
                    }
                } else {
                    warn!(
                        "⚠️ [SMROL] Unexpected message in consensus-vote queue: {:?}",
                        message
                    );
                }
            }

            warn!("⚠️ [SMROL] Consensus vote processor exiting (channel closed)");
        });
    }

    async fn process_consensus_message(
        self: &Arc<Self>,
        sender_id: usize,
        message: SmrolMessage,
    ) -> Result<(), String> {
        let mut consensus = self.consensus.write().await;
        consensus.handle_consensus_message(sender_id, message).await
    }

    pub async fn start_message_loop(self: Arc<Self>) -> Result<(), String> {
        info!("🔄 [SMROL] Message loop starting for node {}", self.node_id);

        self.spawn_processors();

        {
            let sequencing_handle = Arc::clone(&self.sequencing);
            let interval_secs = Self::seq_cleanup_interval_secs();
            tokio::spawn(async move {
                let mut ticker = interval(Duration::from_secs(interval_secs));
                loop {
                    ticker.tick().await;
                    sequencing_handle.cleanup_expired_state().await;
                }
            });
        }

        tokio::spawn(async move {
            info!("🔄 [SMROL] Unified网络循环启动");
            while let Some((sender_id, message)) = self.network.recv().await {
                if let Err(e) = self.process_network_message(sender_id, message).await {
                    warn!("⚠️ [SMROL] 处理网络消息失败: {}", e);
                }
            }
            debug!("ℹ️ [SMROL] Unified网络循环退出");
        });

        info!("✅ [SMROL] 消息处理循环已启动");
        Ok(())
    }

    async fn process_network_message(
        &self,
        sender_id: usize,
        message: SmrolMessage,
    ) -> Result<(), String> {
        match message {
            SmrolMessage::PnfifoProposal {
                sender_id: msg_sender,
                slot,
                value,
            } => {
                if msg_sender != sender_id {
                    warn!(
                        "⚠️ [SMROL] PNFIFO proposal sender mismatch: msg={} net={}",
                        msg_sender, sender_id
                    );
                }

                self.pnfifo_proposal_tx
                    .send((msg_sender, ProposalMsg { slot, value }))
                    .map_err(|e| format!("PNFIFO proposal channel closed: {}", e))?;
                Ok(())
            }
            SmrolMessage::PnfifoVote {
                leader_id,
                sender_id: msg_sender,
                slot,
                signature_share,
            } => {
                if msg_sender != sender_id {
                    warn!(
                        "⚠️ [SMROL] PNFIFO vote sender mismatch: msg={} net={}",
                        msg_sender, sender_id
                    );
                }

                self.pnfifo_vote_tx
                    .send((
                        leader_id,
                        VoteMsg {
                            sender_id: msg_sender,
                            slot,
                            signature_share,
                        },
                    ))
                    .map_err(|e| format!("PNFIFO vote channel send failed: {}", e))?;
                tokio::task::yield_now().await;
                Ok(())
            }
            SmrolMessage::PnfifoFinal {
                leader_id,
                sender_id: msg_sender,
                slot,
                value,
                combined_signature,
            } => {
                if msg_sender != sender_id {
                    warn!(
                        "⚠️ [SMROL] PNFIFO final sender mismatch: msg={} net={}",
                        msg_sender, sender_id
                    );
                }

                self.pnfifo_final_tx
                    .send((
                        leader_id,
                        FinalMsg {
                            sender_id: msg_sender,
                            slot,
                            value,
                            combined_signature,
                        },
                    ))
                    .map_err(|e| format!("PNFIFO final channel send failed: {}", e))?;
                tokio::task::yield_now().await;
                Ok(())
            }
            m @ SmrolMessage::SeqRequest {
                sender_id: msg_sender,
                ..
            }
            | m @ SmrolMessage::SeqResponse {
                sender_id: msg_sender,
                ..
            }
            | m @ SmrolMessage::SeqOrder {
                sender_id: msg_sender,
                ..
            }
            | m @ SmrolMessage::SeqMedian {
                sender_id: msg_sender,
                ..
            }
            | m @ SmrolMessage::SeqFinal {
                sender_id: msg_sender,
                ..
            } => {
                if msg_sender != sender_id {
                    warn!(
                        "⚠️ [SMROL] Sequencing sender mismatch: msg={} net={}",
                        msg_sender, sender_id
                    );
                }
                self.sequencing_inbound_tx
                    .send((sender_id, m))
                    .map_err(|e| format!("Sequencing inbound queue send failed: {}", e))?;
                tokio::task::yield_now().await;
                Ok(())
            }
            msg @ SmrolMessage::ConsensusProposal {
                sender_id: msg_sender,
                ..
            } => {
                if msg_sender != sender_id {
                    warn!(
                        "⚠️ [SMROL] Consensus proposal sender mismatch: msg={} net={}",
                        msg_sender, sender_id
                    );
                }
                self.consensus_proposal_tx
                    .send((sender_id, msg))
                    .map_err(|e| format!("Consensus proposal queue send failed: {}", e))?;
                tokio::task::yield_now().await;
                Ok(())
            }
            msg @ SmrolMessage::ConsensusVote {
                sender_id: msg_sender,
                ..
            } => {
                if msg_sender != sender_id {
                    warn!(
                        "⚠️ [SMROL] Consensus vote sender mismatch: msg={} net={}",
                        msg_sender, sender_id
                    );
                }
                self.consensus_vote_tx
                    .send((sender_id, msg))
                    .map_err(|e| format!("Consensus vote queue send failed: {}", e))?;
                tokio::task::yield_now().await;
                Ok(())
            }
            SmrolMessage::Warmup => Ok(()),
        }
    }

    pub async fn invoke_consensus_and_finalize(&self, epoch: u64) -> Result<(), String> {
        info!("🏛️ [SMROL] Starting consensus for epoch {}", epoch);

        {
            let mut consensus = self.consensus.write().await;
            consensus.run_consensus(epoch).await?;
        }
        Ok(())
    }

    pub async fn should_invoke_consensus(&self, epoch: u64) -> bool {
        let consensus = self.consensus.read().await;
        consensus.get_mi_size(epoch) >= self.config.capital_k // line 39
    }

    // line 38: take over sequenced transaction and add to Mi and finalization
    pub async fn add_sequenced_transaction(&self, entry: TransactionEntry) -> Result<(), String> {
        let entry_meta = (entry.vc_tx.len(), entry.s_tx);

        let epoch = {
            let consensus = self.consensus.read().await;
            consensus.get_current_epoch()
        };

        if let Some(tx_id) = Self::extract_tx_id(&entry) {
            if let Err(e) = self.event_tx.send(SystemEvent::SmrolOrderingCompleted {
                tx_ids: vec![tx_id],
            }) {
                warn!(
                    "⚠️ [SMROL] Node {} failed to emit ordering completion for tx {}: {}",
                    self.node_id, tx_id, e
                );
            }
        }

        let hotstuff_string =
            if let Ok(tx) = bincode::deserialize::<SmrolTransaction>(&entry.payload) {
                tx.to_hotstuff_format(entry.s_tx)
            } else {
                format!(
                    "{}:{}",
                    entry.s_tx,
                    hex::encode(&entry.vc_tx[..std::cmp::min(8, entry.vc_tx.len())])
                )
            };

        if let Some(adapter) = self.hotstuff_adapter.get() {
            adapter.output_to_hotstuff(vec![hotstuff_string.clone()], epoch);
            info!(
                "🚀 [SMROL→HotStuff] Node {} delivered tx for slot {} epoch {}",
                self.node_id, entry_meta.1, epoch
            );
        } else {
            warn!(
                "⚠️ [SMROL] HotStuff adapter not connected, dropping tx slot {}",
                entry_meta.1
            );
        }

        // 临时关闭 finalization，专注测试 HotStuff push 性能
        // {
        //     let mut finalization = self.finalization.lock().await;
        //     finalization.add_to_mi(epoch, entry_for_finalization);
        // }

        // info!(
        //     "🧮 [Manager] 记录Sequencing输出并推给HotStuff: epoch={} vc_bytes={} s_tx={}",
        //     epoch, entry_meta.0, entry_meta.1
        // );

        Ok(())
    }

    fn seq_cleanup_interval_secs() -> u64 {
        std::env::var("SMROL_SEQ_CLEANUP_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|&v| v > 0)
            .unwrap_or(30)
    }

    fn smrol_broadcast_worker_count() -> usize {
        std::env::var("SMROL_BROADCAST_WORKERS")
            .ok()
            .and_then(|v| v.parse().ok())
            .map(|n: usize| n.max(1))
            .unwrap_or(2)
    }

    fn extract_tx_id(entry: &TransactionEntry) -> Option<u64> {
        if let Ok(tx) = bincode::deserialize::<SmrolTransaction>(&entry.payload) {
            Some(tx.id)
        } else if let Ok(text) = std::str::from_utf8(&entry.payload) {
            let parts: Vec<&str> = text.split(':').collect();
            if parts.len() >= 3 && parts[0] == "smrol" {
                return parts[2].parse().ok();
            }
            if !parts.is_empty() {
                return parts[0].parse().ok();
            }
            None
        } else {
            None
        }
    }

    pub async fn debug_status(&self) {
        let consensus = self.consensus.read().await;
        info!("📊 [SMROL Status] Node {}", self.node_id);
        info!("  - Current epoch: {}", consensus.get_current_epoch());
        drop(consensus);
        info!(
            "  - HotStuff adapter connected: {}",
            self.hotstuff_adapter.get().is_some()
        );
        info!(
            "  - Network strong refs: {}",
            Arc::strong_count(&self.network)
        );
    }
}

pub fn load_smrol_config() -> SmrolConfig {
    use std::env;

    SmrolConfig {
        enable: env::var("SMROL_ENABLE")
            .unwrap_or_else(|_| "true".to_string())
            .parse()
            .unwrap_or(true),
        f: env::var("SMROL_F")
            .unwrap_or_else(|_| "1".to_string())
            .parse()
            .unwrap_or(1),
        capital_k: env::var("SMROL_K")
            .unwrap_or_else(|_| "1".to_string())
            .parse()
            .unwrap_or(1),
        epoch_timeout_ms: env::var("SMROL_EPOCH_TIMEOUT_MS")
            .unwrap_or_else(|_| "100".to_string())
            .parse()
            .unwrap_or(100),
        pnfifo_threshold: env::var("SMROL_PNFIFO_THRESHOLD")
            .unwrap_or_else(|_| "3".to_string())
            .parse()
            .unwrap_or(3),
    }
}
