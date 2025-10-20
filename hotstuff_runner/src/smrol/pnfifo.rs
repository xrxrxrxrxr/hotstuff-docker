use crate::smrol::crypto::{
    verify_combined_signature_bytes, verify_signature_share, SmrolThresholdSig,
};
use crate::smrol::message::SmrolMessage;
use crate::smrol::network::SmrolTcpNetwork;
use dashmap::DashMap;
use ed25519_dalek::{Signer, SigningKey, VerifyingKey};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex as StdMutex,
};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::{mpsc, Notify, RwLock};
use tracing::{debug, error, info, warn};

#[derive(Debug)]
struct PnfifoSlotState {
    output: Option<(Vec<u8>, Vec<u8>)>, // (value, signature)
    value: Option<Vec<u8>>,
    votes: HashMap<usize, Vec<u8>>,
    threshold_sig: SmrolThresholdSig,
    proposal_received: bool,
    final_stored: bool,
    pending_final: Option<(Vec<u8>, Vec<u8>)>,
    final_broadcasted: bool,
}

impl PnfifoSlotState {
    fn new(threshold: usize) -> Self {
        Self {
            output: None,
            value: None,
            votes: HashMap::new(),
            threshold_sig: SmrolThresholdSig::new(threshold),
            proposal_received: false,
            final_stored: false,
            pending_final: None,
            final_broadcasted: false,
        }
    }
}

// 专用通道的 PROPOSAL 消息
#[derive(Clone, Debug)]
pub struct ProposalMsg {
    pub slot: u64,
    pub value: Vec<u8>,
}

#[derive(Debug)]
pub struct VoteMsg {
    pub sender_id: usize,
    pub slot: u64,
    pub signature_share: Vec<u8>,
}

#[derive(Debug)]
pub struct FinalMsg {
    pub sender_id: usize,
    pub slot: u64,
    pub value: Vec<u8>,
    pub combined_signature: Vec<u8>,
}

#[derive(Debug)]
pub struct PnfifoBc {
    node_id: usize,
    total_nodes: usize,
    threshold: usize,

    // 算法状态
    current_slot: Arc<AtomicU64>,
    leader_slots: Vec<Option<Arc<DashMap<u64, PnfifoSlotState>>>>,
    leader_notifiers: Vec<Option<Arc<RwLock<HashMap<u64, Arc<Notify>>>>>>,
    proposal_txs: Vec<Option<UnboundedSender<ProposalMsg>>>,
    proposal_rxs: Vec<StdMutex<Option<UnboundedReceiver<ProposalMsg>>>>,
    vote_txs: Vec<Option<mpsc::Sender<VoteMsg>>>,
    vote_rxs: Vec<StdMutex<Option<mpsc::Receiver<VoteMsg>>>>,
    final_txs: Vec<Option<mpsc::Sender<FinalMsg>>>,
    final_rxs: Vec<StdMutex<Option<mpsc::Receiver<FinalMsg>>>>,
    active_leaders: Vec<usize>,

    // 密码学
    signing_key: SigningKey,
    verifying_keys: HashMap<usize, VerifyingKey>,

    // 网络
    network: Arc<SmrolTcpNetwork>,
    broadcast_tx: mpsc::Sender<SmrolMessage>,
}

impl PnfifoBc {
    pub async fn new(
        node_id: usize,
        total_nodes: usize,
        signing_key: SigningKey,
        verifying_keys: HashMap<usize, VerifyingKey>,
        peer_addrs: HashMap<usize, SocketAddr>,
    ) -> Result<Self, String> {
        let threshold = 2 * ((total_nodes - 1) / 3) + 1;

        info!(
            "[PNFIFO] Node {} initializing, threshold: {}/{}",
            node_id, threshold, total_nodes
        );

        let network = Arc::new(SmrolTcpNetwork::new(node_id, peer_addrs));
        network
            .start_server()
            .map_err(|e| format!("Failed to start PNFIFO network: {}", e))?;

        let current_slot = Arc::new(AtomicU64::new(1));

        let (broadcast_tx, mut broadcast_rx) =
            mpsc::channel::<SmrolMessage>(Self::broadcast_queue_capacity());
        let network_for_broadcast = Arc::clone(&network);
        let node_id_for_broadcast = node_id;
        network.spawn(async move {
            info!(
                "📡 [PNFIFO] Node {} 启动专用广播处理器",
                node_id_for_broadcast
            );
            while let Some(msg) = broadcast_rx.recv().await {
                if let Err(e) = network_for_broadcast.broadcast(msg).await {
                    error!("❌ [PNFIFO] 广播处理器失败: {}", e);
                }
            }
            warn!("[PNFIFO] 广播处理器退出 (channel closed)");
        });

        let mut leader_ids: Vec<usize> = verifying_keys.keys().cloned().collect();
        if !leader_ids.contains(&node_id) {
            leader_ids.push(node_id);
        }
        leader_ids.sort_unstable();
        leader_ids.dedup();

        let max_leader_id = *leader_ids.iter().max().unwrap_or(&node_id);
        let capacity = max_leader_id + 1;

        let mut leader_slots = vec![None; capacity];
        let mut leader_notifiers = vec![None; capacity];
        let mut proposal_txs = vec![None; capacity];
        let mut vote_txs = vec![None; capacity];
        let mut final_txs = vec![None; capacity];

        let mut proposal_rxs = Vec::with_capacity(capacity);
        let mut vote_rxs = Vec::with_capacity(capacity);
        let mut final_rxs = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            proposal_rxs.push(StdMutex::new(None));
            vote_rxs.push(StdMutex::new(None));
            final_rxs.push(StdMutex::new(None));
        }

        let mut active_leaders = Vec::with_capacity(leader_ids.len());

        for leader_id in leader_ids {
            let slots = Arc::new(DashMap::new());
            let notifiers = Arc::new(RwLock::new(HashMap::new()));

            let (proposal_tx, proposal_rx) = mpsc::unbounded_channel::<ProposalMsg>();
            let (vote_tx, vote_rx) = mpsc::channel::<VoteMsg>(Self::vote_queue_capacity());
            let (final_tx, final_rx) =
                mpsc::channel::<FinalMsg>(Self::final_queue_capacity());

            leader_slots[leader_id] = Some(slots);
            leader_notifiers[leader_id] = Some(notifiers);

            proposal_txs[leader_id] = Some(proposal_tx);
            vote_txs[leader_id] = Some(vote_tx);
            final_txs[leader_id] = Some(final_tx);

            *proposal_rxs[leader_id]
                .lock()
                .expect("proposal receiver lock poisoned") = Some(proposal_rx);
            *vote_rxs[leader_id]
                .lock()
                .expect("vote receiver lock poisoned") = Some(vote_rx);
            *final_rxs[leader_id]
                .lock()
                .expect("final receiver lock poisoned") = Some(final_rx);

            active_leaders.push(leader_id);
        }

        Ok(Self {
            node_id,
            total_nodes,
            threshold,
            current_slot,
            leader_slots,
            leader_notifiers,
            proposal_txs,
            proposal_rxs,
            vote_txs,
            vote_rxs,
            final_txs,
            final_rxs,
            active_leaders,
            signing_key,
            verifying_keys,
            network,
            broadcast_tx,
        })
    }

    // PNFIFO 内部：处理 leader 的 proposal（在专用通道任务中运行）
    async fn process_leader_proposal_for_state(
        pnfifo: &Arc<Self>,
        leader_id: usize,
        slots: Arc<DashMap<u64, PnfifoSlotState>>,
        notifiers: Arc<RwLock<HashMap<u64, Arc<Notify>>>>,
        slot: u64,
        value: Vec<u8>,
    ) -> Result<(), String> {
        let node_id = pnfifo.node_id;

        info!(
            "[PNFIFO] Node {} received PROPOSAL Leader {} slot {}",
            node_id, leader_id, slot
        );

        if !PnfifoBc::predicate_q_static(&value) {
            debug!(
                "[PNFIFO] Node {} rejected slot {} from leader {}: Q(v) not satisfied",
                node_id, slot, leader_id
            );
            return Ok(());
        }

        // 检查是否有延迟的 FINAL
        let mut skip_vote = false;
        let delayed_final = {
            let mut entry = slots
                .entry(slot)
                .or_insert_with(|| PnfifoSlotState::new(pnfifo.threshold));
            let slot_state = entry.value_mut();

            if slot_state.proposal_received && leader_id != node_id {
                debug!(
                    "[PNFIFO] Node {} already processed Leader {} slot {}",
                    node_id, leader_id, slot
                );
                skip_vote = true;
                None
            } else {
                slot_state.proposal_received = true;
                slot_state.value = Some(value.clone());
                slot_state.pending_final.take()
            }
        };

        if skip_vote {
            return Ok(());
        }

        if let Some((final_value, final_signature)) = delayed_final {
            debug!(
                "[PNFIFO] Node {} processing delayed FINAL for Leader {} slot {}",
                node_id, leader_id, slot
            );

            Self::finalize_with_signature_for_state(
                pnfifo,
                leader_id,
                slots.clone(),
                notifiers.clone(),
                slot,
                final_value,
                final_signature,
            )
            .await?;

            return Ok(());
        }

        let message_to_sign = Self::create_vote_message_static(slot, &value);
        let signature_share = pnfifo
            .signing_key
            .sign(&message_to_sign)
            .to_bytes()
            .to_vec();

        let vote_msg = SmrolMessage::PnfifoVote {
            leader_id,
            sender_id: node_id,
            slot,
            signature_share,
        };

        pnfifo
            .network
            .send_to_node(leader_id, vote_msg)
            .await
            .map_err(|e| format!("Failed to send VOTE: {}", e))?;

        info!(
            "[PNFIFO] Node {} sent *VOTE* for Leader {} slot {}",
            node_id, leader_id, slot
        );

        debug!(
            "[PNFIFO] Node {} skip waiting for FINAL for Leader {} slot {} (pipeline mode)",
            node_id, leader_id, slot
        );

        Ok(())
    }

    pub fn network(&self) -> Arc<SmrolTcpNetwork> {
        Arc::clone(&self.network)
    }

    pub fn get_senders(
        &self,
    ) -> Vec<Option<(UnboundedSender<ProposalMsg>, mpsc::Sender<VoteMsg>, mpsc::Sender<FinalMsg>)>>
    {
        let mut result = Vec::with_capacity(self.proposal_txs.len());
        for idx in 0..self.proposal_txs.len() {
            if let (Some(proposal_tx), Some(vote_tx), Some(final_tx)) = (
                self.proposal_txs[idx].as_ref(),
                self.vote_txs[idx].as_ref(),
                self.final_txs[idx].as_ref(),
            ) {
                result.push(Some((
                    proposal_tx.clone(),
                    vote_tx.clone(),
                    final_tx.clone(),
                )));
            } else {
                result.push(None);
            }
        }
        result
    }

    pub async fn broadcast(&self, slot: u64, value: Vec<u8>) -> Result<u64, String> {
        let slots = self
            .slots_for(self.node_id)
            .ok_or_else(|| format!("missing leader state for node {}", self.node_id))?;

        let proposal = Self::broadcast_proposal_for_leader(
            self.node_id,
            &self.current_slot,
            slots,
            self.threshold,
            slot,
            value,
        )
        .await?;

        self.broadcast_tx
            .send(proposal)
            .await
            .map_err(|e| format!("broadcast queue send failed: {}", e))?;

        debug!(
            "[PNFIFO] Node {} enqueued slot {} for broadcast (single queue)",
            self.node_id, slot
        );

        Ok(slot)
    }

    fn broadcast_queue_capacity() -> usize {
        std::env::var("PNFIFO_BROADCAST_QUEUE_CAP")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1024)
    }

    fn vote_queue_capacity() -> usize {
        std::env::var("PNFIFO_VOTE_QUEUE_CAP")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(4096)
    }

    fn final_queue_capacity() -> usize {
        std::env::var("PNFIFO_FINAL_QUEUE_CAP")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(4096)
    }

    fn slots_for(&self, leader_id: usize) -> Option<Arc<DashMap<u64, PnfifoSlotState>>> {
        self.leader_slots
            .get(leader_id)
            .and_then(|opt| opt.as_ref().map(Arc::clone))
    }

    fn notifiers_for(
        &self,
        leader_id: usize,
    ) -> Option<Arc<RwLock<HashMap<u64, Arc<Notify>>>>> {
        self.leader_notifiers
            .get(leader_id)
            .and_then(|opt| opt.as_ref().map(Arc::clone))
    }

    fn take_proposal_rx_for(&self, leader_id: usize) -> Option<UnboundedReceiver<ProposalMsg>> {
        self.proposal_rxs
            .get(leader_id)
            .and_then(|mutex| mutex.lock().ok()?.take())
    }

    fn take_vote_rx_for(&self, leader_id: usize) -> Option<mpsc::Receiver<VoteMsg>> {
        self.vote_rxs
            .get(leader_id)
            .and_then(|mutex| mutex.lock().ok()?.take())
    }

    fn take_final_rx_for(&self, leader_id: usize) -> Option<mpsc::Receiver<FinalMsg>> {
        self.final_rxs
            .get(leader_id)
            .and_then(|mutex| mutex.lock().ok()?.take())
    }

    async fn broadcast_proposal_for_leader(
        node_id: usize,
        current_slot: &Arc<AtomicU64>,
        slots: Arc<DashMap<u64, PnfifoSlotState>>,
        threshold: usize,
        slot: u64,
        value: Vec<u8>,
    ) -> Result<SmrolMessage, String> {
        current_slot.store(slot.saturating_add(1), std::sync::atomic::Ordering::Relaxed);

        info!(
            "[PNFIFO] Node {} broadcast *PROPOSAL* for slot {}",
            node_id, slot
        );

        {
            let mut entry = slots
                .entry(slot)
                .or_insert_with(|| PnfifoSlotState::new(threshold));
            let slot_state = entry.value_mut();
            slot_state.value = Some(value.clone());
            slot_state.proposal_received = true;
        }

        let proposal = SmrolMessage::PnfifoProposal {
            sender_id: node_id,
            slot,
            value,
        };

        Ok(proposal)
    }

    async fn handle_vote_for_state(
        pnfifo: &Arc<Self>,
        leader_id: usize,
        slots: Arc<DashMap<u64, PnfifoSlotState>>,
        sender_id: usize,
        slot: u64,
        signature_share: Vec<u8>,
    ) -> Result<(), String> {
        let node_id = pnfifo.node_id;

        info!(
            "[PNFIFO] Node {} received VOTE from {} for slot {} leader {}",
            node_id, sender_id, slot, leader_id
        );

        let mut should_finalize = false;
        let mut finalize_data = None;

        {
            let mut entry = slots
                .entry(slot)
                .or_insert_with(|| PnfifoSlotState::new(pnfifo.threshold));
            let slot_state = entry.value_mut();

            if let Some(ref value) = slot_state.value {
                let message_to_verify = PnfifoBc::create_vote_message_static(slot, value);

                if let Some(verifying_key) = pnfifo.verifying_keys.get(&sender_id) {
                    if verify_signature_share(&signature_share, &message_to_verify, verifying_key) {
                        if !slot_state.votes.contains_key(&sender_id) {
                            slot_state.votes.insert(sender_id, signature_share.clone());

                            let reached = slot_state
                                .threshold_sig
                                .add_share(sender_id, signature_share.clone());

                            if slot_state.votes.len() <= pnfifo.threshold {
                                debug!(
                                    "[PNFIFO] Node {} accepted VOTE from {} (slot {} leader {}), votes: {}/{}",
                                    node_id,
                                    sender_id,
                                    slot,
                                    leader_id,
                                    slot_state.votes.len(),
                                    pnfifo.threshold
                                );
                            }

                            if reached && !slot_state.final_broadcasted {
                                if let Ok(combined_sig) = slot_state.threshold_sig.combine() {
                                    slot_state.final_broadcasted = true;
                                    finalize_data = Some((value.clone(), combined_sig));
                                    should_finalize = true;

                                    info!(
                                        "[PNFIFO] Node {} reached threshold for slot {} leader {}",
                                        node_id, slot, leader_id
                                    );
                                }
                            }
                        }
                    } else {
                        warn!(
                            "[PNFIFO] Node {} rejected invalid signature from {}",
                            node_id, sender_id
                        );
                    }
                }
            }
        }

        if should_finalize {
            if let Some((value, combined_signature)) = finalize_data {
                PnfifoBc::broadcast_final(
                    pnfifo.broadcast_tx.clone(),
                    node_id,
                    leader_id,
                    slot,
                    value,
                    combined_signature,
                )
                .await?;
            }
        }

        Ok(())
    }

    async fn broadcast_final(
        broadcast_tx: mpsc::Sender<SmrolMessage>,
        node_id: usize,
        leader_id: usize,
        slot: u64,
        value: Vec<u8>,
        combined_signature: Vec<u8>,
    ) -> Result<(), String> {
        let message = SmrolMessage::PnfifoFinal {
            leader_id,
            sender_id: node_id,
            slot,
            value,
            combined_signature,
        };

        info!("[PNFIFO] broadcast *FINAL* for slot {}", slot);
        broadcast_tx
            .send(message)
            .await
            .map_err(|e| format!("FINAL broadcast failed: {}", e))
    }

    async fn handle_final_for_state(
        pnfifo: &Arc<Self>,
        leader_id: usize,
        slots: Arc<DashMap<u64, PnfifoSlotState>>,
        notifiers: Arc<RwLock<HashMap<u64, Arc<Notify>>>>,
        sender_id: usize,
        slot: u64,
        value: Vec<u8>,
        combined_signature: Vec<u8>,
    ) -> Result<(), String> {
        let node_id = pnfifo.node_id;

        info!(
            "[PNFIFO] Node {} received FINAL from {} for Leader {} slot {}",
            node_id, sender_id, leader_id, slot
        );

        let mut already_finalized = false;
        let proposal_received = {
            if let Some(slot_state) = slots.get(&slot) {
                if slot_state.final_stored {
                    debug!(
                        "[PNFIFO] Node {} already processed FINAL for Leader {} slot {}",
                        node_id, leader_id, slot
                    );
                    already_finalized = true;
                }
                slot_state.proposal_received
            } else {
                false
            }
        };

        if already_finalized {
            return Ok(());
        }

        debug!(
            "[PNFIFO] FINAL proposal_received = {} (leader {} slot {})",
            proposal_received, leader_id, slot
        );

        if !proposal_received {
            let mut finalize_immediately = false;
            {
                let mut entry = slots
                    .entry(slot)
                    .or_insert_with(|| PnfifoSlotState::new(pnfifo.threshold));
                let slot_state = entry.value_mut();

                if slot_state.final_stored {
                    return Ok(());
                }

                if slot_state.proposal_received {
                    finalize_immediately = true;
                } else {
                    slot_state.pending_final = Some((value.clone(), combined_signature.clone()));
                    debug!(
                        "[PNFIFO] Node {} stored pending FINAL for Leader {} slot {} (waiting for PROPOSAL)",
                        node_id, leader_id, slot
                    );
                    return Ok(());
                }
            }

            if finalize_immediately {
                return Self::finalize_with_signature_for_state(
                    pnfifo,
                    leader_id,
                    slots.clone(),
                    notifiers.clone(),
                    slot,
                    value,
                    combined_signature,
                )
                .await;
            }
        }

        Self::finalize_with_signature_for_state(
            pnfifo,
            leader_id,
            slots,
            notifiers,
            slot,
            value,
            combined_signature,
        )
        .await
    }

    async fn finalize_with_signature_for_state(
        pnfifo: &Arc<Self>,
        leader_id: usize,
        slots: Arc<DashMap<u64, PnfifoSlotState>>,
        notifiers: Arc<RwLock<HashMap<u64, Arc<Notify>>>>,
        slot: u64,
        value: Vec<u8>,
        combined_signature: Vec<u8>,
    ) -> Result<(), String> {
        let message_to_verify = PnfifoBc::create_vote_message_static(slot, &value);

        match verify_combined_signature_bytes(
            &combined_signature,
            &message_to_verify,
            &pnfifo.verifying_keys,
            pnfifo.threshold,
        ) {
            Ok(true) => {
                // debug!("[PNFIFO] Node {} slot {} FINAL signature verified", node_id, slot);

                let should_store = {
                    let mut entry = slots
                        .entry(slot)
                        .or_insert_with(|| PnfifoSlotState::new(pnfifo.threshold));
                    let slot_state = entry.value_mut();

                    if slot_state.final_stored {
                        debug!(
                            "[PNFIFO] Node {} already processed FINAL for Leader {} slot {}",
                            pnfifo.node_id, leader_id, slot
                        );
                        false
                    } else {
                        slot_state.value.get_or_insert_with(|| value.clone());
                        slot_state.pending_final = None;
                        true
                    }
                };

                if should_store {
                    Self::store_output_for_state(
                        leader_id,
                        slots,
                        notifiers,
                        pnfifo.threshold,
                        slot,
                        value,
                        combined_signature,
                    )
                    .await;
                }
            }
            Ok(false) => {
                warn!(
                    "[PNFIFO] Node {} slot {} signature verification failed",
                    pnfifo.node_id, slot
                );
            }
            Err(e) => {
                warn!(
                    "[PNFIFO] Node {} slot {} signature verification error: {}",
                    pnfifo.node_id, slot, e
                );
            }
        }

        Ok(())
    }

    pub async fn get_output(&self, leader_id: usize, slot: u64) -> Option<(Vec<u8>, Vec<u8>)> {
        self.leader_slots
            .get(leader_id)
            .and_then(|opt| opt.as_ref())
            .and_then(|slots| slots.get(&slot))
            .and_then(|state| state.output.clone())
    }

    // Sequencing 层使用：等待 output 可用（Algorithm 2 Line 5）
    pub async fn wait_for_output(&self, leader_id: usize, slot: u64) {
        let Some(slots) = self.slots_for(leader_id) else {
            warn!(
                "[PNFIFO] Node {} wait_for_output missing leader {}",
                self.node_id, leader_id
            );
            return;
        };

        if let Some(slot_state) = slots.get(&slot) {
            if slot_state.output.is_some() {
                debug!(
                    "(wait_for_output) fast-path completed. leader {} slot {}",
                    leader_id, slot
                );
                return;
            }
        }

        let Some(notifiers) = self.notifiers_for(leader_id) else {
            warn!(
                "[PNFIFO] Node {} wait_for_output missing notifier for leader {}",
                self.node_id, leader_id
            );
            return;
        };

        let notifier = {
            let mut map = notifiers.write().await;
            Arc::clone(map.entry(slot).or_insert_with(|| Arc::new(Notify::new())))
        };

        // 等待循环
        let mut attempts = 0;
        loop {
            // 1. 先创建 notified Future（关键顺序）
            let notified = notifier.notified();

            // 2. 检查 output 是否已就绪
            if let Some(slot_state) = slots.get(&slot) {
                if slot_state.output.is_some() {
                    if attempts > 0 {
                        debug!(
                            "[PNFIFO] Node {} got output for Leader {} slot {} after {} waits",
                            self.node_id, leader_id, slot, attempts
                        );
                    }
                    return;
                }
            }

            // 3. 等待通知
            attempts += 1;
            if attempts % 100 == 0 {
                warn!(
                    "[PNFIFO] Node {} still waiting for Leader {} slot {} (attempt {})",
                    self.node_id, leader_id, slot, attempts
                );
            }

            notified.await;
        }
    }

    fn predicate_q_static(_value: &[u8]) -> bool {
        true
    }

    fn create_vote_message_static(slot: u64, value: &[u8]) -> Vec<u8> {
        let mut message = Vec::new();
        message.extend_from_slice(&slot.to_be_bytes());
        message.extend_from_slice(value);
        message
    }

    async fn store_output_for_state(
        leader_id: usize,
        slots: Arc<DashMap<u64, PnfifoSlotState>>,
        notifiers: Arc<RwLock<HashMap<u64, Arc<Notify>>>>,
        threshold: usize,
        slot: u64,
        value: Vec<u8>,
        signature: Vec<u8>,
    ) {
        debug!(
            "[PNFIFO] Storing output for Leader {} slot {}",
            leader_id, slot
        );

        {
            let mut entry = slots
                .entry(slot)
                .or_insert_with(|| PnfifoSlotState::new(threshold));
            let slot_state = entry.value_mut();
            slot_state.output = Some((value, signature));
            slot_state.final_stored = true;
        }

        {
            let map = notifiers.read().await;
            if let Some(notifier) = map.get(&slot) {
                notifier.notify_waiters();
                debug!(
                    "[PNFIFO] Notified waiters for Leader {} slot {}",
                    leader_id, slot
                );
            }
        }
    }

    pub async fn get_stats(&self) -> (usize, usize, u64) {
        let mut total_slots = 0;
        let mut completed_slots = 0;

        for slots in &self.leader_slots {
            if let Some(slots_map) = slots {
                total_slots += slots_map.len();
                completed_slots += slots_map
                    .iter()
                    .filter(|entry| entry.value().output.is_some())
                    .count();
            }
        }

        let current_slot = self.current_slot.load(std::sync::atomic::Ordering::Relaxed);

        (total_slots, completed_slots, current_slot)
    }

    pub async fn cleanup_old_slots(&self, keep_recent: u64) {
        let current = self.current_slot.load(std::sync::atomic::Ordering::Relaxed);
        let threshold = current.saturating_sub(keep_recent);

        let mut cleaned_slots = 0;
        let mut cleaned_notifiers = 0;

        for &leader_id in &self.active_leaders {
            if let Some(slots) = self
                .leader_slots
                .get(leader_id)
                .and_then(|opt| opt.as_ref())
            {
                slots.retain(|slot_number, _| {
                    if *slot_number <= threshold {
                        cleaned_slots += 1;
                        false
                    } else {
                        true
                    }
                });
            }

            if let Some(notifiers) = self
                .leader_notifiers
                .get(leader_id)
                .and_then(|opt| opt.as_ref())
            {
                let mut map = notifiers.write().await;
                map.retain(|slot_number, _| {
                    if *slot_number <= threshold {
                        cleaned_notifiers += 1;
                        false
                    } else {
                        true
                    }
                });
            }
        }

        if cleaned_slots > 0 || cleaned_notifiers > 0 {
            info!(
                "[PNFIFO] Node {} cleaned {} slots and {} notifiers < {}, retained {} slots",
                self.node_id,
                cleaned_slots,
                cleaned_notifiers,
                threshold,
                self.leader_slots
                    .iter()
                    .filter_map(|opt| opt.as_ref())
                    .map(|slots| slots.len())
                    .sum::<usize>()
            );
        }
    }

    pub async fn start(self: &Arc<Self>) -> Result<(), String> {
        for &leader_id in &self.active_leaders {
            self.spawn_leader_proposal_processor(leader_id);
            self.spawn_leader_vote_processor(leader_id);
            self.spawn_leader_final_processor(leader_id);
        }

        info!("[PNFIFO] Node {} 初始化完成", self.node_id);
        Ok(())
    }

    fn spawn_leader_proposal_processor(self: &Arc<Self>, leader_id: usize) {
        let Some(slots) = self.slots_for(leader_id) else {
            warn!(
                "⚠️ [PNFIFO] Missing slot state for leader {}",
                leader_id
            );
            return;
        };
        let Some(notifiers) = self.notifiers_for(leader_id) else {
            warn!(
                "⚠️ [PNFIFO] Missing notifier state for leader {}",
                leader_id
            );
            return;
        };
        let Some(mut proposal_rx) = self.take_proposal_rx_for(leader_id) else {
            warn!(
                "⚠️ [PNFIFO] Proposal receiver already taken for leader {}",
                leader_id
            );
            return;
        };

        let pnfifo = Arc::clone(self);
        tokio::spawn(async move {
            info!(
                "[PNFIFO] Node {} proposal processor started (leader {})",
                pnfifo.node_id, leader_id
            );

            while let Some(message) = proposal_rx.recv().await {
                if let Err(e) = PnfifoBc::process_leader_proposal_for_state(
                    &pnfifo,
                    leader_id,
                    Arc::clone(&slots),
                    Arc::clone(&notifiers),
                    message.slot,
                    message.value,
                )
                .await
                {
                    warn!(
                        "⚠️ [PNFIFO] Node {} proposal handling failed (leader {} slot {}): {}",
                        pnfifo.node_id, leader_id, message.slot, e
                    );
                }
            }

            warn!(
                "⚠️ [PNFIFO] Node {} proposal processor exiting (leader {})",
                pnfifo.node_id, leader_id
            );
        });
    }

    fn spawn_leader_vote_processor(self: &Arc<Self>, leader_id: usize) {
        let Some(slots) = self.slots_for(leader_id) else {
            warn!(
                "⚠️ [PNFIFO] Missing slot state for leader {}",
                leader_id
            );
            return;
        };
        let Some(mut vote_rx) = self.take_vote_rx_for(leader_id) else {
            warn!(
                "⚠️ [PNFIFO] Vote receiver already taken for leader {}",
                leader_id
            );
            return;
        };

        let pnfifo = Arc::clone(self);
        tokio::spawn(async move {
            info!(
                "[PNFIFO] Node {} vote processor started (leader {})",
                pnfifo.node_id, leader_id
            );

            while let Some(message) = vote_rx.recv().await {
                if let Err(e) = PnfifoBc::handle_vote_for_state(
                    &pnfifo,
                    leader_id,
                    Arc::clone(&slots),
                    message.sender_id,
                    message.slot,
                    message.signature_share,
                )
                .await
                {
                    warn!(
                        "⚠️ [PNFIFO] Node {} vote handling failed (leader {} slot {}): {}",
                        pnfifo.node_id, leader_id, message.slot, e
                    );
                }
            }

            warn!(
                "⚠️ [PNFIFO] Node {} vote processor exiting (leader {})",
                pnfifo.node_id, leader_id
            );
        });
    }

    fn spawn_leader_final_processor(self: &Arc<Self>, leader_id: usize) {
        let Some(slots) = self.slots_for(leader_id) else {
            warn!(
                "⚠️ [PNFIFO] Missing slot state for leader {}",
                leader_id
            );
            return;
        };
        let Some(notifiers) = self.notifiers_for(leader_id) else {
            warn!(
                "⚠️ [PNFIFO] Missing notifier state for leader {}",
                leader_id
            );
            return;
        };
        let Some(mut final_rx) = self.take_final_rx_for(leader_id) else {
            warn!(
                "⚠️ [PNFIFO] Final receiver already taken for leader {}",
                leader_id
            );
            return;
        };

        let pnfifo = Arc::clone(self);
        tokio::spawn(async move {
            info!(
                "[PNFIFO] Node {} final processor started (leader {})",
                pnfifo.node_id, leader_id
            );

            while let Some(message) = final_rx.recv().await {
                if let Err(e) = PnfifoBc::handle_final_for_state(
                    &pnfifo,
                    leader_id,
                    Arc::clone(&slots),
                    Arc::clone(&notifiers),
                    message.sender_id,
                    message.slot,
                    message.value,
                    message.combined_signature,
                )
                .await
                {
                    warn!(
                        "⚠️ [PNFIFO] Node {} final handling failed (leader {} slot {}): {}",
                        pnfifo.node_id, leader_id, message.slot, e
                    );
                }
            }

            warn!(
                "⚠️ [PNFIFO] Node {} final processor exiting (leader {})",
                pnfifo.node_id, leader_id
            );
        });
    }
}
