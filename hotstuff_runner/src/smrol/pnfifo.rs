use crate::smrol::crypto::{
    verify_combined_signature_bytes, verify_signature_share, SmrolThresholdSig,
};
use crate::smrol::message::SmrolMessage;
use crate::smrol::network::SmrolTcpNetwork;
use dashmap::DashMap;
use ed25519_dalek::{Signer, SigningKey, VerifyingKey};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc as async_mpsc, Mutex as AsyncMutex, Notify};
use tracing::{debug, error, info, warn};

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
    slot_values: DashMap<(usize, u64), Vec<u8>>,
    slot_outputs: DashMap<(usize, u64), (Vec<u8>, Vec<u8>)>,
    slot_proposal_flags: DashMap<(usize, u64), bool>,
    slot_final_flags: DashMap<(usize, u64), bool>,
    slot_votes: DashMap<(usize, u64), HashMap<usize, Vec<u8>>>,
    slot_threshold_sigs: DashMap<(usize, u64), SmrolThresholdSig>,
    slot_pending_finals: DashMap<(usize, u64), (Vec<u8>, Vec<u8>)>,
    notifier_states: DashMap<(usize, u64), Arc<Notify>>,

    proposal_tx: async_mpsc::UnboundedSender<(usize, ProposalMsg)>,
    proposal_rx: Arc<AsyncMutex<async_mpsc::UnboundedReceiver<(usize, ProposalMsg)>>>,
    vote_tx: async_mpsc::UnboundedSender<(usize, VoteMsg)>,
    vote_rx: Arc<AsyncMutex<async_mpsc::UnboundedReceiver<(usize, VoteMsg)>>>,
    final_tx: async_mpsc::UnboundedSender<(usize, FinalMsg)>,
    final_rx: Arc<AsyncMutex<async_mpsc::UnboundedReceiver<(usize, FinalMsg)>>>,

    // 密码学
    signing_key: SigningKey,
    verifying_keys: HashMap<usize, VerifyingKey>,

    // 网络
    network: Arc<SmrolTcpNetwork>,
    broadcast_tx: async_mpsc::UnboundedSender<SmrolMessage>,
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

        let (broadcast_tx, mut broadcast_rx) = async_mpsc::unbounded_channel::<SmrolMessage>();
        let network_for_broadcast = Arc::clone(&network);
        let node_id_for_broadcast = node_id;
        network.spawn(async move {
            info!(
                "📡 [PNFIFO] Node {} 启动专用广播处理器",
                node_id_for_broadcast
            );
            while let Some(msg) = broadcast_rx.recv().await {
                let dispatch_start = Instant::now();
                let msg_kind = match &msg {
                    SmrolMessage::PnfifoProposal { slot, .. } => {
                        format!("proposal(slot={})", slot)
                    }
                    SmrolMessage::PnfifoFinal { slot, .. } => {
                        format!("final(slot={})", slot)
                    }
                    SmrolMessage::PnfifoVote { slot, .. } => {
                        format!("vote(slot={})", slot)
                    }
                    other => format!("{:?}", std::mem::discriminant(other)),
                };

                match network_for_broadcast.broadcast(msg).await {
                    Ok(_) => {
                        let elapsed = dispatch_start.elapsed();
                        if elapsed > Duration::from_millis(10) {
                            warn!(
                                "🐌 [PNFIFO] Node {} 广播 {:?} 延时较高: {:?}",
                                node_id_for_broadcast, msg_kind, elapsed
                            );
                        }
                    }
                    Err(e) => {
                        error!("❌ [PNFIFO] 广播处理器失败: {}", e);
                    }
                }
            }
            warn!("[PNFIFO] 广播处理器退出 (channel closed)");
        });

        let slot_values = DashMap::new();
        let slot_outputs = DashMap::new();
        let slot_proposal_flags = DashMap::new();
        let slot_final_flags = DashMap::new();
        let slot_votes = DashMap::new();
        let slot_threshold_sigs = DashMap::new();
        let slot_pending_finals = DashMap::new();
        let notifier_states = DashMap::new();

        let (proposal_tx, proposal_rx_raw) =
            async_mpsc::unbounded_channel::<(usize, ProposalMsg)>();
        let proposal_rx = Arc::new(AsyncMutex::new(proposal_rx_raw));
        let (vote_tx, vote_rx_raw) = async_mpsc::unbounded_channel::<(usize, VoteMsg)>();
        let vote_rx = Arc::new(AsyncMutex::new(vote_rx_raw));
        let (final_tx, final_rx_raw) = async_mpsc::unbounded_channel::<(usize, FinalMsg)>();
        let final_rx = Arc::new(AsyncMutex::new(final_rx_raw));

        Ok(Self {
            node_id,
            total_nodes,
            threshold,
            current_slot,
            slot_values,
            slot_outputs,
            slot_proposal_flags,
            slot_final_flags,
            slot_votes,
            slot_threshold_sigs,
            slot_pending_finals,
            notifier_states,
            proposal_tx,
            proposal_rx,
            vote_tx,
            vote_rx,
            final_tx,
            final_rx,
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

        let key = (leader_id, slot);

        let already_proposed = pnfifo
            .slot_proposal_flags
            .get(&key)
            .map(|flag| *flag.value())
            .unwrap_or(false);

        if already_proposed && leader_id != node_id {
            debug!(
                "[PNFIFO] Node {} already processed Leader {} slot {}",
                node_id, leader_id, slot
            );
            return Ok(());
        }

        pnfifo.slot_values.insert(key, value.clone());
        pnfifo.slot_proposal_flags.insert(key, true);

        let delayed_final = pnfifo.slot_pending_finals.remove(&key).map(|entry| entry.1);

        if let Some((final_value, final_signature)) = delayed_final {
            debug!(
                "[PNFIFO] Node {} processing delayed FINAL for Leader {} slot {}",
                node_id, leader_id, slot
            );

            Self::finalize_with_signature_for_state(
                pnfifo,
                leader_id,
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

    pub fn proposal_sender(&self) -> async_mpsc::UnboundedSender<(usize, ProposalMsg)> {
        self.proposal_tx.clone()
    }

    pub fn vote_sender(&self) -> async_mpsc::UnboundedSender<(usize, VoteMsg)> {
        self.vote_tx.clone()
    }

    pub fn final_sender(&self) -> async_mpsc::UnboundedSender<(usize, FinalMsg)> {
        self.final_tx.clone()
    }

    pub async fn broadcast(&self, slot: u64, value: Vec<u8>) -> Result<u64, String> {
        let proposal = Self::broadcast_proposal_for_leader(
            self.node_id,
            &self.current_slot,
            (self.node_id, slot),
            value,
            &self.slot_values,
            &self.slot_proposal_flags,
        )
        .await?;

        // self.broadcast_tx
        //     .send(proposal)
        //     .await
        //     .map_err(|e| format!("broadcast queue send failed: {}", e))?;

        // debug!(
        //     "[PNFIFO] Node {} enqueued slot {} for broadcast (single queue)",
        //     self.node_id, slot
        // );

        // Ok(slot)
        let enqueue_start = Instant::now();
        self.broadcast_tx
            .send(proposal)
            .map_err(|_| String::from("broadcast channel closed"))?;
        let enqueue_elapsed = enqueue_start.elapsed();
        debug!(
            "⏱️ [PNFIFO] Node {} enqueue broadcast slot {} 用时 {:?}",
            self.node_id, slot, enqueue_elapsed
        );

        // tokio::task::yield_now().await;
        Ok(slot)
    }

    fn ensure_notifier(&self, leader_id: usize, slot: u64) -> Arc<Notify> {
        let key = (leader_id, slot);
        let entry = self
            .notifier_states
            .entry(key)
            .or_insert_with(|| Arc::new(Notify::new()));
        Arc::clone(entry.value())
    }

    fn get_notifier(&self, leader_id: usize, slot: u64) -> Option<Arc<Notify>> {
        self.notifier_states
            .get(&(leader_id, slot))
            .map(|entry| Arc::clone(entry.value()))
    }

    fn remove_notifier(&self, leader_id: usize, slot: u64) {
        self.notifier_states.remove(&(leader_id, slot));
    }

    async fn broadcast_proposal_for_leader(
        node_id: usize,
        current_slot: &Arc<AtomicU64>,
        key: (usize, u64),
        value: Vec<u8>,
        slot_values: &DashMap<(usize, u64), Vec<u8>>,
        slot_proposal_flags: &DashMap<(usize, u64), bool>,
    ) -> Result<SmrolMessage, String> {
        current_slot.store(
            key.1.saturating_add(1),
            std::sync::atomic::Ordering::Relaxed,
        );

        info!(
            "[PNFIFO] Node {} broadcast *PROPOSAL* for slot {}",
            node_id, key.1
        );

        slot_values.insert(key, value.clone());
        slot_proposal_flags.insert(key, true);

        let proposal = SmrolMessage::PnfifoProposal {
            sender_id: node_id,
            slot: key.1,
            value,
        };

        Ok(proposal)
    }

    // async fn handle_vote_for_state(
    //     pnfifo: &Arc<Self>,
    //     leader_id: usize,
    //     slots: Arc<DashMap<u64, PnfifoSlotState>>,
    //     sender_id: usize,
    //     slot: u64,
    //     signature_share: Vec<u8>,
    // ) -> Result<(), String> {
    //     let node_id = pnfifo.node_id;

    //     info!(
    //         "[PNFIFO] Node {} received VOTE from {} for slot {} leader {}",
    //         node_id, sender_id, slot, leader_id
    //     );

    //     let mut should_finalize = false;
    //     let mut finalize_data = None;

    //     {
    //         let mut entry = slots
    //             .entry(slot)
    //             .or_insert_with(|| PnfifoSlotState::new(pnfifo.threshold));
    //         let slot_state = entry.value_mut();

    //         if let Some(ref value) = slot_state.value {
    //             let message_to_verify = PnfifoBc::create_vote_message_static(slot, value);

    //             if let Some(verifying_key) = pnfifo.verifying_keys.get(&sender_id) {
    //                 if verify_signature_share(&signature_share, &message_to_verify, verifying_key) {
    //                     if !slot_state.votes.contains_key(&sender_id) {
    //                         slot_state.votes.insert(sender_id, signature_share.clone());

    //                         let reached = slot_state
    //                             .threshold_sig
    //                             .add_share(sender_id, signature_share.clone());

    //                         if slot_state.votes.len() <= pnfifo.threshold {
    //                             debug!(
    //                                 "[PNFIFO] Node {} accepted VOTE from {} (slot {} leader {}), votes: {}/{}",
    //                                 node_id,
    //                                 sender_id,
    //                                 slot,
    //                                 leader_id,
    //                                 slot_state.votes.len(),
    //                                 pnfifo.threshold
    //                             );
    //                         }

    //                         if reached && !slot_state.final_broadcasted {
    //                             if let Ok(combined_sig) = slot_state.threshold_sig.combine() {
    //                                 slot_state.final_broadcasted = true;
    //                                 finalize_data = Some((value.clone(), combined_sig));
    //                                 should_finalize = true;

    //                                 info!(
    //                                     "[PNFIFO] Node {} reached threshold for slot {} leader {}",
    //                                     node_id, slot, leader_id
    //                                 );
    //                             }
    //                         }
    //                     }
    //                 } else {
    //                     warn!(
    //                         "[PNFIFO] Node {} rejected invalid signature from {}",
    //                         node_id, sender_id
    //                     );
    //                 }
    //             }
    //         }
    //     }

    //     if should_finalize {
    //         if let Some((value, combined_signature)) = finalize_data {
    //             PnfifoBc::broadcast_final(
    //                 pnfifo.broadcast_tx.clone(),
    //                 node_id,
    //                 leader_id,
    //                 slot,
    //                 value,
    //                 combined_signature,
    //             )
    //             .await?;
    //         }
    //     }

    //     Ok(())
    // }
    async fn handle_vote_for_state(
        pnfifo: &Arc<Self>,
        leader_id: usize,
        sender_id: usize,
        slot: u64,
        signature_share: Vec<u8>,
    ) -> Result<(), String> {
        let node_id = pnfifo.node_id;

        info!(
            "[PNFIFO] Node {} received VOTE from {} for slot {} leader {}",
            node_id, sender_id, slot, leader_id
        );

        let key = (leader_id, slot);

        let Some(value) = pnfifo
            .slot_values
            .get(&key)
            .map(|entry| entry.value().clone())
        else {
            debug!(
                "[PNFIFO] Node {} received VOTE before PROPOSAL for slot {} leader {}",
                node_id, slot, leader_id
            );
            return Ok(());
        };

        let message_to_verify = PnfifoBc::create_vote_message_static(slot, &value);

        let Some(verifying_key) = pnfifo.verifying_keys.get(&sender_id) else {
            warn!(
                "[PNFIFO] Node {} missing verifying key for sender {}",
                node_id, sender_id
            );
            return Ok(());
        };

        if !verify_signature_share(&signature_share, &message_to_verify, verifying_key) {
            warn!(
                "[PNFIFO] Node {} rejected invalid signature from {} for slot {} leader {}",
                node_id, sender_id, slot, leader_id
            );
            return Ok(());
        }

        let vote_count = {
            let mut votes_entry = pnfifo.slot_votes.entry(key).or_insert_with(HashMap::new);
            let votes = votes_entry.value_mut();
            if votes.contains_key(&sender_id) {
                return Ok(());
            }
            votes.insert(sender_id, signature_share.clone());
            votes.len()
        };

        if vote_count <= pnfifo.threshold {
            debug!(
                "[PNFIFO] Node {} accepted VOTE from {} (slot {} leader {}), votes: {}/{}",
                node_id, sender_id, slot, leader_id, vote_count, pnfifo.threshold
            );
        }

        let reached_threshold = {
            let mut sig_entry = pnfifo
                .slot_threshold_sigs
                .entry(key)
                .or_insert_with(|| SmrolThresholdSig::new(pnfifo.threshold));
            sig_entry
                .value_mut()
                .add_share(sender_id, signature_share.clone())
        };

        if reached_threshold {
            debug!(
                "[PNFIFO] Node {} reached threshold for Leader {} slot {} (votes={})",
                node_id, leader_id, slot, vote_count
            );

            let already_final = pnfifo
                .slot_final_flags
                .get(&key)
                .map(|flag| *flag.value())
                .unwrap_or(false)
                || pnfifo.slot_outputs.contains_key(&key);

            if already_final {
                return Ok(());
            }

            let combined_signature = if let Some(sig_entry) = pnfifo.slot_threshold_sigs.get(&key) {
                match sig_entry.combine() {
                    Ok(bytes) => Some(bytes),
                    Err(e) => {
                        warn!(
                            "⚠️ [PNFIFO] Node {} failed to combine signature for slot {} leader {}: {}",
                            node_id, slot, leader_id, e
                        );
                        None
                    }
                }
            } else {
                warn!(
                    "⚠️ [PNFIFO] Node {} missing threshold state for slot {} leader {}",
                    node_id, slot, leader_id
                );
                None
            };

            if let Some(combined_sig) = combined_signature {
                pnfifo.slot_final_flags.insert(key, true);

                PnfifoBc::broadcast_final(
                    pnfifo.broadcast_tx.clone(),
                    node_id,
                    leader_id,
                    slot,
                    value,
                    combined_sig,
                )
                .await?;
            }
        }

        Ok(())
    }

    async fn broadcast_final(
        broadcast_tx: async_mpsc::UnboundedSender<SmrolMessage>,
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
            .map_err(|e| format!("FINAL broadcast failed: {}", e))?;
        // tokio::task::yield_now().await;
        Ok(())
    }

    async fn handle_final_for_state(
        pnfifo: &Arc<Self>,
        leader_id: usize,
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

        let key = (leader_id, slot);

        if pnfifo.slot_outputs.contains_key(&key) {
            debug!(
                "[PNFIFO] Node {} already processed FINAL for Leader {} slot {}",
                node_id, leader_id, slot
            );
            return Ok(());
        }

        let proposal_received = pnfifo
            .slot_proposal_flags
            .get(&key)
            .map(|flag| *flag.value())
            .unwrap_or(false);

        if !proposal_received {
            pnfifo
                .slot_pending_finals
                .insert(key, (value.clone(), combined_signature.clone()));
            debug!(
                "[PNFIFO] Node {} stored pending FINAL for Leader {} slot {} (waiting for PROPOSAL)",
                node_id, leader_id, slot
            );
            return Ok(());
        }

        Self::finalize_with_signature_for_state(pnfifo, leader_id, slot, value, combined_signature)
            .await
    }

    // async fn finalize_with_signature_for_state(
    //     pnfifo: &Arc<Self>,
    //     leader_id: usize,
    //     slots: Arc<DashMap<u64, PnfifoSlotState>>,
    //     notifiers: Arc<RwLock<HashMap<u64, Arc<Notify>>>>,
    //     slot: u64,
    //     value: Vec<u8>,
    //     combined_signature: Vec<u8>,
    // ) -> Result<(), String> {
    //     let message_to_verify = PnfifoBc::create_vote_message_static(slot, &value);

    //     match verify_combined_signature_bytes(
    //         &combined_signature,
    //         &message_to_verify,
    //         &pnfifo.verifying_keys,
    //         pnfifo.threshold,
    //     ) {
    //         Ok(true) => {
    //             // debug!("[PNFIFO] Node {} slot {} FINAL signature verified", node_id, slot);

    //             let should_store = {
    //                 let mut entry = slots
    //                     .entry(slot)
    //                     .or_insert_with(|| PnfifoSlotState::new(pnfifo.threshold));
    //                 let slot_state = entry.value_mut();

    //                 if slot_state.final_stored {
    //                     debug!(
    //                         "[PNFIFO] Node {} already processed FINAL for Leader {} slot {}",
    //                         pnfifo.node_id, leader_id, slot
    //                     );
    //                     false
    //                 } else {
    //                     slot_state.value.get_or_insert_with(|| value.clone());
    //                     slot_state.pending_final = None;
    //                     true
    //                 }
    //             };

    //             if should_store {
    //                 Self::store_output_for_state(
    //                     leader_id,
    //                     slots,
    //                     notifiers,
    //                     pnfifo.threshold,
    //                     slot,
    //                     value,
    //                     combined_signature,
    //                 )
    //                 .await;
    //             }
    //         }
    //         Ok(false) => {
    //             warn!(
    //                 "[PNFIFO] Node {} slot {} signature verification failed",
    //                 pnfifo.node_id, slot
    //             );
    //         }
    //         Err(e) => {
    //             warn!(
    //                 "[PNFIFO] Node {} slot {} signature verification error: {}",
    //                 pnfifo.node_id, slot, e
    //             );
    //         }
    //     }

    //     Ok(())
    // }
    async fn finalize_with_signature_for_state(
        pnfifo: &Arc<Self>,
        leader_id: usize,
        slot: u64,
        value: Vec<u8>,
        combined_signature: Vec<u8>,
    ) -> Result<(), String> {
        // ✅ 在锁外验证签名（~100μs，不持锁）
        let message_to_verify = PnfifoBc::create_vote_message_static(slot, &value);

        match verify_combined_signature_bytes(
            &combined_signature,
            &message_to_verify,
            &pnfifo.verifying_keys,
            pnfifo.threshold,
        ) {
            Ok(true) => {
                let key = (leader_id, slot);

                if pnfifo.slot_outputs.contains_key(&key) {
                    debug!(
                        "[PNFIFO] Node {} already processed FINAL for Leader {} slot {}",
                        pnfifo.node_id, leader_id, slot
                    );
                    return Ok(());
                }

                pnfifo.slot_values.entry(key).or_insert(value.clone());
                pnfifo.slot_pending_finals.remove(&key);
                pnfifo
                    .slot_outputs
                    .insert(key, (value.clone(), combined_signature.clone()));
                pnfifo.slot_final_flags.insert(key, true);

                PnfifoBc::notify_slot(pnfifo, leader_id, slot);
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
        self.slot_outputs
            .get(&(leader_id, slot))
            .map(|entry| entry.value().clone())
    }

    // Sequencing 层使用：等待 output 可用（Algorithm 2 Line 5）
    pub async fn wait_for_output(&self, leader_id: usize, slot: u64) {
        let key = (leader_id, slot);

        if self.slot_outputs.get(&key).is_some() {
            debug!(
                "(wait_for_output) fast-path completed. leader {} slot {}",
                leader_id, slot
            );
            return;
        }

        let notifier = self.ensure_notifier(leader_id, slot);

        let mut attempts = 0;
        loop {
            let notified = notifier.notified();

            if self.slot_outputs.get(&key).is_some() {
                if attempts > 0 {
                    debug!(
                        "[PNFIFO] Node {} got output for Leader {} slot {} after {} waits",
                        self.node_id, leader_id, slot, attempts
                    );
                }
                return;
            }

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

    fn notify_slot(pnfifo: &Arc<Self>, leader_id: usize, slot: u64) {
        if let Some(notifier) = pnfifo.get_notifier(leader_id, slot) {
            notifier.notify_waiters();
            pnfifo.remove_notifier(leader_id, slot);
            debug!(
                "[PNFIFO] Notified waiters for Leader {} slot {}",
                leader_id, slot
            );
        }
    }

    pub async fn get_stats(&self) -> (usize, usize, u64) {
        let total_slots = self.slot_values.len();
        let completed_slots = self.slot_outputs.len();

        let current_slot = self.current_slot.load(std::sync::atomic::Ordering::Relaxed);

        (total_slots, completed_slots, current_slot)
    }

    pub async fn cleanup_old_slots(&self, keep_recent: u64) {
        let current = self.current_slot.load(std::sync::atomic::Ordering::Relaxed);
        let threshold = current.saturating_sub(keep_recent);

        let mut cleaned_values = 0;
        let mut cleaned_votes = 0;
        let mut cleaned_notifiers = 0;
        let mut cleaned_outputs = 0;
        let mut cleaned_thresholds = 0;
        let mut cleaned_pending = 0;

        self.slot_values.retain(|(_, slot_number), _| {
            if *slot_number <= threshold {
                cleaned_values += 1;
                false
            } else {
                true
            }
        });

        self.slot_outputs.retain(|(_, slot_number), _| {
            if *slot_number <= threshold {
                cleaned_outputs += 1;
                false
            } else {
                true
            }
        });

        self.slot_proposal_flags
            .retain(|(_, slot_number), _| *slot_number > threshold);
        self.slot_final_flags
            .retain(|(_, slot_number), _| *slot_number > threshold);

        self.slot_votes.retain(|(_, slot_number), _| {
            if *slot_number <= threshold {
                cleaned_votes += 1;
                false
            } else {
                true
            }
        });

        self.slot_threshold_sigs.retain(|(_, slot_number), _| {
            if *slot_number <= threshold {
                cleaned_thresholds += 1;
                false
            } else {
                true
            }
        });

        self.slot_pending_finals.retain(|(_, slot_number), _| {
            if *slot_number <= threshold {
                cleaned_pending += 1;
                false
            } else {
                true
            }
        });

        self.notifier_states.retain(|(_, slot_number), _| {
            if *slot_number <= threshold {
                cleaned_notifiers += 1;
                false
            } else {
                true
            }
        });

        if cleaned_values > 0
            || cleaned_outputs > 0
            || cleaned_votes > 0
            || cleaned_thresholds > 0
            || cleaned_pending > 0
            || cleaned_notifiers > 0
        {
            info!(
                "[PNFIFO] Node {} cleaned values={} outputs={} votes={} thresholds={} pending={} notifiers={} below {}",
                self.node_id,
                cleaned_values,
                cleaned_outputs,
                cleaned_votes,
                cleaned_thresholds,
                cleaned_pending,
                cleaned_notifiers,
                threshold
            );
        }
    }

    pub async fn start(self: &Arc<Self>) -> Result<(), String> {
        self.spawn_proposal_processor();
        self.spawn_vote_processor();
        self.spawn_final_processor();

        info!("[PNFIFO] Node {} 初始化完成", self.node_id);
        Ok(())
    }

    fn spawn_proposal_processor(self: &Arc<Self>) {
        let proposal_rx = Arc::clone(&self.proposal_rx);
        let pnfifo = Arc::clone(self);
        tokio::spawn(async move {
            info!(
                "[PNFIFO] Node {} proposal processor started",
                pnfifo.node_id
            );

            let mut count = 0;
            let mut total_time = Duration::ZERO;
            let mut max_time = Duration::ZERO;
            let mut last_log = Instant::now();

            while let Some((leader_id, message)) = {
                let mut rx = proposal_rx.lock().await;
                rx.recv().await
            } {
                count += 1;
                let start = Instant::now();
                if let Err(e) = PnfifoBc::process_leader_proposal_for_state(
                    &pnfifo,
                    leader_id,
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
                let elapsed = start.elapsed();
                if elapsed > Duration::from_millis(10) {
                    warn!(
                        "🐌 [SLOW] Proposal handler slow for leader {} slot {}: {:?}",
                        leader_id, message.slot, elapsed
                    );
                }
                total_time += elapsed;
                max_time = max_time.max(elapsed);
                if last_log.elapsed() > Duration::from_secs(1) {
                    let avg = if count > 0 {
                        total_time / count
                    } else {
                        Duration::ZERO
                    };
                    warn!(
                        "📊 [Critical] Proposal stats: {} msgs, avg={:?}, max={:?}, rate={}/s",
                        count, avg, max_time, count
                    );
                    count = 0;
                    total_time = Duration::ZERO;
                    max_time = Duration::ZERO;
                    last_log = Instant::now();
                }
            }

            warn!(
                "⚠️ [PNFIFO] Node {} proposal processor exiting",
                pnfifo.node_id
            );
        });
    }

    fn spawn_vote_processor(self: &Arc<Self>) {
        let vote_rx = Arc::clone(&self.vote_rx);
        let pnfifo = Arc::clone(self);
        tokio::spawn(async move {
            info!("[PNFIFO] Node {} vote processor started", pnfifo.node_id);

            let mut count = 0;
            let mut total_time = Duration::ZERO;
            let mut max_time = Duration::ZERO;
            let mut last_log = Instant::now();

            while let Some((leader_id, message)) = {
                let mut rx = vote_rx.lock().await;
                rx.recv().await
            } {
                count += 1;
                let start = Instant::now();
                if let Err(e) = PnfifoBc::handle_vote_for_state(
                    &pnfifo,
                    leader_id,
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
                let elapsed = start.elapsed();
                if elapsed > Duration::from_millis(10) {
                    warn!(
                        "🐌 [SLOW] Vote handler slow for leader {} slot {}: {:?}",
                        leader_id, message.slot, elapsed
                    );
                }
                total_time += elapsed;
                max_time = max_time.max(elapsed);
                if last_log.elapsed() > Duration::from_secs(1) {
                    let avg = if count > 0 {
                        total_time / count
                    } else {
                        Duration::ZERO
                    };
                    warn!(
                        "📊 [Critical] Vote stats: {} msgs, avg={:?}, max={:?}, rate={}/s",
                        count, avg, max_time, count
                    );
                    count = 0;
                    total_time = Duration::ZERO;
                    max_time = Duration::ZERO;
                    last_log = Instant::now();
                }
            }

            warn!("⚠️ [PNFIFO] Node {} vote processor exiting", pnfifo.node_id);
        });
    }

    fn spawn_final_processor(self: &Arc<Self>) {
        let final_rx = Arc::clone(&self.final_rx);
        let pnfifo = Arc::clone(self);
        tokio::spawn(async move {
            info!("[PNFIFO] Node {} final processor started", pnfifo.node_id);

            let mut count = 0;
            let mut total_time = Duration::ZERO;
            let mut max_time = Duration::ZERO;
            let mut last_log = Instant::now();

            while let Some((leader_id, message)) = {
                let mut rx = final_rx.lock().await;
                rx.recv().await
            } {
                count += 1;
                let start = Instant::now();
                if let Err(e) = PnfifoBc::handle_final_for_state(
                    &pnfifo,
                    leader_id,
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
                let elapsed = start.elapsed();
                if elapsed > Duration::from_millis(10) {
                    warn!(
                        "🐌 [SLOW] Final handler slow for leader {} slot {}: {:?}",
                        leader_id, message.slot, elapsed
                    );
                }
                total_time += elapsed;
                max_time = max_time.max(elapsed);
                if last_log.elapsed() > Duration::from_secs(1) {
                    let avg = if count > 0 {
                        total_time / count
                    } else {
                        Duration::ZERO
                    };
                    warn!(
                        "📊 [Critical] Final stats: {} msgs, avg={:?}, max={:?}, rate={}/s",
                        count, avg, max_time, count
                    );
                    count = 0;
                    total_time = Duration::ZERO;
                    max_time = Duration::ZERO;
                    last_log = Instant::now();
                }
            }

            warn!(
                "⚠️ [PNFIFO] Node {} final processor exiting",
                pnfifo.node_id
            );
        });
    }
}
