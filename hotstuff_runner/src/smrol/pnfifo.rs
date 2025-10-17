use crate::smrol::crypto::{
    verify_combined_signature_bytes, verify_signature_share, SmrolThresholdSig,
};
use crate::smrol::message::SmrolMessage;
use crate::smrol::network::{SmrolNetworkMessage, SmrolTcpNetwork};
use ed25519_dalek::{Signer, SigningKey, VerifyingKey};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc,
};
use tokio::{
    sync::{mpsc, Notify, RwLock},
    time::{sleep, Duration},
};
use tracing::{debug, error, info, warn};
use tracing_subscriber::field::debug;
use uuid::Uuid;

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
#[derive(Debug, Clone)]
struct ProposalTask {
    slot: u64,
    value: Vec<u8>,
}

#[derive(Debug)]
struct VoteTask {
    leader_id: usize,
    sender_id: usize,
    slot: u64,
    signature_share: Vec<u8>,
}

#[derive(Debug)]
struct FinalTask {
    leader_id: usize,
    sender_id: usize,
    slot: u64,
    value: Vec<u8>,
    combined_signature: Vec<u8>,
}

#[derive(Debug)]
pub struct PnfifoBc {
    node_id: usize,
    total_nodes: usize,
    threshold: usize,

    // 算法状态
    current_slot: Arc<AtomicU64>,
    slots: Arc<RwLock<HashMap<(usize, u64), PnfifoSlotState>>>,
    slot_output_notifiers: Arc<RwLock<HashMap<(usize, u64), Arc<Notify>>>>,

    // 密码学
    signing_key: SigningKey,
    verifying_keys: HashMap<usize, VerifyingKey>,

    // 网络
    network: Arc<SmrolTcpNetwork>,

    // 每个 leader 的专用通道发送端
    leader_proposal_senders: HashMap<usize, mpsc::UnboundedSender<ProposalTask>>,
    broadcast_tx: mpsc::Sender<(u64, Vec<u8>)>,
    vote_tx: mpsc::Sender<VoteTask>,
    final_tx: mpsc::Sender<FinalTask>,
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
            .await
            .map_err(|e| format!("Failed to start PNFIFO network: {}", e))?;

        let slots = Arc::new(RwLock::new(HashMap::new()));
        let slot_output_notifiers = Arc::new(RwLock::new(HashMap::new()));
        let current_slot = Arc::new(AtomicU64::new(1));

        let mut leader_proposal_senders = HashMap::new();

        // 为每个 leader 创建专用处理通道
        for &leader_id in verifying_keys.keys() {
            let (tx, mut rx) = mpsc::unbounded_channel::<ProposalTask>();

            let node_id_clone = node_id;
            let slots_clone = Arc::clone(&slots);
            let slot_output_notifiers_clone = Arc::clone(&slot_output_notifiers);
            let signing_key_clone = signing_key.clone();
            let verifying_keys_clone = verifying_keys.clone();
            let network_clone = Arc::clone(&network);
            let threshold_clone = threshold;

            // 为这个 leader 启动专用处理任务
            tokio::spawn(async move {
                info!(
                    "[PNFIFO] Node {} started dedicated task for Leader {}",
                    node_id_clone, leader_id
                );

                while let Some(proposal) = rx.recv().await {
                    if let Err(e) = Self::process_leader_proposal(
                        node_id_clone,
                        &slots_clone,
                        &slot_output_notifiers_clone,
                        &signing_key_clone,
                        &verifying_keys_clone,
                        &network_clone,
                        threshold_clone,
                        leader_id,
                        proposal.slot,
                        proposal.value,
                    )
                    .await
                    {
                        error!(
                            "[PNFIFO] Failed to process Leader {} proposal: {}",
                            leader_id, e
                        );
                    }
                }

                warn!("[PNFIFO] Leader {} processing task exited", leader_id);
            });

            leader_proposal_senders.insert(leader_id, tx);
        }

        let (broadcast_tx, mut broadcast_rx) =
            mpsc::channel::<(u64, Vec<u8>)>(Self::broadcast_queue_capacity());
        let broadcast_workers = Self::broadcast_worker_count();
        let slots_for_broadcast = Arc::clone(&slots);
        let current_slot_for_broadcast = Arc::clone(&current_slot);
        let network_for_broadcast = Arc::clone(&network);
        let node_id_for_broadcast = node_id;
        let threshold_for_broadcast = threshold;

        let mut broadcast_worker_txs = Vec::with_capacity(broadcast_workers);
        for worker_id in 0..broadcast_workers {
            let (worker_tx, mut worker_rx) =
                mpsc::channel::<(u64, Vec<u8>)>(Self::broadcast_queue_capacity());
            broadcast_worker_txs.push(worker_tx);

            let slots_for_worker = Arc::clone(&slots_for_broadcast);
            let current_slot_worker = Arc::clone(&current_slot_for_broadcast);
            let network_worker = Arc::clone(&network_for_broadcast);
            tokio::spawn(async move {
                info!(
                    "[PNFIFO] Node {} broadcast worker {} started (capacity: {})",
                    node_id_for_broadcast,
                    worker_id,
                    Self::broadcast_queue_capacity()
                );

                while let Some((slot, value)) = worker_rx.recv().await {
                    if let Err(e) = Self::broadcast_proposal_static(
                        node_id_for_broadcast,
                        &current_slot_worker,
                        &slots_for_worker,
                        threshold_for_broadcast,
                        &network_worker,
                        slot,
                        value,
                    )
                    .await
                    {
                        error!(
                            "[PNFIFO] Node {} broadcast worker {} failed to process slot {}: {}",
                            node_id_for_broadcast,
                            worker_id,
                            slot,
                            e
                        );
                    }
                }

                warn!(
                    "[PNFIFO] Node {} broadcast worker {} exited (channel closed)",
                    node_id_for_broadcast,
                    worker_id
                );
            });
        }

        let broadcast_senders = Arc::new(broadcast_worker_txs);
        let broadcast_index = Arc::new(AtomicUsize::new(0));
        let senders_clone = Arc::clone(&broadcast_senders);
        tokio::spawn(async move {
            while let Some(task) = broadcast_rx.recv().await {
                let len = senders_clone.len();
                let idx = broadcast_index.fetch_add(1, Ordering::Relaxed) % len.max(1);
                if let Err(e) = senders_clone[idx].send(task).await {
                    warn!(
                        "[PNFIFO] Node {} broadcast dispatcher failed to enqueue task to worker {}: {}",
                        node_id_for_broadcast,
                        idx,
                        e
                    );
                }
            }

            warn!(
                "[PNFIFO] Node {} broadcast dispatcher exited (channel closed)",
                node_id_for_broadcast
            );
        });

        let (vote_tx, mut vote_rx) = mpsc::channel::<VoteTask>(Self::vote_queue_capacity());
        let vote_workers = Self::vote_worker_count();
        let slots_for_vote = Arc::clone(&slots);
        let notif_for_vote = Arc::clone(&slot_output_notifiers);
        let network_for_vote = Arc::clone(&network);
        let verifying_keys_for_vote = Arc::new(verifying_keys.clone());
        let threshold_for_vote = threshold;
        let node_id_for_vote = node_id;

        let mut vote_worker_txs = Vec::with_capacity(vote_workers);
        for worker_id in 0..vote_workers {
            let (worker_tx, mut worker_rx) =
                mpsc::channel::<VoteTask>(Self::vote_queue_capacity());
            vote_worker_txs.push(worker_tx);

            let slots_worker = Arc::clone(&slots_for_vote);
            let notif_worker = Arc::clone(&notif_for_vote);
            let network_worker = Arc::clone(&network_for_vote);
            let verifying_worker = Arc::clone(&verifying_keys_for_vote);
            tokio::spawn(async move {
                info!(
                    "[PNFIFO] Node {} vote worker {} started (capacity: {})",
                    node_id_for_vote,
                    worker_id,
                    Self::vote_queue_capacity()
                );

                while let Some(task) = worker_rx.recv().await {
                    if let Err(e) = PnfifoBc::handle_vote_static(
                        node_id_for_vote,
                        &slots_worker,
                        &notif_worker,
                        threshold_for_vote,
                        &*verifying_worker,
                        &network_worker,
                        task.leader_id,
                        task.sender_id,
                        task.slot,
                        task.signature_share,
                    )
                    .await
                    {
                        error!(
                            "[PNFIFO] Node {} vote worker {} failed slot {} leader {}: {}",
                            node_id_for_vote,
                            worker_id,
                            task.slot,
                            task.leader_id,
                            e
                        );
                    }
                }

                warn!(
                    "[PNFIFO] Node {} vote worker {} exited (channel closed)",
                    node_id_for_vote,
                    worker_id
                );
            });
        }

        let vote_senders = Arc::new(vote_worker_txs);
        let vote_index = Arc::new(AtomicUsize::new(0));
        let vote_senders_clone = Arc::clone(&vote_senders);
        tokio::spawn(async move {
            while let Some(task) = vote_rx.recv().await {
                let len = vote_senders_clone.len();
                let idx = vote_index.fetch_add(1, Ordering::Relaxed) % len.max(1);
                if let Err(e) = vote_senders_clone[idx].send(task).await {
                    warn!(
                        "[PNFIFO] Node {} vote dispatcher failed enqueue to worker {}: {}",
                        node_id_for_vote,
                        idx,
                        e
                    );
                }
            }

            warn!(
                "[PNFIFO] Node {} vote dispatcher exited (channel closed)",
                node_id_for_vote
            );
        });

        let (final_tx, mut final_rx) = mpsc::channel::<FinalTask>(Self::final_queue_capacity());
        let final_workers = Self::final_worker_count();
        let slots_for_final = Arc::clone(&slots);
        let notif_for_final = Arc::clone(&slot_output_notifiers);
        let verifying_keys_for_final = Arc::new(verifying_keys.clone());
        let threshold_for_final = threshold;
        let node_id_for_final = node_id;

        let mut final_worker_txs = Vec::with_capacity(final_workers);
        for worker_id in 0..final_workers {
            let (worker_tx, mut worker_rx) =
                mpsc::channel::<FinalTask>(Self::final_queue_capacity());
            final_worker_txs.push(worker_tx);

            let slots_worker = Arc::clone(&slots_for_final);
            let notif_worker = Arc::clone(&notif_for_final);
            let verifying_worker = Arc::clone(&verifying_keys_for_final);
            tokio::spawn(async move {
                info!(
                    "[PNFIFO] Node {} final worker {} started (capacity: {})",
                    node_id_for_final,
                    worker_id,
                    Self::final_queue_capacity()
                );

                while let Some(task) = worker_rx.recv().await {
                    if let Err(e) = PnfifoBc::handle_final_static(
                        node_id_for_final,
                        &slots_worker,
                        &notif_worker,
                        threshold_for_final,
                        &*verifying_worker,
                        task.leader_id,
                        task.sender_id,
                        task.slot,
                        task.value,
                        task.combined_signature,
                    )
                    .await
                    {
                        error!(
                            "[PNFIFO] Node {} final worker {} failed slot {} leader {}: {}",
                            node_id_for_final,
                            worker_id,
                            task.slot,
                            task.leader_id,
                            e
                        );
                    }
                }

                warn!(
                    "[PNFIFO] Node {} final worker {} exited (channel closed)",
                    node_id_for_final,
                    worker_id
                );
            });
        }

        let final_senders = Arc::new(final_worker_txs);
        let final_index = Arc::new(AtomicUsize::new(0));
        let final_senders_clone = Arc::clone(&final_senders);
        tokio::spawn(async move {
            while let Some(task) = final_rx.recv().await {
                let len = final_senders_clone.len();
                let idx = final_index.fetch_add(1, Ordering::Relaxed) % len.max(1);
                if let Err(e) = final_senders_clone[idx].send(task).await {
                    warn!(
                        "[PNFIFO] Node {} final dispatcher failed enqueue to worker {}: {}",
                        node_id_for_final,
                        idx,
                        e
                    );
                }
            }

            warn!(
                "[PNFIFO] Node {} final dispatcher exited (channel closed)",
                node_id_for_final
            );
        });

        Ok(Self {
            node_id,
            total_nodes,
            threshold,
            current_slot,
            slots,
            slot_output_notifiers,
            signing_key,
            verifying_keys,
            network,
            leader_proposal_senders,
            broadcast_tx,
            vote_tx,
            final_tx,
        })
    }

    // PNFIFO 内部：处理 leader 的 proposal（在专用通道任务中运行）
    async fn process_leader_proposal(
        node_id: usize,
        slots: &Arc<RwLock<HashMap<(usize, u64), PnfifoSlotState>>>,
        slot_output_notifiers: &Arc<RwLock<HashMap<(usize, u64), Arc<Notify>>>>,
        signing_key: &SigningKey,
        verifying_keys: &HashMap<usize, VerifyingKey>,
        network: &Arc<SmrolTcpNetwork>,
        threshold: usize,
        leader_id: usize,
        slot: u64,
        value: Vec<u8>,
    ) -> Result<(), String> {
        info!(
            "[PNFIFO] Node {} received PROPOSAL Leader {} slot {}",
            node_id, leader_id, slot
        );

        // 检查是否有延迟的 FINAL
        let delayed_final = {
            let mut slots_guard = slots.write().await;
            let slot_state = slots_guard
                .entry((leader_id, slot))
                .or_insert_with(|| PnfifoSlotState::new(threshold));

            // 去重检查
            if slot_state.proposal_received && leader_id != node_id {
                debug!(
                    "[PNFIFO] Node {} already processed Leader {} slot {}",
                    node_id, leader_id, slot
                );
                return Ok(());
            }

            slot_state.proposal_received = true;
            slot_state.value = Some(value.clone());

            // 取出可能存在的 pending_final
            slot_state.pending_final.take()
        };

        // 如果有延迟的 FINAL，先处理它
        if let Some((final_value, final_signature)) = delayed_final {
            debug!(
                "[PNFIFO] Node {} processing delayed FINAL for Leader {} slot {}",
                node_id, leader_id, slot
            );

            Self::finalize_with_signature_static(
                node_id,
                slots,
                slot_output_notifiers,
                threshold,
                verifying_keys,
                leader_id,
                slot,
                final_value,
                final_signature,
            )
            .await?;

            return Ok(()); // FINAL 已处理，不需要发 VOTE
        }

        // 生成并发送 VOTE
        let message_to_sign = Self::create_vote_message_static(slot, &value);
        let signature_share = signing_key.sign(&message_to_sign).to_bytes().to_vec();

        let vote_msg = SmrolMessage::PnfifoVote {
            leader_id,
            sender_id: node_id,
            slot,
            signature_share,
        };

        let network_msg = SmrolNetworkMessage {
            from_node_id: node_id,
            to_node_id: Some(leader_id),
            message: vote_msg,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64,
            message_id: format!(
                "pnfifo-vote:{}:{}:{}:{}",
                node_id,
                leader_id,
                slot,
                Uuid::new_v4()
            ),
        };

        network
            .send_message(network_msg)
            .await
            .map_err(|e| format!("Failed to send VOTE: {}", e))?;

        info!(
            "[PNFIFO] Node {} sent *VOTE* for Leader {} slot {}",
            node_id, leader_id, slot
        );

        // PNFIFO 内部等待：等待 FINAL 到达（实现 flag_s 语义）
        debug!(
            "[PNFIFO] Node {} 开始 waiting for FINAL for Leader {} slot {}",
            node_id, leader_id, slot
        );
        // Self::wait_for_final_internal(node_id, slots, slot_output_notifiers, leader_id, slot).await;
        debug!(
            "[PNFIFO] Node {} 结束 waiting for FINAL for Leader {} slot {}",
            node_id, leader_id, slot
        );

        // debug!("[PNFIFO] Node {} completed processing Leader {} slot {}",
        //     node_id, leader_id, slot);

        Ok(())
    }

    // PNFIFO 内部使用：等待 FINAL 处理完成（对应 Algorithm 1 的 flag_s = 0）
    async fn wait_for_final_internal(
        node_id: usize,
        slots: &Arc<RwLock<HashMap<(usize, u64), PnfifoSlotState>>>,
        slot_output_notifiers: &Arc<RwLock<HashMap<(usize, u64), Arc<Notify>>>>,
        leader_id: usize,
        slot: u64,
    ) {
        // Fast path: 快速检查
        {
            let slots_guard = slots.read().await;
            if let Some(slot_state) = slots_guard.get(&(leader_id, slot)) {
                if slot_state.final_stored {
                    return;
                }
            }
        }

        // Slow path: 等待通知
        let notifier = {
            let mut map = slot_output_notifiers.write().await;
            Arc::clone(
                map.entry((leader_id, slot))
                    .or_insert_with(|| Arc::new(Notify::new())),
            )
        };

        let mut attempts = 0;
        loop {
            // 先订阅
            let notified = notifier.notified();

            // 再检查 final_stored
            {
                let slots_guard = slots.read().await;
                if let Some(slot_state) = slots_guard.get(&(leader_id, slot)) {
                    if slot_state.final_stored {
                        if attempts > 0 {
                            debug!("[PNFIFO] Node {} received FINAL for Leader {} slot {} after {} waits",
                                node_id, leader_id, slot, attempts);
                        }
                        return;
                    }
                }
            }

            if attempts % 100 == 0 {
                debug!(
                    "[PNFIFO] Node {} waiting for FINAL Leader {} slot {} (attempt {})",
                    node_id, leader_id, slot, attempts
                );
            }
            attempts += 1;

            // 等待通知
            if attempts < 200 {
                notified.await;
            } else {
                tokio::select! {
                    _ = notified => { }
                    _ = tokio::time::sleep(Duration::from_secs(3)) => {
                        warn!("[PNFIFO] Node {} timeout waiting for FINAL Leader {} slot {}",
                            node_id, leader_id, slot);
                        return;
                    }
                }
            }
        }
    }

    pub fn network(&self) -> Arc<SmrolTcpNetwork> {
        Arc::clone(&self.network)
    }

    pub async fn start(&self) -> Result<(), String> {
        self.start_network_listener().await;
        info!("[PNFIFO] Node {} network listener started", self.node_id);
        Ok(())
    }

    async fn start_network_listener(&self) {
        let pnfifo_rx = self.network.get_pnfifo_receiver();
        let node_id = self.node_id;
        let leader_proposal_senders = self.leader_proposal_senders.clone();
        let vote_tx = self.vote_tx.clone();
        let final_tx = self.final_tx.clone();

        tokio::spawn(async move {
            info!("[PNFIFO] Node {} network listener started", node_id);

            let mut rx = pnfifo_rx.lock().await;

            while let Some((sender_id, message)) = rx.recv().await {
                match message {
                    SmrolMessage::PnfifoProposal {
                        sender_id: prop_sender,
                        slot,
                        value,
                    } => {
                        // debug!("[PNFIFO] Node {} received PROPOSAL from {} slot {}",
                        //     node_id, prop_sender, slot);

                        // 检查 Q(v)
                        if !PnfifoBc::predicate_q_static(&value) {
                            debug!(
                                "[PNFIFO] Node {} rejected slot {}: Q(v) not satisfied",
                                node_id, slot
                            );
                            continue;
                        }

                        // 发送到该 leader 的专用通道
                        if let Some(tx) = leader_proposal_senders.get(&prop_sender) {
                            if let Err(e) = tx.send(ProposalTask { slot, value }) {
                                error!(
                                    "[PNFIFO] Failed to send to Leader {} channel: {}",
                                    prop_sender, e
                                );
                            }
                        } else {
                            warn!("[PNFIFO] No channel for Leader {}", prop_sender);
                        }
                    }

                    SmrolMessage::PnfifoVote {
                        leader_id,
                        sender_id: vote_sender,
                        slot,
                        signature_share,
                    } => {
                        if let Err(e) = vote_tx
                            .send(VoteTask {
                                leader_id,
                                sender_id: vote_sender,
                                slot,
                                signature_share,
                            })
                            .await
                        {
                            error!(
                                "[PNFIFO] Failed to enqueue vote for leader {}, slot {}: {}",
                                leader_id, slot, e
                            );
                        }
                    }

                    SmrolMessage::PnfifoFinal {
                        leader_id,
                        sender_id: final_sender,
                        slot,
                        value,
                        combined_signature,
                    } => {
                        if let Err(e) = final_tx
                            .send(FinalTask {
                                leader_id,
                                sender_id: final_sender,
                                slot,
                                value,
                                combined_signature,
                            })
                            .await
                        {
                            error!(
                                "[PNFIFO] Failed to enqueue final for leader {}, slot {}: {}",
                                leader_id, slot, e
                            );
                        }
                    }

                    _ => {
                        warn!("[PNFIFO] Received non-PNFIFO message");
                    }
                }
            }
        });
    }

    pub async fn broadcast(&self, slot: u64, value: Vec<u8>) -> Result<u64, String> {
        self.broadcast_tx
            .send((slot, value))
            .await
            .map_err(|e| format!("broadcast queue send failed: {}", e))?;

        debug!(
            "[PNFIFO] Node {} enqueued slot {} for broadcast (queue)",
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

    fn broadcast_worker_count() -> usize {
        std::env::var("PNFIFO_BROADCAST_WORKERS")
            .ok()
            .and_then(|v| v.parse().ok())
            .map(|n: usize| n.max(1))
            .unwrap_or(2)
    }

    fn vote_worker_count() -> usize {
        std::env::var("PNFIFO_VOTE_WORKERS")
            .ok()
            .and_then(|v| v.parse().ok())
            .map(|n: usize| n.max(1))
            .unwrap_or(4)
    }

    fn final_worker_count() -> usize {
        std::env::var("PNFIFO_FINAL_WORKERS")
            .ok()
            .and_then(|v| v.parse().ok())
            .map(|n: usize| n.max(1))
            .unwrap_or(4)
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

    async fn broadcast_proposal_static(
        node_id: usize,
        current_slot: &Arc<AtomicU64>,
        slots: &Arc<RwLock<HashMap<(usize, u64), PnfifoSlotState>>>,
        threshold: usize,
        network: &Arc<SmrolTcpNetwork>,
        slot: u64,
        value: Vec<u8>,
    ) -> Result<(), String> {
        current_slot.store(slot.saturating_add(1), std::sync::atomic::Ordering::Relaxed);

        info!(
            "[PNFIFO] Node {} broadcast *PROPOSAL* for slot {}",
            node_id, slot
        );

        {
            let mut slots_guard = slots.write().await;
            let slot_state = slots_guard
                .entry((node_id, slot))
                .or_insert_with(|| PnfifoSlotState::new(threshold));
            slot_state.value = Some(value.clone());
            slot_state.proposal_received = true;
        }

        let proposal = SmrolMessage::PnfifoProposal {
            sender_id: node_id,
            slot,
            value: value.clone(),
        };

        let network_msg = SmrolNetworkMessage {
            from_node_id: node_id,
            to_node_id: None,
            message: proposal,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64,
            message_id: format!("pnfifo-proposal:{}:{}:{}", node_id, slot, Uuid::new_v4()),
        };

        network
            .send_message(network_msg)
            .await
            .map_err(|e| format!("PROPOSAL broadcast failed: {}", e))?;

        debug!(
            "[PNFIFO] Node {} completed slot {} PROPOSAL broadcast (worker)",
            node_id, slot
        );

        Ok(())
    }

    async fn handle_vote_static(
        node_id: usize,
        slots: &Arc<RwLock<HashMap<(usize, u64), PnfifoSlotState>>>,
        slot_output_notifiers: &Arc<RwLock<HashMap<(usize, u64), Arc<Notify>>>>,
        threshold: usize,
        verifying_keys: &HashMap<usize, VerifyingKey>,
        network: &Arc<SmrolTcpNetwork>,
        leader_id: usize,
        sender_id: usize,
        slot: u64,
        signature_share: Vec<u8>,
    ) -> Result<(), String> {
        info!(
            "[PNFIFO] Node {} received VOTE from {} for slot {} leader {}",
            node_id, sender_id, slot, leader_id
        );

        let mut should_finalize = false;
        let mut finalize_data = None;

        {
            let mut slots_guard = slots.write().await;
            if let Some(slot_state) = slots_guard.get_mut(&(leader_id, slot)) {
                if let Some(ref value) = slot_state.value {
                    let message_to_verify = PnfifoBc::create_vote_message_static(slot, value);

                    if let Some(verifying_key) = verifying_keys.get(&sender_id) {
                        if verify_signature_share(
                            &signature_share,
                            &message_to_verify,
                            verifying_key,
                        ) {
                            if !slot_state.votes.contains_key(&sender_id) {
                                slot_state.votes.insert(sender_id, signature_share.clone());

                                let reached = slot_state
                                    .threshold_sig
                                    .add_share(sender_id, signature_share.clone());

                                if slot_state.votes.len() <= threshold {
                                    debug!(
                                        "[PNFIFO] Node {} accepted VOTE from {} (slot {} leader {}), votes: {}/{}",
                                        node_id, sender_id, slot, leader_id,
                                        slot_state.votes.len(), threshold
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
        }

        if should_finalize {
            if let Some((value, combined_signature)) = finalize_data {
                PnfifoBc::broadcast_final(
                    Arc::clone(network),
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
        network: Arc<SmrolTcpNetwork>,
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

        let network_msg = SmrolNetworkMessage {
            from_node_id: node_id,
            to_node_id: None,
            message,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64,
            message_id: format!("pnfifo-final:{}:{}:{}", node_id, slot, Uuid::new_v4()),
        };

        // let result = network.send_message(network_msg).await;
        info!("[PNFIFO] broadcast *FINAL* for slot {}", slot);
        network.send_message(network_msg).await
    }

    async fn handle_final_static(
        node_id: usize,
        slots: &Arc<RwLock<HashMap<(usize, u64), PnfifoSlotState>>>,
        slot_output_notifiers: &Arc<RwLock<HashMap<(usize, u64), Arc<Notify>>>>,
        threshold: usize,
        verifying_keys: &HashMap<usize, VerifyingKey>,
        leader_id: usize,
        sender_id: usize,
        slot: u64,
        value: Vec<u8>,
        combined_signature: Vec<u8>,
    ) -> Result<(), String> {
        info!(
            "[PNFIFO] Node {} received FINAL from {} for Leader {} slot {}",
            node_id, sender_id, leader_id, slot
        );

        // 检查是否已收到 PROPOSAL
        let proposal_received = {
            let slots_guard = slots.read().await;
            if let Some(slot_state) = slots_guard.get(&(leader_id, slot)) {
                if slot_state.final_stored {
                    debug!(
                        "[PNFIFO] Node {} already processed FINAL for Leader {} slot {}",
                        node_id, leader_id, slot
                    );
                    return Ok(());
                }
                slot_state.proposal_received
            } else {
                false
            }
        };
        debug!(
            "[PNFIFO] FINAL proposal_received = {} (leader {} slot {})",
            proposal_received, leader_id, slot
        );

        if !proposal_received {
            // FINAL 先于 PROPOSAL 到达，暂存
            let mut slots_guard = slots.write().await;
            let slot_state = slots_guard
                .entry((leader_id, slot))
                .or_insert_with(|| PnfifoSlotState::new(threshold));

            if slot_state.final_stored {
                return Ok(());
            }

            if slot_state.proposal_received {
                drop(slots_guard);
                return Self::finalize_with_signature_static(
                    node_id,
                    slots,
                    slot_output_notifiers,
                    threshold,
                    verifying_keys,
                    leader_id,
                    slot,
                    value,
                    combined_signature,
                )
                .await;
            }

            slot_state.pending_final = Some((value, combined_signature));
            debug!(
                "[PNFIFO] Node {} stored pending FINAL for Leader {} slot {} (waiting for PROPOSAL)",
                node_id, leader_id, slot
            );
            return Ok(());
        }

        // PROPOSAL 已收到，直接 finalize
        Self::finalize_with_signature_static(
            node_id,
            slots,
            slot_output_notifiers,
            threshold,
            verifying_keys,
            leader_id,
            slot,
            value,
            combined_signature,
        )
        .await
    }

    async fn finalize_with_signature_static(
        node_id: usize,
        slots: &Arc<RwLock<HashMap<(usize, u64), PnfifoSlotState>>>,
        slot_output_notifiers: &Arc<RwLock<HashMap<(usize, u64), Arc<Notify>>>>,
        threshold: usize,
        verifying_keys: &HashMap<usize, VerifyingKey>,
        leader_id: usize,
        slot: u64,
        value: Vec<u8>,
        combined_signature: Vec<u8>,
    ) -> Result<(), String> {
        let message_to_verify = PnfifoBc::create_vote_message_static(slot, &value);

        match verify_combined_signature_bytes(
            &combined_signature,
            &message_to_verify,
            verifying_keys,
            threshold,
        ) {
            Ok(true) => {
                // debug!("[PNFIFO] Node {} slot {} FINAL signature verified", node_id, slot);

                let should_store = {
                    let mut slots_guard = slots.write().await;
                    let slot_state = slots_guard
                        .entry((leader_id, slot))
                        .or_insert_with(|| PnfifoSlotState::new(threshold));

                    if slot_state.final_stored {
                        debug!(
                            "[PNFIFO] Node {} already processed FINAL for Leader {} slot {}",
                            node_id, leader_id, slot
                        );
                        // drop(slots_guard);
                        false
                    } else {
                        slot_state.value.get_or_insert_with(|| value.clone());
                        slot_state.pending_final = None;
                        true
                    }
                };

                if should_store {
                    Self::store_output_static(
                        slots,
                        slot_output_notifiers,
                        threshold,
                        leader_id,
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
                    node_id, slot
                );
            }
            Err(e) => {
                warn!(
                    "[PNFIFO] Node {} slot {} signature verification error: {}",
                    node_id, slot, e
                );
            }
        }

        Ok(())
    }

    pub async fn get_output(&self, leader_id: usize, slot: u64) -> Option<(Vec<u8>, Vec<u8>)> {
        let slots = self.slots.read().await;
        slots
            .get(&(leader_id, slot))
            .and_then(|state| state.output.clone())
    }

    // Sequencing 层使用：等待 output 可用（Algorithm 2 Line 5）
    pub async fn wait_for_output(&self, leader_id: usize, slot: u64) {
        // Fast path: 先快速检查
        {
            let slots = self.slots.read().await;
            if let Some(slot_state) = slots.get(&(leader_id, slot)) {
                if slot_state.output.is_some() {
                    debug!(
                        "(wait_for_output) fast-path completed. leader {} slot {}",
                        leader_id, slot
                    );
                    return;
                }
            }
        }

        // Slow path: 获取或创建 notifier
        let notifier = {
            let mut map = self.slot_output_notifiers.write().await;
            Arc::clone(
                map.entry((leader_id, slot))
                    .or_insert_with(|| Arc::new(Notify::new())),
            )
        };

        // 等待循环
        let mut attempts = 0;
        loop {
            // 1. 先创建 notified Future（关键顺序）
            let notified = notifier.notified();

            // 2. 检查 output 是否已就绪
            {
                let slots = self.slots.read().await;
                if let Some(slot_state) = slots.get(&(leader_id, slot)) {
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

    async fn store_output_static(
        slots: &Arc<RwLock<HashMap<(usize, u64), PnfifoSlotState>>>,
        slot_output_notifiers: &Arc<RwLock<HashMap<(usize, u64), Arc<Notify>>>>,
        threshold: usize,
        leader_id: usize,
        slot: u64,
        value: Vec<u8>,
        signature: Vec<u8>,
    ) {
        debug!(
            "[PNFIFO] Storing output for Leader {} slot {}",
            leader_id, slot
        );

        // store output
        {
            let mut guard = slots.write().await;
            let slot_state = guard
                .entry((leader_id, slot))
                .or_insert_with(|| PnfifoSlotState::new(threshold));
            slot_state.output = Some((value, signature));
            slot_state.final_stored = true;
        }

        // 唤醒所有等待这个 output 的任务
        // 包括：wait_for_final_internal 和 wait_for_output
        // let notifier = {
        //     let mut map = slot_output_notifiers.write().await;
        //     Arc::clone(
        //         map.entry((leader_id, slot))
        //             .or_insert_with(|| Arc::new(Notify::new())),
        //     )
        // };
        // notifier.notify_waiters();
        // notify waiters
        {
            let map = slot_output_notifiers.read().await; // ✅ 用 read 锁
            if let Some(notifier) = map.get(&(leader_id, slot)) {
                notifier.notify_waiters();
                debug!(
                    "[PNFIFO] Notified waiters for Leader {} slot {}",
                    leader_id, slot
                );
            }
        }

        // debug!("[PNFIFO] Notified waiters for Leader {} slot {}", leader_id, slot);
    }

    pub async fn get_stats(&self) -> (usize, usize, u64) {
        let slots = self.slots.read().await;
        let total_slots = slots.len();
        let completed_slots = slots.values().filter(|s| s.output.is_some()).count();
        let current_slot = self.current_slot.load(std::sync::atomic::Ordering::Relaxed);

        (total_slots, completed_slots, current_slot)
    }

    pub async fn cleanup_old_slots(&self, keep_recent: u64) {
        let current = self.current_slot.load(std::sync::atomic::Ordering::Relaxed);
        let threshold = current.saturating_sub(keep_recent);

        let mut slots = self.slots.write().await;
        slots.retain(|&(_, slot_number), _| slot_number > threshold);

        debug!(
            "[PNFIFO] Node {} cleaned slots < {}, retained {} slots",
            self.node_id,
            threshold,
            slots.len()
        );
    }
}
