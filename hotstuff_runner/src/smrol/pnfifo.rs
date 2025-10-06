use crate::smrol::crypto::{
    verify_combined_signature_bytes, verify_signature_share, SmrolThresholdSig,
};
use crate::smrol::message::SmrolMessage;
use crate::smrol::network::{SmrolNetworkMessage, SmrolTcpNetwork};
use ed25519_dalek::{Signer, SigningKey, VerifyingKey};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{atomic::AtomicU64, Arc};
use tokio::{
    sync::{mpsc, Notify, RwLock},
    time::{sleep, Duration},
};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

#[derive(Debug)]
struct PnfifoSlotState {
    output: Option<(Vec<u8>, Vec<u8>)>, // (value, signature)
    value: Option<Vec<u8>>,
    votes: HashMap<usize, Vec<u8>>,
    threshold_sig: SmrolThresholdSig,
    proposal_received: bool,
    final_received: bool,
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
            final_received: false,
            pending_final: None,
            final_broadcasted: false,
        }
    }
}

const FINAL_BROADCAST_MAX_RETRIES: usize = 5;
const FINAL_BROADCAST_INITIAL_DELAY_MS: u64 = 50;
const FINAL_BROADCAST_MAX_DELAY_MS: u64 = 1_000;

// 专用通道的 PROPOSAL 消息
#[derive(Debug, Clone)]
struct ProposalTask {
    slot: u64,
    value: Vec<u8>,
}

#[derive(Debug)]
pub struct PnfifoBc {
    node_id: usize,
    total_nodes: usize,
    threshold: usize,

    // 算法状态
    current_slot: AtomicU64,
    slots: Arc<RwLock<HashMap<(usize, u64), PnfifoSlotState>>>,
    slot_output_notifiers: Arc<RwLock<HashMap<(usize, u64), Arc<Notify>>>>,

    // 密码学
    signing_key: SigningKey,
    verifying_keys: HashMap<usize, VerifyingKey>,

    // 网络
    network: Arc<SmrolTcpNetwork>,

    // 每个 leader 的专用通道发送端
    leader_proposal_senders: HashMap<usize, mpsc::UnboundedSender<ProposalTask>>,
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
                info!("[PNFIFO] Node {} started dedicated task for Leader {}", 
                    node_id_clone, leader_id);

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
                        error!("[PNFIFO] Failed to process Leader {} proposal: {}", leader_id, e);
                    }
                }

                warn!("[PNFIFO] Leader {} processing task exited", leader_id);
            });

            leader_proposal_senders.insert(leader_id, tx);
        }

        Ok(Self {
            node_id,
            total_nodes,
            threshold,
            current_slot: AtomicU64::new(1),
            slots,
            slot_output_notifiers,
            signing_key,
            verifying_keys,
            network,
            leader_proposal_senders,
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
        debug!("[PNFIFO] Node {} processing Leader {} slot {}", node_id, leader_id, slot);

        // 检查是否有延迟的 FINAL
        let delayed_final = {
            let mut slots_guard = slots.write().await;
            let slot_state = slots_guard
                .entry((leader_id, slot))
                .or_insert_with(|| PnfifoSlotState::new(threshold));

            // 去重检查
            if slot_state.proposal_received {
                debug!("[PNFIFO] Node {} already processed Leader {} slot {}", 
                    node_id, leader_id, slot);
                return Ok(());
            }

            slot_state.proposal_received = true;
            slot_state.value = Some(value.clone());

            // 取出可能存在的 pending_final
            slot_state.pending_final.take()
        };

        // 如果有延迟的 FINAL，先处理它
        if let Some((final_value, final_signature)) = delayed_final {
            debug!("[PNFIFO] Node {} processing delayed FINAL for Leader {} slot {}", 
                node_id, leader_id, slot);
            
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
                node_id, leader_id, slot,
                Uuid::new_v4()
            ),
        };

        network
            .send_message(network_msg)
            .await
            .map_err(|e| format!("Failed to send VOTE: {}", e))?;

        debug!("[PNFIFO] Node {} sent VOTE for Leader {} slot {}", node_id, leader_id, slot);

        // PNFIFO 内部等待：等待 FINAL 到达（实现 flag_s 语义）
        Self::wait_for_final_internal(node_id, slots, slot_output_notifiers, leader_id, slot).await;

        debug!("[PNFIFO] Node {} completed processing Leader {} slot {}", 
            node_id, leader_id, slot);

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
                if slot_state.final_received {
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

            // 再检查 final_received
            {
                let slots_guard = slots.read().await;
                if let Some(slot_state) = slots_guard.get(&(leader_id, slot)) {
                    if slot_state.final_received {
                        if attempts > 0 {
                            debug!("[PNFIFO] Node {} received FINAL for Leader {} slot {} after {} waits",
                                node_id, leader_id, slot, attempts);
                        }
                        return;
                    }
                }
            }

            if attempts % 100 == 0 {
                debug!("[PNFIFO] Node {} waiting for FINAL Leader {} slot {} (attempt {})",
                    node_id, leader_id, slot, attempts);
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
        let slots = Arc::clone(&self.slots);
        let slot_output_notifiers = Arc::clone(&self.slot_output_notifiers);
        let threshold = self.threshold;
        let verifying_keys = self.verifying_keys.clone();
        let network = Arc::clone(&self.network);

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
                        debug!("[PNFIFO] Node {} received PROPOSAL from {} slot {}", 
                            node_id, prop_sender, slot);

                        // 检查 Q(v)
                        if !PnfifoBc::predicate_q_static(&value) {
                            debug!("[PNFIFO] Node {} rejected slot {}: Q(v) not satisfied", 
                                node_id, slot);
                            continue;
                        }

                        // 发送到该 leader 的专用通道
                        if let Some(tx) = leader_proposal_senders.get(&prop_sender) {
                            if let Err(e) = tx.send(ProposalTask { slot, value }) {
                                error!("[PNFIFO] Failed to send to Leader {} channel: {}", 
                                    prop_sender, e);
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
                        // spawn 处理 VOTE
                        let slots = Arc::clone(&slots);
                        let slot_output_notifiers = Arc::clone(&slot_output_notifiers);
                        let verifying_keys = verifying_keys.clone();
                        let network = Arc::clone(&network);

                        tokio::spawn(async move {
                            if let Err(e) = PnfifoBc::handle_vote_static(
                                node_id,
                                &slots,
                                &slot_output_notifiers,
                                threshold,
                                &verifying_keys,
                                &network,
                                leader_id,
                                vote_sender,
                                slot,
                                signature_share,
                            )
                            .await
                            {
                                error!("[PNFIFO] Failed to handle VOTE: {}", e);
                            }
                        });
                    }

                    SmrolMessage::PnfifoFinal {
                        leader_id,
                        sender_id: final_sender,
                        slot,
                        value,
                        combined_signature,
                    } => {
                        // spawn 处理 FINAL
                        let slots = Arc::clone(&slots);
                        let slot_output_notifiers = Arc::clone(&slot_output_notifiers);
                        let verifying_keys = verifying_keys.clone();

                        tokio::spawn(async move {
                            if let Err(e) = PnfifoBc::handle_final_static(
                                node_id,
                                &slots,
                                &slot_output_notifiers,
                                threshold,
                                &verifying_keys,
                                leader_id,
                                final_sender,
                                slot,
                                value,
                                combined_signature,
                            )
                            .await
                            {
                                error!("[PNFIFO] Failed to handle FINAL: {}", e);
                            }
                        });
                    }

                    _ => {
                        warn!("[PNFIFO] Received non-PNFIFO message");
                    }
                }
            }
        });
    }

    pub async fn broadcast(&self, slot: u64, value: Vec<u8>) -> Result<u64, String> {
        self.current_slot
            .store(slot.saturating_add(1), std::sync::atomic::Ordering::Relaxed);

        info!("[PNFIFO] Node {} broadcasting proposal for slot {}", self.node_id, slot);

        // 初始化 slot 状态
        {
            let mut slots = self.slots.write().await;
            let slot_state = slots
                .entry((self.node_id, slot))
                .or_insert_with(|| PnfifoSlotState::new(self.threshold));
            slot_state.value = Some(value.clone());
            slot_state.proposal_received = true;
        }

        // 广播 PROPOSAL
        let proposal = SmrolMessage::PnfifoProposal {
            sender_id: self.node_id,
            slot,
            value: value.clone(),
        };

        let network_msg = SmrolNetworkMessage {
            from_node_id: self.node_id,
            to_node_id: None,
            message: proposal,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64,
            message_id: format!("pnfifo-proposal:{}:{}:{}", 
                self.node_id, slot, Uuid::new_v4()),
        };

        self.network
            .send_message(network_msg)
            .await
            .map_err(|e| format!("PROPOSAL broadcast failed: {}", e))?;

        debug!("[PNFIFO] Node {} completed slot {} PROPOSAL broadcast", self.node_id, slot);
        Ok(slot)
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
        debug!("[PNFIFO] Node {} received VOTE from {} for slot {} leader {}", 
            node_id, sender_id, slot, leader_id);

        let mut should_finalize = false;
        let mut finalize_data = None;

        {
            let mut slots_guard = slots.write().await;
            if let Some(slot_state) = slots_guard.get_mut(&(leader_id, slot)) {
                if let Some(ref value) = slot_state.value {
                    let message_to_verify = PnfifoBc::create_vote_message_static(slot, value);

                    if let Some(verifying_key) = verifying_keys.get(&sender_id) {
                        if verify_signature_share(&signature_share, &message_to_verify, verifying_key)
                        {
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
                            warn!("[PNFIFO] Node {} rejected invalid signature from {}", 
                                node_id, sender_id);
                        }
                    }
                }
            }
        }

        if should_finalize {
            if let Some((value, combined_signature)) = finalize_data {
                PnfifoBc::broadcast_final_with_retry(
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

    async fn broadcast_final_with_retry(
        network: Arc<SmrolTcpNetwork>,
        node_id: usize,
        leader_id: usize,
        slot: u64,
        value: Vec<u8>,
        combined_signature: Vec<u8>,
    ) -> Result<(), String> {
        let mut delay = Duration::from_millis(FINAL_BROADCAST_INITIAL_DELAY_MS);

        for attempt in 1..=FINAL_BROADCAST_MAX_RETRIES {
            let message = SmrolMessage::PnfifoFinal {
                leader_id,
                sender_id: node_id,
                slot,
                value: value.clone(),
                combined_signature: combined_signature.clone(),
            };

            let network_msg = SmrolNetworkMessage {
                from_node_id: node_id,
                to_node_id: None,
                message,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_micros() as u64,
                message_id: format!(
                    "pnfifo-final:{}:{}:{}:{}",
                    node_id, slot, attempt,
                    Uuid::new_v4()
                ),
            };

            match network.send_message(network_msg).await {
                Ok(_) => {
                    if attempt > 1 {
                        info!("[PNFIFO] Node {} slot {} FINAL retry {} succeeded", 
                            node_id, slot, attempt);
                    } else {
                        debug!("[PNFIFO] Node {} broadcast slot {} FINAL", node_id, slot);
                    }
                    return Ok(());
                }
                Err(err) => {
                    if attempt == FINAL_BROADCAST_MAX_RETRIES {
                        return Err(format!(
                            "FINAL broadcast failed after {} attempts: {}",
                            FINAL_BROADCAST_MAX_RETRIES, err
                        ));
                    }

                    warn!(
                        "[PNFIFO] Node {} slot {} FINAL attempt {} failed: {}, retrying in {}ms",
                        node_id, slot, attempt, err, delay.as_millis()
                    );

                    sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_millis(FINAL_BROADCAST_MAX_DELAY_MS));
                }
            }
        }

        Err("FINAL broadcast retry logic exited unexpectedly".to_string())
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
        debug!("[PNFIFO] Node {} received FINAL from {} for Leader {} slot {}", 
            node_id, sender_id, leader_id, slot);

        // 检查是否已收到 PROPOSAL
        let proposal_received = {
            let slots_guard = slots.read().await;
            if let Some(slot_state) = slots_guard.get(&(leader_id, slot)) {
                if slot_state.final_received {
                    debug!("[PNFIFO] Node {} already processed FINAL for Leader {} slot {}", 
                        node_id, leader_id, slot);
                    return Ok(());
                }
                slot_state.proposal_received
            } else {
                false
            }
        };

        if !proposal_received {
            // FINAL 先于 PROPOSAL 到达，暂存
            let mut slots_guard = slots.write().await;
            let slot_state = slots_guard
                .entry((leader_id, slot))
                .or_insert_with(|| PnfifoSlotState::new(threshold));

            if slot_state.final_received {
                return Ok(());
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
                debug!("[PNFIFO] Node {} slot {} FINAL signature verified", node_id, slot);

                let should_store = {
                    let mut slots_guard = slots.write().await;
                    let slot_state = slots_guard
                        .entry((leader_id, slot))
                        .or_insert_with(|| PnfifoSlotState::new(threshold));

                    if slot_state.final_received {
                        debug!("[PNFIFO] Node {} already processed FINAL for Leader {} slot {}", 
                            node_id, leader_id, slot);
                        return Ok(());
                    }

                    slot_state.value.get_or_insert_with(|| value.clone());
                    slot_state.pending_final = None;
                    true
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
                warn!("[PNFIFO] Node {} slot {} signature verification failed", node_id, slot);
            }
            Err(e) => {
                warn!("[PNFIFO] Node {} slot {} signature verification error: {}", 
                    node_id, slot, e);
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
        // Fast path
        {
            let slots = self.slots.read().await;
            if let Some(slot_state) = slots.get(&(leader_id, slot)) {
                if slot_state.output.is_some() {
                    return;
                }
            }
        }

        // Slow path
        let notifier = {
            let mut map = self.slot_output_notifiers.write().await;
            Arc::clone(
                map.entry((leader_id, slot))
                    .or_insert_with(|| Arc::new(Notify::new())),
            )
        };

        let mut attempts = 0;
        loop {
            let notified = notifier.notified();

            {
                let slots = self.slots.read().await;
                if let Some(slot_state) = slots.get(&(leader_id, slot)) {
                    if slot_state.output.is_some() {
                        if attempts > 0 {
                            debug!("[PNFIFO] Node {} got output for Leader {} slot {} after {} waits",
                                self.node_id, leader_id, slot, attempts);
                        }
                        return;
                    }
                }
            }

            if attempts % 100 == 0 {
                debug!("[PNFIFO] Node {} waiting for output Leader {} slot {} (attempt {})",
                    self.node_id, leader_id, slot, attempts);
            }
            attempts += 1;

            if attempts < 200 {
                notified.await;
            } else {
                tokio::select! {
                    _ = notified => { }
                    _ = tokio::time::sleep(Duration::from_millis(1)) => {
                        warn!("[PNFIFO] Node {} timeout waiting for output Leader {} slot {}", 
                            self.node_id, leader_id, slot);
                        return;
                    }
                }
            }
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
        debug!("[PNFIFO] Storing output for Leader {} slot {}", leader_id, slot);

        {
            let mut guard = slots.write().await;
            let slot_state = guard
                .entry((leader_id, slot))
                .or_insert_with(|| PnfifoSlotState::new(threshold));
            slot_state.output = Some((value, signature));
            slot_state.final_received = true;
        }

        // 唤醒所有等待这个 output 的任务
        // 包括：wait_for_final_internal 和 wait_for_output
        let notifier = {
            let mut map = slot_output_notifiers.write().await;
            Arc::clone(
                map.entry((leader_id, slot))
                    .or_insert_with(|| Arc::new(Notify::new())),
            )
        };
        notifier.notify_waiters();

        debug!("[PNFIFO] Notified waiters for Leader {} slot {}", leader_id, slot);
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
            self.node_id, threshold,
            slots.len()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;
    use std::collections::HashMap;
    use std::net::SocketAddr;

    #[tokio::test]
    async fn test_pnfifo_basic() {
        let signing_key = SigningKey::from_bytes(&[1u8; 32]);
        let verifying_key = signing_key.verifying_key();

        let mut verifying_keys = HashMap::new();
        verifying_keys.insert(0, verifying_key);

        let mut peer_addrs: HashMap<usize, SocketAddr> = HashMap::new();
        peer_addrs.insert(0, "127.0.0.1:21000".parse().unwrap());

        let pnfifo = PnfifoBc::new(0, 1, signing_key, verifying_keys, peer_addrs)
            .await
            .unwrap();

        let value = b"test_value".to_vec();
        let slot = pnfifo.broadcast(1, value.clone()).await.unwrap();

        assert_eq!(slot, 1);

        let (total, _, current) = pnfifo.get_stats().await;
        assert_eq!(total, 1);
        assert_eq!(current, 2);
    }

    #[tokio::test]
    async fn test_predicate_q() {
        let signing_key = SigningKey::from_bytes(&[2u8; 32]);
        let verifying_key = signing_key.verifying_key();

        let mut verifying_keys = HashMap::new();
        verifying_keys.insert(1, verifying_key);

        let mut peer_addrs: HashMap<usize, SocketAddr> = HashMap::new();
        peer_addrs.insert(1, "127.0.0.1:21001".parse().unwrap());

        let pnfifo = PnfifoBc::new(1, 1, signing_key, verifying_keys, peer_addrs)
            .await
            .unwrap();

        assert!(PnfifoBc::predicate_q_static(b"any_value"));
    }
}