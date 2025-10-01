use crate::smrol::crypto::{
    verify_combined_signature_bytes, verify_signature_share, SmrolThresholdSig,
};
use crate::smrol::message::SmrolMessage;
use crate::smrol::network::{SmrolNetworkMessage, SmrolTcpNetwork};
use ed25519_dalek::{Signer, SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};
use tracing_subscriber::field::debug;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{atomic::AtomicU64, Arc};
use tokio::{
    sync::RwLock,
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
    proposal_senders: HashSet<usize>,
    final_received: bool,
    pending_final: Option<(Vec<u8>, Vec<u8>)>,
}

impl PnfifoSlotState {
    fn new(threshold: usize) -> Self {
        Self {
            output: None,
            value: None,
            votes: HashMap::new(),
            threshold_sig: SmrolThresholdSig::new(threshold),
            proposal_received: false,
            proposal_senders: HashSet::new(),
            final_received: false,
            pending_final: None,
        }
    }
}

#[derive(Debug)]
pub struct PnfifoBc {
    node_id: usize,
    total_nodes: usize,
    threshold: usize, // 2f + 1

    // 算法状态
    current_slot: AtomicU64,
    slots: Arc<RwLock<HashMap<(usize, u64), PnfifoSlotState>>>,
    leader_flags: Arc<RwLock<HashMap<usize, bool>>>,

    // 密码学
    signing_key: SigningKey,
    verifying_keys: HashMap<usize, VerifyingKey>,

    // 网络
    network: Arc<SmrolTcpNetwork>,
}

impl PnfifoBc {
    pub async fn new(
        node_id: usize,
        total_nodes: usize,
        signing_key: SigningKey,
        verifying_keys: HashMap<usize, VerifyingKey>,
        peer_addrs: HashMap<usize, SocketAddr>,
    ) -> Result<Self, String> {
        let threshold = 2 * ((total_nodes - 1) / 3) + 1; // 2f + 1

        info!(
            "🔄 PNFIFO-BC Initialization: Node {}, threshold: {}/{}",
            node_id, threshold, total_nodes
        );

        let network = Arc::new(SmrolTcpNetwork::new(node_id, peer_addrs));
        network
            .start_server()
            .await
            .map_err(|e| format!("Failed to start PNFIFO network: {}", e))?;

        Ok(Self {
            node_id,
            total_nodes,
            threshold,
            current_slot: AtomicU64::new(1),
            slots: Arc::new(RwLock::new(HashMap::new())),
            leader_flags: Arc::new(RwLock::new(
                verifying_keys.keys().map(|id| (*id, false)).collect(),
            )),
            signing_key,
            verifying_keys,
            network,
        })
    }

    async fn wait_for_flag_clear(
        leader_flags: &Arc<RwLock<HashMap<usize, bool>>>,
        leader_id: usize,
        slot: u64,
        node_id: usize,
    ) {
        let mut attempts: u64 = 0;
        loop {
            {
                let flags = leader_flags.read().await;
                if !flags.get(&leader_id).copied().unwrap_or(false) {
                    if attempts > 0 {
                        debug!(
                            "⏱️ [PNFIFO-BC] Node {} flag_{} at slot {} after {} checks",
                            node_id,
                            leader_id,
                            slot,
                            attempts
                        );
                    }
                    break;
                }
            }

            if attempts == 0 {
                debug!(
                    "⏳ [PNFIFO-BC] Node {} wait flag_{} at slot {}",
                    node_id, leader_id, slot
                );
            }

            attempts += 1;
            if attempts % 100 == 0 {
                debug!(
                    "⏳ [PNFIFO-BC] Node {} still waiting flag_{} at slot {} after {} checks",
                    node_id,
                    leader_id,
                    slot,
                    attempts
                );
            }
            sleep(Duration::from_millis(10)).await;
        }
    }

    /// Return a clone of the underlying SMROL network handle so other
    /// components can share the same transport instance.
    pub fn network(&self) -> Arc<SmrolTcpNetwork> {
        Arc::clone(&self.network)
    }

    pub async fn start(&self) -> Result<(), String> {
        // 启动网络监听器
        self.start_network_listener().await;

        info!("✅ [PNFIFO-BC] Node {} 网络监听器已启动", self.node_id);
        Ok(())
    }

    async fn start_network_listener(&self) {
        let pnfifo_rx = self.network.get_pnfifo_receiver();
        let node_id = self.node_id;
        let slots = Arc::clone(&self.slots);
        let threshold = self.threshold;
        let verifying_keys = self.verifying_keys.clone();
        let signing_key = self.signing_key.clone();
        let network = Arc::clone(&self.network);
        let leader_flags = Arc::clone(&self.leader_flags);

        tokio::spawn(async move {
            info!("📡 [PNFIFO-BC] Node {} 启动网络消息监听器", node_id);

            let mut rx = pnfifo_rx.lock().await;

            while let Some((sender_id, message)) = rx.recv().await {
                // debug!(
                //     "📨 [PNFIFO-BC] Node {} 收到来自 {} 的消息: {:?}",
                //     node_id,
                //     sender_id,
                //     std::mem::discriminant(&message)
                // );

                match message {
                    SmrolMessage::PnfifoProposal {
                        sender_id: prop_sender,
                        slot,
                        value,
                    } => {
                        if let Err(e) = PnfifoBc::handle_proposal_static(
                            node_id,
                            &slots,
                            &leader_flags,
                            threshold,
                            &verifying_keys,
                            &signing_key,
                            &network,
                            prop_sender,
                            slot,
                            value,
                        )
                        .await
                        {
                            error!("处理PROPOSAL失败: {}", e);
                        }
                    }
                    SmrolMessage::PnfifoVote {
                        leader_id,
                        sender_id: vote_sender,
                        slot,
                        signature_share,
                    } => {
                        if let Err(e) = PnfifoBc::handle_vote_static(
                            node_id,
                            &slots,
                            &leader_flags,
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
                            error!("处理VOTE失败: {}", e);
                        }
                    }
                    SmrolMessage::PnfifoFinal {
                        leader_id,
                        sender_id: final_sender,
                        slot,
                        value,
                        combined_signature,
                    } => {
                        if let Err(e) = PnfifoBc::handle_final_static(
                            node_id,
                            &slots,
                            &leader_flags,
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
                            error!("处理FINAL失败: {}", e);
                        }
                    }
                    _ => {
                        warn!("收到非PNFIFO消息: {:?}", std::mem::discriminant(&message));
                    }
                }
            }
        });
    }

    // pub fn set_network_sender(&mut self, tx: tokio::sync::mpsc::UnboundedSender<(usize, PnfifoMessage)>) {
    //     self.message_tx = Some(tx);
    // }

    // 算法1: PNFIFO-BC_s[k](v_k) - 发送者广播值
    pub async fn broadcast(&self, slot: u64, value: Vec<u8>) -> Result<u64, String> {
        self.current_slot
            .store(slot.saturating_add(1), std::sync::atomic::Ordering::Relaxed);

        {
            let mut flags = self.leader_flags.write().await;
            flags.insert(self.node_id, false);
        }

        info!(
            "📤 [PNFIFO-BC] Node {} Broadcast Proposal for slot {}, length: {} bytes",
            self.node_id,
            slot,
            value.len()
        );

        debug!(
            "🧾 [PNFIFO-BC] Node {} proposal payload={} slot={}",
            self.node_id,
            hex::encode(&value[..std::cmp::min(8, value.len())]),
            slot
        );

        // 初始化slot状态
        {
            let mut slots = self.slots.write().await;
            slots.insert((self.node_id, slot), PnfifoSlotState::new(self.threshold));
        }

        // 广播PROPOSAL消息 (line 2 in algorithm)
        let proposal = SmrolMessage::PnfifoProposal {
            sender_id: self.node_id,
            slot,
            value: value.clone(),
        };

        let message_id = format!(
            "pnfifo-proposal:{}:{}:{}",
            self.node_id,
            slot,
            Uuid::new_v4()
        );

        let network_msg = SmrolNetworkMessage {
            from_node_id: self.node_id,
            to_node_id: None, // 广播给所有节点
            message: proposal,
            // when the message is created
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64,
            message_id,
        };

        self.network.parallel_broadcast(&network_msg).await;

        debug!(
            "✅ [PNFIFO-BC] Node {} 完成slot {} 广播",
            self.node_id, slot
        );
        Ok(slot)
    }

    // 处理接收到的PROPOSAL消息 (lines 3-7 in algorithm)
    async fn handle_proposal_static(
        node_id: usize,
        slots: &Arc<RwLock<HashMap<(usize, u64), PnfifoSlotState>>>,
        leader_flags: &Arc<RwLock<HashMap<usize, bool>>>,
        threshold: usize,
        verifying_keys: &HashMap<usize, VerifyingKey>,
        signing_key: &SigningKey,
        network: &Arc<SmrolTcpNetwork>,
        sender_id: usize,
        slot: u64,
        value: Vec<u8>,
    ) -> Result<(), String> {
        debug!(
            "📥 [PNFIFO-BC] Node {} 收到来自 {} 的PROPOSAL, slot: {}",
            node_id, sender_id, slot
        );

        if !PnfifoBc::predicate_q_static(&value) {
            debug!(
                "❌ [PNFIFO-BC] Node {} 拒绝slot {} proposal: predicate Q(v) 未满足",
                node_id, slot
            );
            return Ok(());
        }

        let mut vote_message: Option<SmrolMessage> = None;
        let mut delayed_finalize: Option<(Vec<u8>, Vec<u8>)> = None;

        PnfifoBc::wait_for_flag_clear(leader_flags, sender_id, slot, node_id).await;

        {
            let mut slots_guard = slots.write().await;
            let slot_state = slots_guard
                .entry((sender_id, slot))
                .or_insert_with(|| PnfifoSlotState::new(threshold));

            if slot_state.proposal_senders.contains(&sender_id) {
                debug!(
                    "🔁 [PNFIFO-BC] Node {} 已处理过来自 {} 的slot {} proposal，跳过",
                    node_id, sender_id, slot
                );
                return Ok(());
            }

            let mut flag_guard = leader_flags.write().await;
            let flag_entry = flag_guard.entry(sender_id).or_insert(false);

            if *flag_entry {
                debug!(
                    "♻️ [PNFIFO-BC] Node {} flag already set for leader {} slot {}, skipping",
                    node_id, sender_id, slot
                );
                return Ok(());
            }

            debug!(
                "🧮 [PNFIFO-BC] Node {} accept proposal slot {} from {} (flag was {})",
                node_id, slot, sender_id, *flag_entry
            );

            slot_state.proposal_senders.insert(sender_id);
            slot_state.proposal_received = true;
            slot_state.value = Some(value.clone());

            let message_to_sign = PnfifoBc::create_vote_message_static(slot, &value);
            let signature_share = signing_key.sign(&message_to_sign).to_bytes().to_vec();

            vote_message = Some(SmrolMessage::PnfifoVote {
                leader_id: sender_id,
                sender_id: node_id,
                slot,
                signature_share,
            });

            *flag_entry = true;
            debug!(
                "🇺🇳 [FLAG_s] Flag_{} set to {} at slot {} (proposal)",
                sender_id, *flag_entry, slot
            );

            info!(
                "🗳️ [PNFIFO-BC] Node {} 发送 VOTE for slot {} (Leader = {})",
                node_id, slot, sender_id
            );

            if let Some(pending) = slot_state.pending_final.take() {
                delayed_finalize = Some(pending);
            }
        }

        // make another attempt for delayed final messages
        if let Some((pending_value, pending_signature)) = delayed_finalize {
            PnfifoBc::finalize_with_signature(
                node_id,
                slots,
                leader_flags,
                threshold,
                verifying_keys,
                sender_id,
                slot,
                pending_value,
                pending_signature,
            )
            .await?;
        }

        // 发送投票给提议者
        if let Some(vote_message) = vote_message {
            let network_msg = SmrolNetworkMessage {
                from_node_id: node_id,
                to_node_id: Some(sender_id), // 单播给提议者
                message: vote_message,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_micros() as u64,
                message_id: format!(
                    "pnfifo-vote:{}:{}:{}:{}",
                    node_id,
                    sender_id,
                    slot,
                    Uuid::new_v4()
                ),
            };

            network
                .send_message(network_msg)
                .await
                .map_err(|e| format!("发送VOTE失败: {}", e))?;
        }

        Ok(())
    }

    // 处理接收到的VOTE消息 (lines 8-13 in algorithm)
    async fn handle_vote_static(
        node_id: usize,
        slots: &Arc<RwLock<HashMap<(usize, u64), PnfifoSlotState>>>,
        _leader_flags: &Arc<RwLock<HashMap<usize, bool>>>,
        threshold: usize,
        verifying_keys: &HashMap<usize, VerifyingKey>,
        network: &Arc<SmrolTcpNetwork>,
        leader_id: usize,
        sender_id: usize,
        slot: u64,
        signature_share: Vec<u8>,
    ) -> Result<(), String> {
        debug!(
            "🗳️ [PNFIFO-BC] Node {} 收到来自 {} 的VOTE, slot: {}",
            node_id, sender_id, slot
        );

        let mut should_finalize = false;
        let mut finalize_data = None;

        {
            let mut slots_guard = slots.write().await;
            if let Some(slot_state) = slots_guard.get_mut(&(leader_id, slot)) {
                // 验证签名份额
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

                                debug!(
                                    "✅ [PNFIFO-BC] Node {} 接受来自 {} 的有效VOTE, 当前票数: {}/{}",
                                    node_id,
                                    sender_id,
                                    slot_state.votes.len(),
                                    threshold
                                );

                                if !reached {
                                    debug!(
                                        "🧩 [PNFIFO-BC] Node {} slot {} awaiting more votes (have {} need {})",
                                        node_id,
                                        slot,
                                        slot_state.votes.len(),
                                        threshold
                                    );
                                }

                                if reached {
                                    if let Ok(combined_sig) = slot_state.threshold_sig.combine() {
                                        finalize_data = Some((value.clone(), combined_sig));
                                        should_finalize = true;

                                        info!(
                                            "🎯 [PNFIFO-BC] Node {} slot {} 达到阈值, 准备finalize",
                                            node_id, slot
                                        );
                                        debug!(
                                            "🔐 [PNFIFO-BC] Node {} slot {} collected votes from {:?}",
                                            node_id,
                                            slot,
                                            slot_state.votes.keys().cloned().collect::<Vec<_>>()
                                        );
                                    }
                                }
                            }
                        } else {
                            warn!(
                                "❌ [PNFIFO-BC] Node {} 拒绝来自 {} 的无效签名",
                                node_id, sender_id
                            );
                        }
                    }
                }
            }
        }

        // 广播FINAL消息
        if should_finalize {
            if let Some((value, combined_signature)) = finalize_data {
                let final_message = SmrolMessage::PnfifoFinal {
                    leader_id,
                    sender_id: node_id,
                    slot,
                    value: value.clone(),
                    combined_signature: combined_signature.clone(),
                };

                let network_msg = SmrolNetworkMessage {
                    from_node_id: node_id,
                    to_node_id: None, // 广播给所有节点
                    message: final_message,
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_micros() as u64,
                    message_id: format!("pnfifo-final:{}:{}:{}", node_id, slot, Uuid::new_v4()),
                };

                network
                    .send_message(network_msg)
                    .await
                    .map_err(|e| format!("广播FINAL失败: {}", e))?;

                debug!(
                    "✅ [PNFIFO-BC] Node {} 广播slot {} FINAL as leader",
                    node_id, slot
                );
                // 更新本地输出
                // PnfifoBc::store_output_static(
                //     slots,
                //     threshold,
                //     leader_id,
                //     slot,
                //     value,
                //     combined_signature,
                // )
                // .await;
            }
        }

        Ok(())
    }

    // 处理接收到的FINAL消息 (lines 14-18 in algorithm)
    async fn handle_final_static(
        node_id: usize,
        slots: &Arc<RwLock<HashMap<(usize, u64), PnfifoSlotState>>>,
        leader_flags: &Arc<RwLock<HashMap<usize, bool>>>,
        threshold: usize,
        verifying_keys: &HashMap<usize, VerifyingKey>,
        leader_id: usize,
        sender_id: usize,
        slot: u64,
        value: Vec<u8>,
        combined_signature: Vec<u8>,
    ) -> Result<(), String> {
        debug!(
            "🏁 [PNFIFO-BC] Node {} 收到来自 {} 的FINAL, slot: {}",
            node_id, sender_id, slot
        );

        {
            let mut slots_guard = slots.write().await;
            let slot_state = slots_guard
                .entry((leader_id, slot))
                .or_insert_with(|| PnfifoSlotState::new(threshold));

            if slot_state.final_received {
                debug!(
                    "🔁 [PNFIFO-BC] Node {} 已处理 leader {} slot {} 的FINAL, 忽略重复",
                    node_id, leader_id, slot
                );
                return Ok(());
            }

            let flag_ready = {
                let flags = leader_flags.read().await;
                *flags.get(&leader_id).unwrap_or(&false)
            };

            if !flag_ready {
                slot_state.pending_final = Some((value.clone(), combined_signature.clone()));
                debug!(
                    "⏳ [PNFIFO-BC] Node {} 暂存slot {} 来自 {} 的FINAL，等待flag=1",
                    node_id, slot, leader_id
                );
                return Ok(());
            }

            slot_state.pending_final = None;
        }

        PnfifoBc::finalize_with_signature(
            node_id,
            slots,
            leader_flags,
            threshold,
            verifying_keys,
            leader_id,
            slot,
            value,
            combined_signature,
        )
        .await
    }

    async fn finalize_with_signature(
        node_id: usize,
        slots: &Arc<RwLock<HashMap<(usize, u64), PnfifoSlotState>>>,
        leader_flags: &Arc<RwLock<HashMap<usize, bool>>>,
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
                let mut should_store = false;
                {
                    let mut slots_guard = slots.write().await;
                    let slot_state = slots_guard
                        .entry((leader_id, slot))
                        .or_insert_with(|| PnfifoSlotState::new(threshold));

                    if slot_state.final_received {
                        debug!(
                            "🔁 [PNFIFO-BC] Node {} 已处理 leader {} slot {} 的FINAL, 忽略重复",
                            node_id, leader_id, slot
                        );
                        return Ok(());
                    }

                    slot_state.value.get_or_insert_with(|| value.clone());
                    slot_state.pending_final = None;
                    slot_state.final_received = true;
                    should_store = true;
                }

                if should_store {
                    PnfifoBc::store_output_static(
                        slots,
                        threshold,
                        leader_id,
                        slot,
                        value.clone(),
                        combined_signature.clone(),
                    )
                    .await;

                    {
                        let mut flags = leader_flags.write().await;
                        flags.insert(leader_id, false);
                    }
                    debug!(
                        "🇺🇳 [FLAG_s] Flag_{} set to {} at slot {} (final)",
                        leader_id, false, slot
                    );
                }
            }
            Ok(false) => {
                warn!(
                    "❌ [PNFIFO-BC] Node {} slot {} 组合签名验证未通过",
                    node_id, slot
                );
            }
            Err(e) => {
                warn!(
                    "❌ [PNFIFO-BC] Node {} slot {} 验证组合签名出错: {}",
                    node_id, slot, e
                );
            }
        }

        Ok(())
    }

    // 获取slot的输出
    pub async fn get_output(&self, leader_id: usize, slot: u64) -> Option<(Vec<u8>, Vec<u8>)> {
        let slots = self.slots.read().await;
        slots
            .get(&(leader_id, slot))
            .and_then(|state| state.output.clone())
    }

    // 谓词Q - 检查值是否有效 (简化实现)
    fn predicate_q(&self, _value: &[u8]) -> bool {
        // dummy implementation, always returns true
        true
    }

    // 静态谓词Q方法，供静态函数调用
    fn predicate_q_static(value: &[u8]) -> bool {
        // value.len() == 32
        true
    }

    // 创建投票消息
    fn create_vote_message(&self, slot: u64, value: &[u8]) -> Vec<u8> {
        let mut message = Vec::new();
        message.extend_from_slice(&slot.to_be_bytes());
        message.extend_from_slice(value);
        message
    }

    // 静态创建投票消息方法，供静态函数调用
    fn create_vote_message_static(slot: u64, value: &[u8]) -> Vec<u8> {
        let mut message = Vec::new();
        message.extend_from_slice(&slot.to_be_bytes());
        message.extend_from_slice(value);
        message
    }

    // 存储输出
    async fn store_output(&self, slot: u64, value: Vec<u8>, signature: Vec<u8>) {
        let mut slots = self.slots.write().await;
        if let Some(slot_state) = slots.get_mut(&(self.node_id, slot)) {
            slot_state.output = Some((value, signature));
            slot_state.final_received = true;
        }
    }

    async fn store_output_static(
        slots: &Arc<RwLock<HashMap<(usize, u64), PnfifoSlotState>>>,
        threshold: usize,
        leader_id: usize,
        slot: u64,
        value: Vec<u8>,
        signature: Vec<u8>,
    ) {
        debug!(
            "🏁 [PNFIFO-BC] Node {} 存储slot {} 的输出, leader_id: {}",
            leader_id, slot, leader_id
        );
        let mut guard = slots.write().await;
        let slot_state = guard
            .entry((leader_id, slot))
            .or_insert_with(|| PnfifoSlotState::new(threshold));
        slot_state.output = Some((value, signature));
        slot_state.final_received = true;
    }

    // 获取统计信息
    pub async fn get_stats(&self) -> (usize, usize, u64) {
        let slots = self.slots.read().await;
        let total_slots = slots.len();
        let completed_slots = slots.values().filter(|s| s.output.is_some()).count();
        let current_slot = self.current_slot.load(std::sync::atomic::Ordering::Relaxed);

        (total_slots, completed_slots, current_slot)
    }

    // 清理旧的slot状态
    pub async fn cleanup_old_slots(&self, keep_recent: u64) {
        let current = self.current_slot.load(std::sync::atomic::Ordering::Relaxed);
        let threshold = current.saturating_sub(keep_recent);

        let mut slots = self.slots.write().await;
        slots.retain(|&(_, slot_number), _| slot_number > threshold);

        debug!(
            "🧹 [PNFIFO-BC] Node {} 清理slot < {}, 保留 {} 个slots",
            self.node_id,
            threshold,
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
        assert_eq!(current, 2); // next slot should track provided value
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

        assert!(pnfifo.predicate_q(b"any_value"));
        assert!(PnfifoBc::predicate_q_static(b"any_value"));
    }
}
