use crate::smrol::{
    consensus::TransactionEntry,
    crypto::ErasurePackage,
    finalization::OutputFinalization,
    message::{SmrolMessage, SmrolTransaction},
    network::SmrolTcpNetwork,
    pnfifo::PnfifoBc,
};
use dashmap::{mapref::entry::Entry, DashMap, DashSet};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::Instant;
use threshold_crypto::{PublicKeySet, SecretKeyShare, Signature, SignatureShare, SIG_SIZE};
use tokio::{
    sync::Mutex,
    time::{timeout, Duration},
};
use tracing::{debug, info, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeqRequest {
    pub seq_num: u64,
    pub tx: Transaction,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeqResponse {
    pub vc: Vec<u8>,
    pub s: u64,
    pub sigma: Vec<u8>, // signature
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeqOrder {
    pub vc: Vec<u8>,
    pub records: Vec<SeqResponseRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeqMedian {
    pub vc: Vec<u8>,
    pub s_tx: u64, // median sequence number
    pub sigma_seq: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeqFinal {
    pub vc: Vec<u8>,
    pub s_tx: u64, // median sequence number
    pub sigma: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeqResponseRecord {
    pub sender: usize,
    pub sequence: u64,
    pub signature: Vec<u8>,
}

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub struct VC([u8; 32]);

impl VC {
    pub fn from_slice(bytes: &[u8]) -> Self {
        assert!(bytes.len() >= 32, "VC length must be at least 32 bytes");
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes[..32]);
        VC(arr)
    }
}

impl From<&[u8]> for VC {
    fn from(bytes: &[u8]) -> Self {
        VC::from_slice(bytes)
    }
}

impl From<&Vec<u8>> for VC {
    fn from(vec: &Vec<u8>) -> Self {
        VC::from_slice(vec)
    }
}

#[derive(Debug)]
pub struct TransactionSequencing {
    pub f: usize,
    pub n: usize,
    pub process_id: usize, // node_id
    pub network: Arc<SmrolTcpNetwork>,
    pub pnfifo: Arc<PnfifoBc>,
    pub threshold_share: SecretKeyShare,
    pub threshold_public: PublicKeySet,
    pub finalization: Arc<Mutex<OutputFinalization>>,
    broadcast_seq: AtomicU64,
    local_seq: AtomicU64,
    buf: DashSet<VC>,
    pending_txs: DashMap<VC, Transaction>,
    erasure_store: DashMap<VC, ErasurePackage>,
    originated_vcs: DashSet<VC>,
    pending_seq_finals: DashMap<VC, Vec<SeqFinal>>,
    response_shares: DashMap<VC, Vec<SeqResponseRecord>>,
    completed_responses: DashSet<VC>,
    median_shares: DashMap<VC, HashMap<usize, SignatureShare>>,
    tx_sequence_map: DashMap<VC, u64>,
}

#[derive(Debug)]
struct SequencingTask {
    sender_id: usize,
    message: SmrolMessage,
}

impl TransactionSequencing {
    pub fn new(
        process_id: usize,
        n: usize,
        f: usize,
        network: Arc<SmrolTcpNetwork>,
        pnfifo: Arc<PnfifoBc>,
        threshold_share: SecretKeyShare,
        threshold_public: PublicKeySet,
        finalization: Arc<Mutex<OutputFinalization>>,
    ) -> Self {
        Self {
            f,
            n,
            process_id,
            network,
            pnfifo,
            threshold_share,
            threshold_public,
            finalization,
            broadcast_seq: AtomicU64::new(1),
            local_seq: AtomicU64::new(1),
            buf: DashSet::new(),
            pending_txs: DashMap::new(),
            erasure_store: DashMap::new(),
            originated_vcs: DashSet::new(),
            pending_seq_finals: DashMap::new(),
            response_shares: DashMap::new(),
            completed_responses: DashSet::new(),
            median_shares: DashMap::new(),
            tx_sequence_map: DashMap::new(),
        }
    }

    // Function SMROL-broadcast(k, tx) - Line 1-3
    pub async fn smrol_broadcast(&self, tx: SmrolTransaction) -> Result<(), String> {
        let s = self.broadcast_seq.fetch_add(1, Ordering::SeqCst);

        // debug!(
        //     "🚀 [Sequencing] node={} generate SEQ-REQUEST (k={}, tx_id={})",
        //     self.process_id, s, tx.id
        // );

        let payload =
            bincode::serialize(&tx).map_err(|e| format!("序列化SmrolTransaction失败: {}", e))?;
        let seq_request = SeqRequest {
            seq_num: s,
            tx: Transaction { payload },
        };

        let tx_hash = hex::encode(&seq_request.tx.payload[..std::cmp::min(8, seq_request.tx.payload.len())]);
        let message = SmrolMessage::SeqRequest {
            tx_hash,
            transaction: tx.clone(),
            sender_id: self.process_id,
            sequence_number: s,
        };
        self.network
            .broadcast(message)
            .await
            .map_err(|e| format!("广播SEQ-REQUEST失败: {}", e))?;

        //debug: insert to originated vc earlier but with additional computation
        // let data_shards = std::cmp::max(1, self.f + 1);
        // let total_shards = std::cmp::max(data_shards, self.n);
        // let encoded = ErasurePackage::encode(&payload_clone, data_shards, total_shards)
        //     .map_err(|e| format!("纠删码编码失败: {}", e))?;
        // let vc_root = encoded.merkle_root();
        // let vc_tx = vc_root.to_vec();
        // self.originated_vcs.insert(vc_tx.clone());

        let originated_count = self.originated_vcs.len();
        info!(
            "[Sequencing] Node {} broadcast *SEQ-REQUEST* k={}, originated_vcs.len() = {}",
            self.process_id, s, originated_count
        );

        Ok(())
    }

    // Handle SEQ-REQUEST message - Lines 4-17
    pub async fn handle_seq_request(
        &self,
        sender: usize,
        req: SeqRequest,
        encoded_package: ErasurePackage,
        wait_duration: Duration,
        encode_duration: Duration,
    ) -> Result<Option<TransactionEntry>, String> {
        info!(
            "📥 [Sequencing] Line 2:4: received SEQ-REQUEST, node {} seq_num: {} tx={}",
            sender,
            req.seq_num,
            hex::encode(&req.tx.payload[..std::cmp::min(8, req.tx.payload.len())])
        );

        let vc_root = encoded_package.merkle_root();
        let vc_tx = vc_root.to_vec();
        let vc_key = VC::from_slice(&vc_tx);

        self.buf.insert(vc_key);

        // Assign sequence number (Lines 7-11)
        let s = match self.tx_sequence_map.entry(vc_key) {
            Entry::Occupied(entry) => *entry.get(),
            Entry::Vacant(entry) => {
                let assigned_s = self.local_seq.fetch_add(1, Ordering::SeqCst);
                debug!(
                    "🧮 [Sequencing] Line 2:7-11 node={} assigned new sequence {}",
                    self.process_id, assigned_s
                );
                entry.insert(assigned_s);
                assigned_s
            }
        };

        debug!(
            "🧮 [Sequencing] Line 2:7-11 node={} assigned sequence {} for vc={} (req_seq={} from {})",
            self.process_id,
            s,
            hex::encode(&vc_tx[..std::cmp::min(8, vc_tx.len())]),
            req.seq_num,
            sender
        );
        let process_id = self.process_id;

        // Persist local mappings for later reconstruction and consensus input
        self.pending_txs
            .entry(vc_key)
            .or_insert_with(|| req.tx.clone());
        self.erasure_store
            .entry(vc_key)
            .or_insert(encoded_package.clone());

        if sender == self.process_id {
            self.originated_vcs.insert(vc_key);
            debug!(
                "[Sequencing] Node {} added to originated_vcs, now has {} vcs",
                self.process_id,
                self.originated_vcs.len()
            );
        }

        let pending_finals = self
            .pending_seq_finals
            .remove(&vc_key)
            .map(|(_, finals)| finals);

        // Input to PNFIFO-BC (Line 15)
        let enqueue_start = Instant::now();
        let queue_result = self.pnfifo.broadcast(s, vc_tx.clone()).await;
        let enqueue_delay = enqueue_start.elapsed();

        debug!(
            "[Sequencing-Timing] ⏰ FIFO broadcast enqueued after {:?}, wait {:?}, encoding {:?}.",
            enqueue_delay, wait_duration, encode_duration,
        );

        match queue_result {
            Ok(_) => {
                debug!(
                    "📡 [Sequencing] Line 2:15 node={} forwarded req.seq_num {} vc to PNFIFO slot {}",
                    process_id, req.seq_num, s
                );
            }
            Err(err) => {
                warn!("❌ [Sequencing] PNFIFO broadcast enqueue failed: {}", err);
            }
        }

        // Sign and respond (Lines 16-17)
        let message = format!("sequence:{}:{}", hex::encode(&vc_tx), s);
        let sigma = self
            .threshold_share
            .sign(message.as_bytes())
            .to_bytes()
            .to_vec();

        let response_msg = SmrolMessage::SeqResponse {
            vc: vc_tx.clone(),
            signature_share: sigma.clone(),
            sender_id: self.process_id,
            sequence_number: s,
        };
        self.network
            .send_to_node(sender, response_msg)
            .await
            .map_err(|e| format!("发送SEQ-RESPONSE失败: {}", e))?;
        info!(
            "[Sequencing] Sent *SEQ-RESPONSE* to {}, s={}, tx={}",
            sender,
            s,
            hex::encode(&req.tx.payload[..std::cmp::min(8, req.tx.payload.len())])
        );

        // Check if we have deferred FINAL messages waiting for this vc
        let mut finalized_entry: Option<TransactionEntry> = None;
        if let Some(mut pending_finals) = pending_finals {
            // process in arrival order
            for final_msg in pending_finals.drain(..) {
                if let Some(entry) = self.finalize_ready_final(final_msg).await {
                    finalized_entry = Some(entry);
                    break;
                }
            }
        }

        Ok(finalized_entry)
    }

    // Handle SEQ-RESPONSE message - Lines 18-23
    pub async fn handle_seq_response(
        &self,
        sender: usize,
        resp: SeqResponse,
    ) -> Result<(), String> {
        // point to point so skip the check
        // if self.originated_vcs.contains(&resp.vc) {
        info!(
            "📥 [Sequencing] received SEQ-RESPONSE from Node {} as leader",
            sender
        );
        // Original SEQ-REQUEST sender collects sequence responses (Algorithm 2, line 19)
        if self.verify_signature(&resp, sender)? {
            let vc_key = VC::from_slice(&resp.vc);
            if self.completed_responses.contains(&vc_key) {
                debug!(
                    "🧾 [Sequencing] node={} already satisfied response threshold for vc={}, ignoring duplicate",
                    self.process_id,
                    hex::encode(&resp.vc[..std::cmp::min(8, resp.vc.len())])
                );
                return Ok(());
            }
            let threshold = 2 * self.f + 1;
            let mut maybe_records: Option<Vec<SeqResponseRecord>> = None;
            let collected;

            match self.response_shares.entry(vc_key) {
                Entry::Occupied(mut occ) => {
                    let records = occ.get_mut();
                    if records.iter().any(|r| r.sender == sender) {
                        collected = records.len();
                    } else if records.len() >= threshold {
                        collected = records.len();
                    } else {
                        records.push(SeqResponseRecord {
                            sender,
                            sequence: resp.s,
                            signature: resp.sigma.clone(),
                        });
                        collected = records.len();
                        if collected == threshold {
                            let taken = std::mem::take(records);
                            maybe_records = Some(taken);
                        }
                    }
                }
                Entry::Vacant(vac) => {
                    let mut vec = Vec::with_capacity(threshold);
                    vec.push(SeqResponseRecord {
                        sender,
                        sequence: resp.s,
                        signature: resp.sigma.clone(),
                    });
                    collected = vec.len();
                    if collected == threshold {
                        maybe_records = Some(vec);
                    } else {
                        vac.insert(vec);
                    }
                }
            }

            if collected > 2 * self.f + 1 {
                return Ok(());
            }
            debug!(
                "🧾 [Sequencing] node={} collected {} / {} responses for vc={}",
                self.process_id,
                collected,
                2 * self.f + 1,
                hex::encode(&resp.vc[..std::cmp::min(8, resp.vc.len())])
            );

            // Check if collected 2f+1 sequences (Line 22)
            if let Some(records) = maybe_records {
                self.completed_responses.insert(vc_key);
                self.response_shares.remove(&vc_key);
                let network_records: Vec<(usize, u64, Vec<u8>)> = records
                    .iter()
                    .map(|r| (r.sender, r.sequence, r.signature.clone()))
                    .collect();
                let order_msg = SmrolMessage::SeqOrder {
                    vc: resp.vc.clone(),
                    responses: network_records,
                    sender_id: self.process_id,
                };
                self.network
                    .broadcast(order_msg)
                    .await
                    .map_err(|e| format!("广播SEQ-ORDER失败: {}", e))?;
                info!(
                    "📤 [Sequencing] node={} broadcasting *SEQ-ORDER* for vc={}, s={}",
                    self.process_id,
                    hex::encode(&resp.vc[..std::cmp::min(8, resp.vc.len())]),
                    resp.s
                );
            }
        } else {
            warn!(
                "❌ [Sequencing] Invalid signature in SEQ-RESPONSE from Node {}",
                sender
            );
        }
        // }
        Ok(())
    }

    // Handle SEQ-ORDER message - Lines 24-28
    pub async fn handle_seq_order(&self, sender: usize, order: SeqOrder) -> Result<(), String> {
        info!("📥 [Sequencing] Node {} received SEQ-ORDER", sender);

        if self.verify_seq_order(&order) {
            let sequences: Vec<u64> = order.records.iter().map(|r| r.sequence).collect();
            let median = self.calculate_median(&sequences); // line 26
            debug!(
                "✅ [Sequencing] Verified SEQ-ORDER from Node {} with median sequence {}",
                sender, median
            );

            debug!(
                "📊 [Sequencing] node={} seq_order vc={} median={} records={:?}",
                self.process_id,
                hex::encode(&order.vc[..std::cmp::min(8, order.vc.len())]),
                median,
                sequences
            );

            // Create threshold signature share
            let message = format!("median:{}:{}", median, hex::encode(&order.vc));
            let sigma_seq = self
                .threshold_share
                .sign(message.as_bytes())
                .to_bytes()
                .to_vec();

            let median_msg = SmrolMessage::SeqMedian {
                vc: order.vc.clone(),
                median_sequence: median,
                proof: sigma_seq,
                sender_id: self.process_id,
            };
            self.network
                .send_to_node(sender, median_msg)
                .await
                .map_err(|e| format!("发送SEQ-MEDIAN失败: {}", e))?;
            info!(
                "[Sequencing] Node={} sent *SEQ-MEDIAN* vc={} median={}",
                self.process_id,
                hex::encode(&order.vc[..std::cmp::min(8, order.vc.len())]),
                median,
            );
        }
        Ok(())
    }

    // Handle SEQ-MEDIAN message - Lines 29-35
    pub async fn handle_seq_median(&self, sender: usize, median: SeqMedian) -> Result<(), String> {
        info!("📥 [Sequencing] received SEQ-MEDIAN from {}", sender);
        // point to point so skip the check
        // if self.originated_vcs.contains(&median.vc) {
        // Original SEQ-REQUEST sender gathers median shares (Algorithm 2, line 30)
        if self.verify_threshold_share(&median, sender)? {
            let vc_key = VC::from_slice(&median.vc);
            let threshold = self.f + 1;

            // line 31
            let mut share_bytes = [0u8; SIG_SIZE];
            share_bytes.copy_from_slice(&median.sigma_seq);
            let share = SignatureShare::from_bytes(&share_bytes)
                .map_err(|e| format!("无法解析threshold share: {}", e))?;

            let mut ready_to_broadcast: Option<Vec<(usize, SignatureShare)>> = None;
            let entry_len = match self.median_shares.entry(vc_key) {
                Entry::Occupied(mut occ) => {
                    let map = occ.get_mut();
                    if map.contains_key(&sender) {
                        map.len()
                    } else if map.len() >= threshold {
                        map.len()
                    } else {
                        map.insert(sender, share);
                        let len = map.len();
                        if len == threshold {
                            let taken_map = std::mem::take(map);
                            occ.remove();
                            ready_to_broadcast = Some(taken_map.into_iter().collect::<Vec<(
                                usize,
                                SignatureShare,
                            )>>(
                            ));
                        }
                        len
                    }
                }
                Entry::Vacant(vac) => {
                    let mut map = HashMap::with_capacity(threshold);
                    map.insert(sender, share);
                    let len = map.len();
                    if len == threshold {
                        ready_to_broadcast =
                            Some(map.into_iter().collect::<Vec<(usize, SignatureShare)>>());
                        len
                    } else {
                        vac.insert(map);
                        len
                    }
                }
            };

            debug!(
                "🔑 [Sequencing] node={} stored median share {}/{} for vc={} s_tx={} from {}",
                self.process_id,
                entry_len,
                self.f + 1,
                hex::encode(&median.vc[..std::cmp::min(8, median.vc.len())]),
                median.s_tx,
                sender
            );

            if let Some(shares) = ready_to_broadcast {
                let combined_sig = self
                    .threshold_public
                    .combine_signatures(shares.iter().map(|(id, share)| (*id, share)))
                    .map_err(|e| format!("阈值签名组合失败: {}", e))?;

                debug!(
                    "🔐 [Sequencing] node={} collected {} median shares for vc={} (s_tx={})",
                    self.process_id,
                    shares.len(),
                    hex::encode(&median.vc[..std::cmp::min(8, median.vc.len())]),
                    median.s_tx
                );

                let sigma_bytes = combined_sig.to_bytes().to_vec();
                let seq_final = SeqFinal {
                    vc: median.vc.clone(),
                    s_tx: median.s_tx,
                    sigma: sigma_bytes.clone(),
                };
                let final_msg = SmrolMessage::SeqFinal {
                    vc: seq_final.vc.clone(),
                    final_sequence: seq_final.s_tx,
                    combined_signature: sigma_bytes,
                    sender_id: self.process_id,
                };
                self.network
                    .broadcast(final_msg)
                    .await
                    .map_err(|e| format!("广播SEQ-FINAL失败: {}", e))?;
                info!(
                    "[Sequencing] Node {} broadcast *SEQ-FINAL* {} for vc = {:?}",
                    self.process_id,
                    sender,
                    hex::encode(&median.vc[..std::cmp::min(8, median.vc.len())])
                );
            }
        }
        // }
        Ok(())
    }

    // Handle SEQ-FINAL message - Lines 36-38
    pub async fn handle_seq_final(
        &self,
        final_msg: SeqFinal,
    ) -> Result<Option<TransactionEntry>, String> {
        let vc_key = VC::from_slice(&final_msg.vc);
        let has_payload =
            self.pending_txs.contains_key(&vc_key) || self.erasure_store.contains_key(&vc_key);

        info!(
            "📥 [Sequencing] Node {} received SEQ-FINAL, vc={}, pending_txs contains: {}",
            self.process_id,
            hex::encode(&final_msg.vc[..8]),
            has_payload
        );
        debug!(
            "[Sequencing] SEQ-FINAL vc={}, pending_txs contains: {}",
            hex::encode(&final_msg.vc[..8]),
            has_payload
        );

        // NOTE: 改成原子操作？
        if self.verify_combined_signature(&final_msg)? {
            let (in_vc_ledger, in_mi) = {
                let finalization = self.finalization.lock().await;
                (
                    finalization.is_in_vc_ledger(&final_msg.vc),
                    finalization.is_in_mi(&final_msg.vc),
                )
            };

            if in_vc_ledger || in_mi {
                debug!(
                    "ℹ️ [Sequencing] SEQ-FINAL vc={} already finalized, ignoring",
                    hex::encode(&final_msg.vc[..std::cmp::min(8, final_msg.vc.len())])
                );
                return Ok(None);
            }

            Ok(self.finalize_ready_final(final_msg).await)
        } else {
            Ok(None)
        }
    }

    // Helper functions
    fn encode_transaction(
        &self,
        tx: &Transaction,
        data_shards: usize,
    ) -> Result<ErasurePackage, String> {
        let data_shards = std::cmp::max(1, data_shards);
        let total_shards = std::cmp::max(data_shards, self.n);
        ErasurePackage::encode(&tx.payload, data_shards, total_shards)
    }

    fn create_vector_commitment(&self, encoded: &ErasurePackage) -> Vec<u8> {
        encoded.merkle_root().to_vec()
    }

    pub async fn wait_for_log_condition_static(
        pnfifo: &Arc<PnfifoBc>,
        process_id: usize,
        leader_id: usize,
        seq_num: u64,
    ) -> (bool, Duration) {
        if seq_num <= 1 {
            return (true, Duration::from_millis(0));
        }
        let target_slot = seq_num - 1;
        debug!(
            "⏱️ [Sequencing] wait_for_log_condition - start wait_for_output: leader {}, target_slot {}",
            leader_id, target_slot
        );
        let t0 = Instant::now();

        let timeout_ms: u64 = std::env::var("SMROL_LOG_GUARD_TIMEOUT_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(50);

        let waited_ok = match timeout(
            Duration::from_millis(timeout_ms),
            pnfifo.wait_for_output(leader_id, target_slot),
        )
        .await
        {
            Ok(_) => true,
            Err(_) => {
                warn!(
                    "⏳ [Sequencing] wait_for_log_condition timed out after {}ms for leader {} slot {} — proceeding",
                    timeout_ms, leader_id, target_slot
                );
                true
            }
        };

        let wait = t0.elapsed();
        info!(
            "⏱️ [Sequencing] Node {} wait_for_log_condition end: leader {} target_slot {} waited {:?}",
            process_id,
            leader_id,
            target_slot,
            wait
        );
        (waited_ok, wait)
    }

    fn verify_signature(&self, resp: &SeqResponse, sender: usize) -> Result<bool, String> {
        if resp.sigma.len() != SIG_SIZE {
            return Err(format!(
                "signature share length invalid: {}",
                resp.sigma.len()
            ));
        }

        let mut share_bytes = [0u8; SIG_SIZE];
        share_bytes.copy_from_slice(&resp.sigma);
        let share = SignatureShare::from_bytes(&share_bytes)
            .map_err(|e| format!("无法解析signature share: {}", e))?;

        let pk_share = self.threshold_public.public_key_share(sender);
        let message = format!("sequence:{}:{}", hex::encode(&resp.vc), resp.s);

        Ok(pk_share.verify(&share, message.as_bytes()))
    }

    fn verify_seq_order(&self, order: &SeqOrder) -> bool {
        if order.records.len() != 2 * self.f + 1 {
            return false;
        }

        for record in &order.records {
            if record.signature.len() != SIG_SIZE {
                warn!(
                    "❌ [Sequencing] Invalid signature length in SeqOrder from node {}",
                    record.sender
                );
                return false;
            }

            let mut share_bytes = [0u8; SIG_SIZE];
            share_bytes.copy_from_slice(&record.signature);
            let share = match SignatureShare::from_bytes(&share_bytes) {
                Ok(share) => share,
                Err(e) => {
                    warn!(
                        "❌ [Sequencing] Failed to parse signature share from node {}: {}",
                        record.sender, e
                    );
                    return false;
                }
            };

            let pk_share = self.threshold_public.public_key_share(record.sender);
            let message = format!("sequence:{}:{}", hex::encode(&order.vc), record.sequence);

            if !pk_share.verify(&share, message.as_bytes()) {
                warn!(
                    "❌ [Sequencing] Invalid signature share from node {} in SeqOrder",
                    record.sender
                );
                return false;
            }
        }

        true
    }

    fn verify_threshold_share(&self, median: &SeqMedian, sender: usize) -> Result<bool, String> {
        if median.sigma_seq.len() != SIG_SIZE {
            return Err(format!(
                "threshold signature share length invalid: {}",
                median.sigma_seq.len()
            ));
        }

        let mut share_bytes = [0u8; SIG_SIZE];
        share_bytes.copy_from_slice(&median.sigma_seq);
        let share = SignatureShare::from_bytes(&share_bytes)
            .map_err(|e| format!("无法解析threshold share: {}", e))?;

        let pk_share = self.threshold_public.public_key_share(sender);
        let message = format!("median:{}:{}", median.s_tx, hex::encode(&median.vc));

        Ok(pk_share.verify(&share, message.as_bytes()))
    }

    fn verify_combined_signature(&self, final_msg: &SeqFinal) -> Result<bool, String> {
        if final_msg.sigma.len() != SIG_SIZE {
            return Err(format!(
                "combined signature length invalid: {}",
                final_msg.sigma.len()
            ));
        }

        let mut sig_bytes = [0u8; SIG_SIZE];
        sig_bytes.copy_from_slice(&final_msg.sigma);
        let signature =
            Signature::from_bytes(&sig_bytes).map_err(|e| format!("无法解析组合签名: {}", e))?;

        let message = format!("median:{}:{}", final_msg.s_tx, hex::encode(&final_msg.vc));
        Ok(self
            .threshold_public
            .public_key()
            .verify(&signature, message.as_bytes()))
    }

    fn calculate_median(&self, s_vec: &[u64]) -> u64 {
        let mut sorted = s_vec.to_vec();
        sorted.sort();
        sorted[sorted.len() / 2]
    }

    // Public stats methods
    pub async fn get_pending_count(&self) -> usize {
        self.pending_txs.len()
    }

    pub async fn get_current_seq(&self) -> u64 {
        self.local_seq.load(Ordering::SeqCst).saturating_sub(1)
    }

    fn state_limit() -> usize {
        std::env::var("SMROL_SEQ_STATE_MAX_ENTRIES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&v| v > 0)
            .unwrap_or(500)
    }

    fn cleanup_log_prefix() -> &'static str {
        "🧹 [Sequencing] cleanup"
    }

    pub async fn cleanup_expired_state(&self) {
        let limit = Self::state_limit();

        if self.pending_txs.len() > limit {
            let removed = self.pending_txs.len();
            self.pending_txs.clear();
            self.erasure_store.clear();
            self.pending_seq_finals.clear();
            debug!(
                "{} cleared {} pending_txs entries (and related caches)",
                Self::cleanup_log_prefix(),
                removed
            );
        }

        if self.erasure_store.len() > limit {
            let removed = self.erasure_store.len();
            self.erasure_store.clear();
            debug!(
                "{} cleared {} erasure_store entries",
                Self::cleanup_log_prefix(),
                removed
            );
        }

        if self.pending_seq_finals.len() > limit {
            let removed = self.pending_seq_finals.len();
            self.pending_seq_finals.clear();
            debug!(
                "{} cleared {} pending_seq_finals entries",
                Self::cleanup_log_prefix(),
                removed
            );
        }

        if self.originated_vcs.len() > limit {
            let removed = self.originated_vcs.len();
            self.originated_vcs.clear();
            debug!(
                "{} cleared {} originated_vcs entries",
                Self::cleanup_log_prefix(),
                removed
            );
        }

        if self.response_shares.len() > limit {
            let removed = self.response_shares.len();
            self.response_shares.clear();
            debug!(
                "{} cleared {} response entries",
                Self::cleanup_log_prefix(),
                removed
            );
        }

        if self.completed_responses.len() > limit {
            let removed = self.completed_responses.len();
            self.completed_responses.clear();
            debug!(
                "{} cleared {} completed response markers",
                Self::cleanup_log_prefix(),
                removed
            );
        }

        if self.median_shares.len() > limit {
            let removed = self.median_shares.len();
            self.median_shares.clear();
            debug!(
                "{} cleared {} median entries",
                Self::cleanup_log_prefix(),
                removed
            );
        }

        if self.tx_sequence_map.len() > limit {
            let removed = self.tx_sequence_map.len();
            self.tx_sequence_map.clear();
            debug!(
                "{} cleared {} tx_sequence_map entries",
                Self::cleanup_log_prefix(),
                removed
            );
        }

        if self.buf.len() > limit {
            let removed = self.buf.len();
            self.buf.clear();
            debug!(
                "{} cleared {} buffered vcs",
                Self::cleanup_log_prefix(),
                removed
            );
        }
    }

    async fn store_pending_final(&self, final_msg: SeqFinal) {
        info!(
            "⏳ [Sequencing] node={} 缓存SEQ-FINAL等待载荷: vc={} s_tx={}",
            self.process_id,
            hex::encode(&final_msg.vc[..std::cmp::min(8, final_msg.vc.len())]),
            final_msg.s_tx
        );
        let vc_key = VC::from_slice(&final_msg.vc);
        self.pending_seq_finals
            .entry(vc_key)
            .or_insert_with(Vec::new)
            .push(final_msg);
    }

    async fn finalize_ready_final(&self, final_msg: SeqFinal) -> Option<TransactionEntry> {
        let vc_key = VC::from_slice(&final_msg.vc);

        let payload = if let Some((_, tx)) = self.pending_txs.remove(&vc_key) {
            tx.payload
        } else {
            let reconstructed = self
                .erasure_store
                .get(&vc_key)
                .and_then(|pkg| pkg.reconstruct_full().ok());

            if let Some(bytes) = reconstructed {
                self.erasure_store.remove(&vc_key);
                bytes
            } else {
                self.store_pending_final(final_msg).await;
                return None;
            }
        };

        self.pending_seq_finals.remove(&vc_key);
        self.originated_vcs.remove(&vc_key);
        self.completed_responses.remove(&vc_key);

        let entry = TransactionEntry {
            vc_tx: final_msg.vc.clone(),
            s_tx: final_msg.s_tx,
            sigma: final_msg.sigma.clone(),
            payload,
        };

        debug!(
            "✅ [Sequencing] Finalized VC forwarded to consensus: vc_len={}, s_tx={}",
            entry.vc_tx.len(),
            entry.s_tx
        );
        debug!(
            "🎯 [Sequencing] node={} finalizing vc={} s_tx={}",
            self.process_id,
            hex::encode(&entry.vc_tx[..std::cmp::min(8, entry.vc_tx.len())]),
            entry.s_tx
        );

        Some(entry)
    }
}
