use crate::smrol::{
    consensus::TransactionEntry,
    crypto::ErasurePackage,
    finalization::OutputFinalization,
    message::{SmrolMessage, SmrolTransaction},
    network::SmrolTcpNetwork,
    pnfifo::PnfifoBc,
};
use bincode::de;
use serde::{Deserialize, Serialize};
use tracing_subscriber::field::debug;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use threshold_crypto::{PublicKeySet, SecretKeyShare, Signature, SignatureShare, SIG_SIZE};
use tokio::{
    sync::{mpsc, Mutex, RwLock},
    time::{sleep, Duration, timeout},
};
use tracing::{debug, error, info, warn};

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

#[derive(Debug)]
pub struct TransactionSequencing {
    pub seq_i: u64,
    pub buf: HashSet<Vec<u8>>,
    pub k: u64,
    pub f: usize,
    pub n: usize,
    pub process_id: usize, // node_id
    pub network: Arc<SmrolTcpNetwork>,
    pub pnfifo: Arc<PnfifoBc>,
    pub threshold_share: SecretKeyShare,
    pub threshold_public: PublicKeySet,
    pub finalization: Arc<Mutex<OutputFinalization>>,
    pub pending_txs: HashMap<Vec<u8>, Transaction>,
    pub s_vec_map: HashMap<Vec<u8>, Vec<SeqResponseRecord>>,
    pub threshold_sigs: HashMap<u64, BTreeMap<usize, SignatureShare>>, // line 32: S[\bar{s}_tx] -> (j, sigma_seq_j)
    pub erasure_store: HashMap<Vec<u8>, ErasurePackage>,
    pub tx_sequence_map: HashMap<Vec<u8>, u64>,
    pub originated_vcs: HashSet<Vec<u8>>,
    pub pending_seq_finals: HashMap<Vec<u8>, Vec<SeqFinal>>,
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
            seq_i: 1,
            buf: HashSet::new(),
            k: 1,
            f,
            n,
            process_id,
            network,
            pnfifo,
            threshold_share,
            threshold_public,
            finalization,
            pending_txs: HashMap::new(),
            s_vec_map: HashMap::new(),
            threshold_sigs: HashMap::new(),
            erasure_store: HashMap::new(),
            tx_sequence_map: HashMap::new(),
            originated_vcs: HashSet::new(),
            pending_seq_finals: HashMap::new(),
        }
    }

    // Function SMROL-broadcast(k, tx) - Line 1-3
    pub async fn smrol_broadcast(&mut self, tx: SmrolTransaction) -> Result<(), String> {
        let s = self.k; // Get current sequence number k
        self.k += 1;

        // debug!(
        //     "🚀 [Sequencing] node={} generate SEQ-REQUEST (k={}, tx_id={})",
        //     self.process_id, s, tx.id
        // );

        let payload =
            bincode::serialize(&tx).map_err(|e| format!("序列化SmrolTransaction失败: {}", e))?;
        let payload_clone=payload.clone();

        let seq_request = SeqRequest {
            seq_num: s,
            tx: Transaction { payload },
        };
        self.network.multicast_seq_request(seq_request).await?; // Line 3： seq_request(seq_i, tx_serialized)

        //debug: insert to originated vc earlier but with additional computation
        let data_shards = std::cmp::max(1, self.f + 1);
        let total_shards = std::cmp::max(data_shards, self.n);
        let encoded = ErasurePackage::encode(&payload_clone, data_shards, total_shards)
            .map_err(|e| format!("纠删码编码失败: {}", e))?;
        let vc_root = encoded.merkle_root();
        let vc_tx = vc_root.to_vec();
        self.originated_vcs.insert(vc_tx.clone());

        info!("[Sequencing] Node {} broadcast *SEQ-REQUEST* k={}, originated_vcs.len() = {}, vc={}",
            self.process_id, s, self.originated_vcs.len(), hex::encode(&vc_tx[..std::cmp::min(8, vc_tx.len())]));

        Ok(())
    }

    // Handle SEQ-REQUEST message - Lines 4-17
    pub async fn handle_seq_request(
        &mut self,
        sender: usize,
        req: SeqRequest,
    ) -> Result<Option<TransactionEntry>, String> {
        info!(
            "📥 [Sequencing] Line 2:4: received SEQ-REQUEST, node {} seq_num: {} tx={}",
            sender, req.seq_num, hex::encode(&req.tx.payload[..std::cmp::min(8, req.tx.payload.len())])
        );
        let wait_time = Instant::now();

        // Avoid downgrade attack (Line 5)
        if !self.wait_for_log_condition(sender, req.seq_num).await {
            warn!("❌ [Sequencing] Log condition verification failed");
            return Ok(None);
        }

        debug!(
            "✅ [Sequencing] Line 2:5: Log condition verified for SEQ-REQUEST from {} with seq_num {}. Continue to Line 12-14",
            sender, req.seq_num
        );

        let encode_time=Instant::now();

        // Encode transaction with Reed-Solomon erasure coding (Lines 12-14)
        let data_shards = std::cmp::max(1, self.f + 1);
        let total_shards = std::cmp::max(data_shards, self.n);
        let encoded = ErasurePackage::encode(&req.tx.payload, data_shards, total_shards)
            .map_err(|e| format!("纠删码编码失败: {}", e))?;

        let vc_root = encoded.merkle_root();
        let vc_tx = vc_root.to_vec();
        self.buf.insert(vc_tx.clone());

        // Assign sequence number (Lines 7-11)
        let s = if let Some(existing) = self.tx_sequence_map.get(&vc_tx) {
            *existing
        } else {
            let assigned_s = self.seq_i;
            self.seq_i += 1;
            debug!(
                "🧮 [Sequencing] Line 2:7-11 node={} local seq_i={}, just assigned {}",
                self.process_id,
                self.seq_i,
                assigned_s
            );
            self.tx_sequence_map.insert(vc_tx.clone(), assigned_s);
            assigned_s
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
            .entry(vc_tx.clone())
            .or_insert_with(|| req.tx.clone());
        self.erasure_store
            .entry(vc_tx.clone())
            .or_insert(encoded.clone());

        if sender == self.process_id {
            self.originated_vcs.insert(vc_tx.clone());
            debug!("[Sequencing] Node {} added to originated_vcs, now has {} vcs",
            self.process_id, self.originated_vcs.len());// *
        }

        // Input to PNFIFO-BC (Line 15)
        let enqueue_start = Instant::now();
        let wait = wait_time.elapsed();
        let encode = encode_time.elapsed();
        let queue_result = self.pnfifo.broadcast(s, vc_tx.clone()).await;
        let enqueue_delay = enqueue_start.elapsed();

        debug!(
            "[Sequencing-Timing] ⏰ FIFO broadcast enqueued after {:?}, wait {:?}, encoding {:?}.",
            enqueue_delay,
            wait,
            encode,
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

        let response = SeqResponse {
            vc: vc_tx.clone(),
            s,
            sigma,
        };
        // Broadcast SEQ-RESPONSE so the requester can collect 2f+1 quickly
        self.network.send_seq_response(sender,response).await?;
        info!("[Sequencing] Sent *SEQ-RESPONSE* to {}, s={}, tx={}", sender, s, hex::encode(&req.tx.payload[..std::cmp::min(8, req.tx.payload.len())]));

        // Check if we have deferred FINAL messages waiting for this vc
        let mut finalized_entry: Option<TransactionEntry> = None;
        if let Some(mut pending_finals) = self.pending_seq_finals.remove(&vc_tx) {
            // process in arrival order
            for final_msg in pending_finals.drain(..) {
                if let Some(entry) = self.finalize_ready_final(final_msg) {
                    finalized_entry = Some(entry);
                    break;
                }
            }
        }

        Ok(finalized_entry)
    }

    // Handle SEQ-RESPONSE message - Lines 18-23
    pub async fn handle_seq_response(
        &mut self,
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
                // Collect sequence numbers for tx (Line 21)
                self.s_vec_map
                    .entry(resp.vc.clone())
                    .or_insert_with(Vec::new)
                    .push(SeqResponseRecord {
                        sender,
                        sequence: resp.s,
                        signature: resp.sigma.clone(),
                    });

                let collected = self.s_vec_map[&resp.vc].len();
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
                if collected == 2 * self.f + 1 {
                    let records = self.s_vec_map[&resp.vc].clone();
                    let seq_order = SeqOrder {
                        vc: resp.vc.clone(),
                        records,
                    };
                    self.network.multicast_seq_order(seq_order).await?;
                    info!(
                        "📤 [Sequencing] node={} broadcasting *SEQ-ORDER* for vc={}, s={}",
                        self.process_id,
                        hex::encode(&resp.vc[..std::cmp::min(8, resp.vc.len())]),
                        resp.s
                    );
                }
            }else {
                warn!("❌ [Sequencing] Invalid signature in SEQ-RESPONSE from Node {}", sender);
            }
        // }
        Ok(())
    }

    // Handle SEQ-ORDER message - Lines 24-28
    pub async fn handle_seq_order(&mut self, sender: usize, order: SeqOrder) -> Result<(), String> {
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

            let seq_median = SeqMedian {
                vc: order.vc.clone(),
                s_tx: median,
                sigma_seq,
            };
            self.network.send_seq_median(sender, seq_median).await?;
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
    pub async fn handle_seq_median(
        &mut self,
        sender: usize,
        median: SeqMedian,
    ) -> Result<(), String> {
        info!("📥 [Sequencing] received SEQ-MEDIAN from {}", sender);
        // point to point so skip the check
        // if self.originated_vcs.contains(&median.vc) {
            // Original SEQ-REQUEST sender gathers median shares (Algorithm 2, line 30)
            if self.verify_threshold_share(&median, sender)? {
                // line 31
                let mut share_bytes = [0u8; SIG_SIZE];
                share_bytes.copy_from_slice(&median.sigma_seq);
                let share = SignatureShare::from_bytes(&share_bytes)
                    .map_err(|e| format!("无法解析threshold share: {}", e))?;

                // Collect shares for threshold signature of entry [median.vc] (Line 32)
                let entry = self
                    .threshold_sigs
                    .entry(median.s_tx)
                    .or_insert_with(BTreeMap::new);
                entry.insert(sender, share);

                debug!(
                    "🔑 [Sequencing] node={} stored median share {}/{} for vc={} s_tx={} from {}",
                    self.process_id,
                    entry.len(),
                    self.f + 1,
                    hex::encode(&median.vc[..std::cmp::min(8, median.vc.len())]),
                    median.s_tx,
                    sender
                );

                if entry.len() == self.f + 1 {
                    let combined_sig = self
                        .threshold_public
                        .combine_signatures(entry.iter().map(|(id, share)| (*id, share)))
                        .map_err(|e| format!("阈值签名组合失败: {}", e))?;

                    debug!(
                        "🔐 [Sequencing] node={} collected {} median shares for vc={} (s_tx={})",
                        self.process_id,
                        entry.len(),
                        hex::encode(&median.vc[..std::cmp::min(8, median.vc.len())]),
                        median.s_tx
                    );

                    let seq_final = SeqFinal {
                        vc: median.vc.clone(),
                        s_tx: median.s_tx,
                        sigma: combined_sig.to_bytes().to_vec(),
                    };
                    self.network.multicast_seq_final(seq_final).await?;
                    info!("[Sequencing] Node {} broadcast *SEQ-FINAL* {} for vc = {:?}",self.process_id, sender, hex::encode(&median.vc[..std::cmp::min(8, median.vc.len())]));
                    // 清理已使用的shares，避免重复广播
                    self.threshold_sigs.remove(&median.s_tx);
                }
            }
        // }
        Ok(())
    }

    // Handle SEQ-FINAL message - Lines 36-38
    pub async fn handle_seq_final(
        &mut self,
        final_msg: SeqFinal,
    ) -> Result<Option<TransactionEntry>, String> {
        info!("📥 [Sequencing] Node {} received SEQ-FINAL, vc={}, pending_txs contains: {}", self.process_id, hex::encode(&final_msg.vc[..8]), self.pending_txs.contains_key(&final_msg.vc));
        debug!("[Sequencing] SEQ-FINAL vc={}, pending_txs contains: {}",
            hex::encode(&final_msg.vc[..8]),
            self.pending_txs.contains_key(&final_msg.vc));

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

            if !self.pending_txs.contains_key(&final_msg.vc) {
                info!(
                    "⏳ [Sequencing] node={} SEQ-FINAL vc={} not ready, caching for later processing",
                    self.process_id,
                    hex::encode(&final_msg.vc[..std::cmp::min(8, final_msg.vc.len())])
                );
                self.store_pending_final(final_msg);
                return Ok(None);
            }

            Ok(self.finalize_ready_final(final_msg))
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

    async fn wait_for_log_condition(&self, leader_id: usize, seq_num: u64) -> bool {
        if seq_num <= 1 {
            return true;
        }
        let target_slot = seq_num - 1;
        debug!(
            "⏱️ [Sequencing] wait_for_log_condition - start wait_for_output: leader {}, target_slot {}",
            leader_id, target_slot
        );
        let t0=Instant::now();

        // configurable soft timeout to avoid head-of-line blocking under load
        let timeout_ms: u64 = std::env::var("SMROL_LOG_GUARD_TIMEOUT_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(50);

        let waited_ok = match timeout(Duration::from_millis(timeout_ms), self.pnfifo.wait_for_output(leader_id, target_slot)).await {
            Ok(_) => true,
            Err(_) => {
                warn!(
                    "⏳ [Sequencing] wait_for_log_condition timed out after {}ms for leader {} slot {} — proceeding",
                    timeout_ms, leader_id, target_slot
                );
                true
            }
        };

        let wait=t0.elapsed();
        info!(
            "⏱️ [Sequencing] wait_for_log_condition - end: leader {} target_slot {} waited {:?}",
            leader_id, target_slot, wait
        );
        waited_ok
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

    fn add_to_mi(&mut self, vc: &[u8], _s_tx: &u64) {
        if self.pending_txs.contains_key(vc) {
            return;
        }

        let payload = self
            .erasure_store
            .get(vc)
            .and_then(|pkg| pkg.reconstruct_full().ok())
            .unwrap_or_else(|| vc.to_vec());

        self.pending_txs
            .entry(vc.to_vec())
            .or_insert(Transaction { payload });
    }

    // Public stats methods
    pub fn get_pending_count(&self) -> usize {
        self.pending_txs.len()
    }

    pub fn get_current_seq(&self) -> u64 {
        self.seq_i
    }

    async fn handle_smrol_message_inner(
        &mut self,
        sender_id: usize,
        message: SmrolMessage,
    ) -> Result<Option<TransactionEntry>, String> {
        match message {
            SmrolMessage::SeqRequest {
                tx_hash: _,
                transaction,
                sender_id: _,
                sequence_number,
            } => {
                let payload = bincode::serialize(&transaction)
                    .map_err(|e| format!("序列化SmrolTransaction失败: {}", e))?;
                let req = SeqRequest {
                    seq_num: sequence_number,
                    tx: Transaction { payload },
                };
                self.handle_seq_request(sender_id, req).await
            }
            SmrolMessage::SeqResponse {
                vc,
                signature_share,
                sender_id: _,
                sequence_number,
            } => {
                debug!("📥 [Sequencing] Node {} processing SeqResponse from {} for vc={}, s={}",
                self.process_id, sender_id, hex::encode(&vc[..8]), sequence_number);
            
                let resp = SeqResponse {
                    vc,
                    s: sequence_number,
                    sigma: signature_share,
                };
                self.handle_seq_response(sender_id, resp).await?;
                Ok(None)
            }
            SmrolMessage::SeqOrder {
                vc,
                responses,
                sender_id,
            } => {
                debug!(
                    "📥 [Sequencing] Node {} processing SeqOrder from {} for vc={}, responses={}",
                    self.process_id,
                    sender_id,
                    hex::encode(&vc[..8]),
                    responses.len()
                );
                let records = responses
                    .into_iter()
                    .map(|(sender, sequence, signature)| SeqResponseRecord {
                        sender,
                        sequence,
                        signature,
                    })
                    .collect();
                let order = SeqOrder { vc, records };
                self.handle_seq_order(sender_id, order).await?;
                Ok(None)
            }
            SmrolMessage::SeqMedian {
                vc,
                median_sequence,
                proof,
                sender_id,
            } => {
                debug!(
                    "📥 [Sequencing] Node {} processing SeqMedian from {} for vc={}",
                    self.process_id,
                    sender_id,
                    hex::encode(&vc[..8])
                );
                let median_msg = SeqMedian {
                    vc,
                    s_tx: median_sequence,
                    sigma_seq: proof,
                };
                self.handle_seq_median(sender_id, median_msg).await?;
                Ok(None)
            }
            SmrolMessage::SeqFinal {
                vc,
                final_sequence,
                combined_signature,
                sender_id: _,
            } => {
                let final_msg = SeqFinal {
                    vc,
                    s_tx: final_sequence,
                    sigma: combined_signature,
                };
                self.handle_seq_final(final_msg).await
            }
            _ => {
                debug!(
                    "收到非Sequencing消息: {:?}",
                    std::mem::discriminant(&message)
                );
                Ok(None)
            }
        }
    }

    /// 并发处理网络层收到的消息：内部获取Mutex锁后复用原有逻辑
    pub async fn handle_smrol_message(
        sequencing_handle: Arc<Mutex<TransactionSequencing>>,
        sender_id: usize,
        message: SmrolMessage,
    ) -> Result<Option<TransactionEntry>, String> {
        let mut sequencing = sequencing_handle.lock().await;
        sequencing
            .handle_smrol_message_inner(sender_id, message)
            .await
    }

    fn store_pending_final(&mut self, final_msg: SeqFinal) {
        info!(
            "⏳ [Sequencing] node={} 缓存SEQ-FINAL等待载荷: vc={} s_tx={}",
            self.process_id,
            hex::encode(&final_msg.vc[..std::cmp::min(8, final_msg.vc.len())]),
            final_msg.s_tx
        );
        self.pending_seq_finals
            .entry(final_msg.vc.clone())
            .or_default()
            .push(final_msg);
    }

    fn finalize_ready_final(&mut self, final_msg: SeqFinal) -> Option<TransactionEntry> {
        if !self.pending_txs.contains_key(&final_msg.vc) {
            // 事务尚未就绪，重新缓存等待
            let msg = final_msg.clone();
            self.store_pending_final(final_msg);
            debug!(
                "⏳ [Sequencing] node={} 重新缓存SEQ-FINAL等待 finalize_ready_final 载荷: vc={} s_tx={}",
                self.process_id,
                hex::encode(&msg.vc[..std::cmp::min(8, msg.vc.len())]),
                msg.s_tx
            );
            return None;
        }

        // self.add_to_mi(&final_msg.vc, &final_msg.s_tx);

        let payload = if let Some(tx) = self.pending_txs.remove(&final_msg.vc) {
            tx.payload
        } else if let Some(bytes) = self
            .erasure_store
            .get(&final_msg.vc)
            .and_then(|pkg| pkg.reconstruct_full().ok())
        {
            bytes
        } else {
            self.store_pending_final(final_msg);
            return None;
        };

        self.add_to_mi(&final_msg.vc, &final_msg.s_tx);

        let entry = TransactionEntry {
            vc_tx: final_msg.vc.clone(),
            s_tx: final_msg.s_tx,
            sigma: final_msg.sigma.clone(),
            payload,
        };

        // *
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

    /// 启动消息处理循环 - 从网络层的sequencing_rx接收消息
    pub async fn start_message_loop(self, network: Arc<SmrolTcpNetwork>) -> Result<(), String> {
        let sequencing_handle = Arc::new(Mutex::new(self));
        let node_id = {
            let guard = sequencing_handle.lock().await;
            guard.process_id
        };
        let sequencing_rx = network.get_sequencing_receiver();

        info!("🔄 [Sequencing] Node {} 消息处理循环启动", node_id);

        let mut rx = sequencing_rx.lock().await;

        let queue_capacity = Self::inbound_queue_capacity();
        let (task_tx, mut task_rx) = mpsc::channel::<SequencingTask>(queue_capacity);
        let worker_handle = Arc::clone(&sequencing_handle);
        let worker_node = node_id;

        tokio::spawn(async move {
            info!(
                "[Sequencing] Node {} sequencing worker started (capacity: {})",
                worker_node,
                queue_capacity
            );

            while let Some(task) = task_rx.recv().await {
                match TransactionSequencing::handle_smrol_message(
                    Arc::clone(&worker_handle),
                    task.sender_id,
                    task.message,
                )
                .await
                {
                    Ok(Some(_entry)) => {
                        debug!(
                            "[Sequencing] Node {} processed message and produced entry",
                            worker_node
                        );
                    }
                    Ok(None) => {}
                    Err(e) => error!(
                        "❌ [Sequencing] Node {} worker failed to process message: {}",
                        worker_node,
                        e
                    ),
                }
            }

            warn!(
                "⚠️ [Sequencing] Node {} sequencing worker exited (channel closed)",
                worker_node
            );
        });

        while let Some((sender_id, message)) = rx.recv().await {
            if let Err(e) = task_tx
                .send(SequencingTask {
                    sender_id,
                    message,
                })
                .await
            {
                error!(
                    "❌ [Sequencing] Node {} failed to enqueue sequencing message from {}: {}",
                    node_id,
                    sender_id,
                    e
                );
            }
        }

        warn!("⚠️ [Sequencing] Node {} 消息处理循环退出", node_id);
        Ok(())
    }

    fn inbound_queue_capacity() -> usize {
        std::env::var("SEQUENCING_QUEUE_CAP")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(2048)
    }
}
