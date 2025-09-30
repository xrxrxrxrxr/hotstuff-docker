use crate::smrol::{
    consensus::TransactionEntry,
    crypto::ErasurePackage,
    finalization::OutputFinalization,
    message::{SmrolMessage, SmrolTransaction},
    network::SmrolTcpNetwork,
    pnfifo::PnfifoBc,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use tokio::{
    sync::{Mutex, RwLock},
    time::{sleep, Duration},
};
use tracing::{debug, error, info, warn};
use threshold_crypto::{PublicKeySet, SecretKeyShare, Signature, SignatureShare, SIG_SIZE};

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
    pub s_tx: u64,
    pub sigma_seq: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeqFinal {
    pub vc: Vec<u8>,
    pub s_tx: u64,
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
        }
    }

    // Function SMROL-broadcast(k, tx) - Line 1-3
    pub async fn smrol_broadcast(&mut self, tx: SmrolTransaction) -> Result<(), String> {
        let s = self.seq_i; // Get current sequence number k
        self.seq_i += 1;

        let payload =
            bincode::serialize(&tx).map_err(|e| format!("序列化SmrolTransaction失败: {}", e))?;

        let seq_request = SeqRequest {
            seq_num: s,
            tx: Transaction { payload },
        };
        self.network.multicast_seq_request(seq_request).await // Line 3： seq_request(seq_i, tx_serialized)
    }

    // Handle SEQ-REQUEST message - Lines 4-17
    pub async fn handle_seq_request(
        &mut self,
        sender: usize,
        req: SeqRequest,
    ) -> Result<(), String> {
        debug!(
            "📥 [Sequencing] 收到来自 {} 的SEQ-REQUEST, seq_num: {}",
            sender, req.seq_num
        );

        // Avoid downgrade attack (Line 5)
        if !self.wait_for_log_condition(sender, req.seq_num).await {
            warn!("❌ [Sequencing] Log condition verification failed");
            return Ok(());
        }

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
            self.tx_sequence_map.insert(vc_tx.clone(), assigned_s);
            assigned_s
        };

        // Persist local mappings for later reconstruction and consensus input
        self.pending_txs
            .entry(vc_tx.clone())
            .or_insert_with(|| req.tx.clone());
        self.erasure_store
            .entry(vc_tx.clone())
            .or_insert(encoded.clone());

        // Input to PNFIFO-BC (Line 15)
        let pnfifo = Arc::clone(&self.pnfifo);
        let vc_for_pnfifo = vc_tx.clone();
        tokio::spawn(async move {
            if let Err(err) = pnfifo.broadcast(vc_for_pnfifo).await {
                warn!("❌ [Sequencing] PNFIFO broadcast failed: {}", err);
            }
        });

        // Sign and respond (Lines 16-17)
        let message = format!("sequence:{}:{}", hex::encode(&vc_tx), s);
        let sigma = self
            .threshold_share
            .sign(message.as_bytes())
            .to_bytes()
            .to_vec();

        let response = SeqResponse {
            vc: vc_tx,
            s,
            sigma,
        };
        self.network.multicast_seq_response(response).await?;

        Ok(())
    }

    // Handle SEQ-RESPONSE message - Lines 18-23
    pub async fn handle_seq_response(
        &mut self,
        sender: usize,
        resp: SeqResponse,
    ) -> Result<(), String> {
        debug!("📥 [Sequencing] 收到来自 Node {} 的SEQ-RESPONSE", sender);

        if sender == self.process_id {
            // Sender of original request (Line 19)
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

                // Check if collected 2f+1 sequences (Line 22)
                if self.s_vec_map[&resp.vc].len() == 2 * self.f + 1 {
                    let records = self.s_vec_map[&resp.vc].clone();
                    let seq_order = SeqOrder {
                        vc: resp.vc.clone(),
                        records,
                    };
                    self.network.multicast_seq_order(seq_order).await?;
                }
            }
        }
        Ok(())
    }

    // Handle SEQ-ORDER message - Lines 24-28
    pub async fn handle_seq_order(&mut self, sender: usize, order: SeqOrder) -> Result<(), String> {
        debug!("📥 [Sequencing] 收到来自 Node {} 的SEQ-ORDER", sender);

        if self.verify_seq_order(&order) {
            let sequences: Vec<u64> = order.records.iter().map(|r| r.sequence).collect();
            let median = self.calculate_median(&sequences); // line 26
            info!(
                "✅ [Sequencing] Verified SEQ-ORDER from Node {} with median sequence {}",
                sender, median
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
        }
        Ok(())
    }

    // Handle SEQ-MEDIAN message - Lines 29-35
    pub async fn handle_seq_median(
        &mut self,
        sender: usize,
        median: SeqMedian,
    ) -> Result<(), String> {
        debug!("📥 [Sequencing] 收到来自 {} 的SEQ-MEDIAN", sender);

        if sender == self.process_id {
            // Original sender (Line 30) is the request sender
            if self.verify_threshold_share(&median, sender)? { // line 31
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

                if entry.len() == self.f + 1 {
                    let combined_sig = self
                        .threshold_public
                        .combine_signatures(entry.iter().map(|(id, share)| (*id, share)))
                        .map_err(|e| format!("阈值签名组合失败: {}", e))?;

                    let seq_final = SeqFinal {
                        vc: median.vc.clone(),
                        s_tx: median.s_tx,
                        sigma: combined_sig.to_bytes().to_vec(),
                    };
                    self.network.multicast_seq_final(seq_final).await?;
                    // 清理已使用的shares，避免重复广播
                    self.threshold_sigs.remove(&median.s_tx);
                }
            }
        }
        Ok(())
    }

    // Handle SEQ-FINAL message - Lines 36-38
    pub async fn handle_seq_final(
        &mut self,
        final_msg: SeqFinal,
    ) -> Result<Option<TransactionEntry>, String> {
        debug!("📥 [Sequencing] 收到SEQ-FINAL消息");

        if self.verify_combined_signature(&final_msg)? { // line 36
            let (in_vc_ledger, in_mi) = {
                let finalization = self.finalization.lock().await;
                (
                    finalization.is_in_vc_ledger(&final_msg.vc),
                    finalization.is_in_mi(&final_msg.vc),
                )
            };

            if !in_vc_ledger && !in_mi {
                // Retain local bookkeeping for Algorithm 3's Mi set.
                self.add_to_mi(&final_msg.vc, &final_msg.s_tx);

                let entry = TransactionEntry {
                    vc_tx: final_msg.vc.clone(),
                    s_tx: final_msg.s_tx,
                    sigma: final_msg.sigma.clone(),
                };

                info!(
                    "✅ [Sequencing] Finalized VC forwarded to consensus: vc_len={}, s_tx={}",
                    entry.vc_tx.len(),
                    entry.s_tx
                );
                return Ok(Some(entry));
            }
        }
        Ok(None)
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
        let mut attempts: u64 = 0;

        loop {
            if self
                .pnfifo
                .get_output(leader_id, target_slot)
                .await
                .is_some()
            {
                if attempts > 0 {
                    debug!(
                        "⏱️ [Sequencing] wait_for_log_condition satisfied after {} checks for leader {} slot {}",
                        attempts,
                        leader_id,
                        target_slot
                    );
                }
                return true;
            }

            attempts = attempts.saturating_add(1);
            sleep(Duration::from_millis(10)).await;
        }
    }

    fn verify_signature(&self, resp: &SeqResponse, sender: usize) -> Result<bool, String> {
        if resp.sigma.len() != SIG_SIZE {
            return Err(format!("signature share length invalid: {}", resp.sigma.len()));
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
        let signature = Signature::from_bytes(&sig_bytes)
            .map_err(|e| format!("无法解析组合签名: {}", e))?;

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

    /// 处理从网络层接收到的SmrolMessage
    pub async fn handle_smrol_message(
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
                self.handle_seq_request(sender_id, req).await?;
                Ok(None)
            }
            SmrolMessage::SeqResponse {
                vc,
                signature_share,
                sender_id: _,
                sequence_number,
            } => {
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
                sender_id: _,
            } => {
                if responses.len() == 1 {
                    let (origin, s_tx, sigma_seq) = responses.into_iter().next().unwrap();
                    let median_msg = SeqMedian {
                        vc,
                        s_tx,
                        sigma_seq,
                    };
                    self.handle_seq_median(origin, median_msg).await?;
                } else {
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
                }
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

    /// 启动消息处理循环 - 从网络层的sequencing_rx接收消息
    pub async fn start_message_loop(mut self, network: Arc<SmrolTcpNetwork>) -> Result<(), String> {
        let sequencing_rx = network.get_sequencing_receiver();

        info!("🔄 [Sequencing] Node {} 启动消息处理循环", self.process_id);

        let mut rx = sequencing_rx.lock().await;

        while let Some((sender_id, message)) = rx.recv().await {
            debug!(
                "📨 [Sequencing] Node {} 处理来自节点{}的消息",
                self.process_id, sender_id
            );

            match self.handle_smrol_message(sender_id, message).await {
                Ok(Some(entry)) => {
                    debug!(
                        "[Sequencing] 自主消息循环产生共识输入 vc_len={} s_tx={}",
                        entry.vc_tx.len(),
                        entry.s_tx
                    );
                    // Manager-driven pipeline consumes these entries; here we just drop them.
                }
                Ok(None) => {}
                Err(e) => error!("❌ [Sequencing] 处理消息失败: {}", e),
            }
        }

        warn!("⚠️ [Sequencing] Node {} 消息处理循环退出", self.process_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::smrol::crypto::derive_threshold_keys;
    use ed25519_dalek::{SigningKey, VerifyingKey};
    use std::net::SocketAddr;
    use tokio::time::{sleep, Duration};

    async fn create_test_sequencer(
        process_id: usize,
    ) -> (TransactionSequencing, HashMap<usize, VerifyingKey>) {
        let signing_key = SigningKey::from_bytes(&[process_id as u8 + 1; 32]);

        let mut verifying_keys = HashMap::new();
        let mut peer_addrs = HashMap::new();

        // Create keys for 4 nodes (f=1, so n=4)
        for i in 0..4 {
            let key = SigningKey::from_bytes(&[i as u8 + 1; 32]);
            verifying_keys.insert(i, key.verifying_key());
            peer_addrs.insert(
                i,
                format!("127.0.0.1:{}", 21000 + i)
                    .parse::<SocketAddr>()
                    .unwrap(),
            );
        }

        let (threshold_share, threshold_public) =
            derive_threshold_keys(process_id, 1, &verifying_keys)
                .expect("derive threshold keys");

        let network = Arc::new(SmrolTcpNetwork::new(process_id, peer_addrs.clone()));
        let pnfifo = Arc::new(
            PnfifoBc::new(
                process_id,
                4,
                signing_key.clone(),
                verifying_keys.clone(),
                peer_addrs,
            )
            .await
            .expect("Failed to create PNFIFO-BC"),
        );

        let finalization = Arc::new(Mutex::new(OutputFinalization::new(
            process_id,
            1,
            Arc::clone(&network),
            signing_key.clone(),
        )));

        let sequencing = TransactionSequencing::new(
            process_id,
            4,
            1,
            network,
            pnfifo,
            threshold_share,
            threshold_public,
            finalization,
        );

        (sequencing, verifying_keys)
    }

    #[tokio::test]
    async fn test_smrol_broadcast() {
        let (mut sequencer, _) = create_test_sequencer(0).await;
        let tx = SmrolTransaction {
            id: 1,
            from: "alice".into(),
            to: "bob".into(),
            amount: 42,
            client_id: "client".into(),
            timestamp: 0,
            nonce: 0,
        };

        let initial_seq = sequencer.seq_i;
        let result = sequencer.smrol_broadcast(tx).await;

        assert!(result.is_ok());
        assert_eq!(sequencer.seq_i, initial_seq + 1);
        println!("✓ SMROL broadcast increments sequence number");
    }

    #[tokio::test]
    async fn test_seq_request_handling() {
        let (mut sequencer, _) = create_test_sequencer(0).await;
        let smrol_tx = SmrolTransaction {
            id: 2,
            from: "alice".into(),
            to: "carol".into(),
            amount: 11,
            client_id: "client".into(),
            timestamp: 0,
            nonce: 1,
        };
        let payload = bincode::serialize(&smrol_tx).unwrap();
        let tx = Transaction { payload };
        let req = SeqRequest { seq_num: 1, tx };

        let initial_seq = sequencer.seq_i;
        let result = sequencer.handle_seq_request(1, req).await;

        assert!(result.is_ok());
        assert_eq!(sequencer.seq_i, initial_seq + 1);
        assert_eq!(sequencer.buf.len(), 1);
        assert_eq!(sequencer.pending_txs.len(), 1);
        assert_eq!(sequencer.erasure_store.len(), 1);
        println!("✓ SEQ-REQUEST handling works correctly");
    }

    #[tokio::test]
    async fn test_seq_response_collection() {
        let (mut sequencer, verifying_keys) = create_test_sequencer(0).await;
        let vc = b"test_vc".to_vec();

        // Simulate collecting 2f+1 responses
        for i in 0..3 {
            // 2*1+1 = 3 responses for f=1
            let (share, _) =
                derive_threshold_keys(i, 1, &verifying_keys).expect("derive threshold share");
            let sequence = 10 + i as u64;
            let message = format!("sequence:{}:{}", hex::encode(&vc), sequence);
            let sigma = share.sign(message.as_bytes()).to_bytes().to_vec();

            let resp = SeqResponse {
                vc: vc.clone(),
                s: sequence,
                sigma,
            };
            let result = sequencer.handle_seq_response(0, resp).await;
            assert!(result.is_ok());
        }

        assert_eq!(sequencer.s_vec_map.get(&vc).unwrap().len(), 3);
        println!("✓ SEQ-RESPONSE collection reaches 2f+1 threshold");
    }

    #[tokio::test]
    async fn test_median_calculation() {
        let (sequencer, _) = create_test_sequencer(0).await;

        // Test odd number of elements
        let s_vec = vec![5, 2, 8, 1, 9];
        let median = sequencer.calculate_median(&s_vec);
        assert_eq!(median, 5);

        // Test with 2f+1 elements (f=1, so 3 elements)
        let s_vec = vec![10, 15, 12];
        let median = sequencer.calculate_median(&s_vec);
        assert_eq!(median, 12);

        println!("✓ Median calculation works correctly");
    }

    #[tokio::test]
    async fn test_complete_sequencing_flow() {
        let (mut sequencer, _) = create_test_sequencer(0).await;
        let smrol_tx = SmrolTransaction {
            id: 3,
            from: "alice".into(),
            to: "dave".into(),
            amount: 9,
            client_id: "client".into(),
            timestamp: 0,
            nonce: 2,
        };
        let payload = bincode::serialize(&smrol_tx).unwrap();
        let tx = Transaction {
            payload: payload.clone(),
        };

        println!("Starting complete sequencing flow test...");

        // Step 1: SMROL broadcast
        let result = sequencer.smrol_broadcast(smrol_tx.clone()).await;
        assert!(result.is_ok());
        assert_eq!(sequencer.seq_i, 2);

        // Step 2: Handle SEQ-REQUEST
        let req = SeqRequest {
            seq_num: 1,
            tx: Transaction { payload },
        };
        let result = sequencer.handle_seq_request(1, req).await;
        assert!(result.is_ok());
        assert_eq!(sequencer.buf.len(), 1);

        println!("✓ Complete sequencing flow executed successfully");
    }

    #[tokio::test]
    async fn test_vector_commitment() {
        let (sequencer, _) = create_test_sequencer(0).await;
        let smrol_tx = SmrolTransaction {
            id: 4,
            from: "alice".into(),
            to: "erin".into(),
            amount: 99,
            client_id: "client".into(),
            timestamp: 0,
            nonce: 3,
        };
        let tx = Transaction {
            payload: bincode::serialize(&smrol_tx).unwrap(),
        };

        let encoded = sequencer
            .encode_transaction(&tx, 3)
            .expect("erasure coding must succeed");
        assert_eq!(encoded.shards.len(), std::cmp::max(3, sequencer.n));

        let vc = sequencer.create_vector_commitment(&encoded);
        assert_eq!(vc.len(), 32);

        println!("✓ Vector commitment creation works");
    }

    #[tokio::test]
    async fn test_signature_verification() {
        let (sequencer, verifying_keys) = create_test_sequencer(0).await;
        let vc = b"test_vc".to_vec();

        // Create valid signature
        let message = format!("sequence:{}:10", hex::encode(&vc));
        let (share, _) = derive_threshold_keys(0, 1, &verifying_keys).expect("derive threshold");
        let sigma = share.sign(message.as_bytes()).to_bytes().to_vec();

        let resp = SeqResponse { vc, s: 10, sigma };
        let result = sequencer.verify_signature(&resp, 0);

        assert!(result.is_ok());
        assert!(result.unwrap());

        println!("✓ Signature verification works");
    }
}
