use crate::smrol::{
    consensus::TransactionEntry,
    crypto::{
        verify_combined_signature_bytes,
        verify_signature_share,
        SmrolThresholdSig,
    },
    message::SmrolMessage,
    network::SmrolTcpNetwork,
    pnfifo::PnfifoBc,
};
use ed25519_dalek::{Signer, SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    pub data: Vec<u8>,
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
    pub sigma: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeqOrder {
    pub vc: Vec<u8>,
    pub s_vec: Vec<u64>,
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

#[derive(Debug)]
pub struct TransactionSequencing {
    pub seq_i: u64,
    pub buf: HashSet<Vec<u8>>,
    pub k: u64,
    pub f: usize,
    pub n: usize,
    pub process_id: usize,
    pub network: Arc<SmrolTcpNetwork>,
    pub pnfifo: PnfifoBc,
    pub signing_key: SigningKey,
    pub verifying_keys: HashMap<usize, VerifyingKey>,
    pub pending_txs: HashMap<Vec<u8>, Transaction>,
    pub s_vec_map: HashMap<Vec<u8>, Vec<u64>>,
    pub threshold_sigs: HashMap<Vec<u8>, SmrolThresholdSig>,
}

impl TransactionSequencing {
    pub fn new(
        process_id: usize,
        n: usize,
        f: usize,
        network: Arc<SmrolTcpNetwork>,
        pnfifo: PnfifoBc,
        signing_key: SigningKey,
        verifying_keys: HashMap<usize, VerifyingKey>,
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
            signing_key,
            verifying_keys,
            pending_txs: HashMap::new(),
            s_vec_map: HashMap::new(),
            threshold_sigs: HashMap::new(),
        }
    }

    // Function SMROL-broadcast(k, tx) - Line 1-3
    pub async fn smrol_broadcast(&mut self, tx: Transaction) -> Result<(), String> {
        let s = self.seq_i; // Get current sequence number k
        self.seq_i += 1;

        let seq_request = SeqRequest { seq_num: s, tx };
        self.network.multicast_seq_request(seq_request).await
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
        if !self.verify_log_condition(&req) {
            warn!("❌ [Sequencing] Log condition verification failed");
            return Ok(());
        }

        // Assign sequence number (Lines 7-11)
        let s = if !self.pending_txs.contains_key(&req.tx.data) {
            let assigned_s = self.seq_i;
            self.seq_i += 1;
            assigned_s
        } else {
            self.get_assigned_seq(&req.tx.data).unwrap_or(self.seq_i)
        };

        // Encode and create vector commitment (Lines 12-14)
        let encoded = self.encode_transaction(&req.tx, self.f + 1);
        let vc_tx = self.create_vector_commitment(&encoded);
        self.buf.insert(vc_tx.clone());

        // Input to PNFIFO-BC (Line 15)
        self.pnfifo.broadcast(vc_tx.clone()).await?;

        // Sign and respond (Lines 16-17)
        let message = format!("sequence:{:?}:{}:{}", req.seq_num, hex::encode(&vc_tx), s);
        let sigma = self
            .signing_key
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
        debug!("📥 [Sequencing] 收到来自 {} 的SEQ-RESPONSE", sender);

        if sender == self.process_id {
            // Sender of original request (Line 19)
            if self.verify_signature(&resp, sender)? {
                // Collect sequence numbers for tx (Line 21)
                self.s_vec_map
                    .entry(resp.vc.clone())
                    .or_insert_with(Vec::new)
                    .push(resp.s);

                // Check if collected 2f+1 sequences (Line 22)
                if self.s_vec_map[&resp.vc].len() == 2 * self.f + 1 {
                    let s_vec = &self.s_vec_map[&resp.vc];
                    let seq_order = SeqOrder {
                        vc: resp.vc,
                        s_vec: s_vec.clone(),
                    };
                    self.network.multicast_seq_order(seq_order).await?;
                }
            }
        }
        Ok(())
    }

    // Handle SEQ-ORDER message - Lines 24-28
    pub async fn handle_seq_order(&mut self, sender: usize, order: SeqOrder) -> Result<(), String> {
        debug!("📥 [Sequencing] 收到来自 {} 的SEQ-ORDER", sender);

        if self.verify_seq_order(&order) {
            let median = self.calculate_median(&order.s_vec);

            // Create threshold signature share
            let message = format!("median:{}:{}", median, hex::encode(&order.vc));
            let sigma_seq = self
                .signing_key
                .sign(message.as_bytes())
                .to_bytes()
                .to_vec();

            let seq_median = SeqMedian {
                vc: order.vc,
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
            // Original sender (Line 30)
            if self.verify_threshold_share(&median, sender)? {
                // Collect threshold signature shares
                let threshold_sig = self
                    .threshold_sigs
                    .entry(median.vc.clone())
                    .or_insert_with(|| SmrolThresholdSig::new(self.f + 1));

                let has_threshold = threshold_sig.add_share(sender, median.sigma_seq.clone());
                if has_threshold {
                    let combined_sig = threshold_sig
                        .combine()
                        .map_err(|e| format!("阈值签名组合失败: {}", e))?;

                    let seq_final = SeqFinal {
                        vc: median.vc,
                        s_tx: median.s_tx,
                        sigma: combined_sig,
                    };
                    self.network.multicast_seq_final(seq_final).await?;
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

        if self.verify_combined_signature(&final_msg)? {
            if !self.is_in_vc_ledger(&final_msg.vc) && !self.is_in_mi(&final_msg.vc) {
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
    fn encode_transaction(&self, tx: &Transaction, k: usize) -> Vec<Vec<u8>> {
        // Simple encoding - split transaction data into k parts
        let data = &tx.data;
        let chunk_size = (data.len() + k - 1) / k;

        (0..k)
            .map(|i| {
                let start = i * chunk_size;
                let end = std::cmp::min(start + chunk_size, data.len());
                if start < data.len() {
                    data[start..end].to_vec()
                } else {
                    vec![]
                }
            })
            .collect()
    }

    fn create_vector_commitment(&self, encoded: &[Vec<u8>]) -> Vec<u8> {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        for chunk in encoded {
            hasher.update(chunk);
        }
        hasher.finalize().to_vec()
    }

    fn verify_log_condition(&self, _req: &SeqRequest) -> bool {
        // Implement log verification to avoid downgrade attacks
        true // Simplified for now
    }

    fn get_assigned_seq(&self, tx_data: &[u8]) -> Option<u64> {
        // Return existing sequence number if already assigned
        self.pending_txs.get(tx_data).map(|_| self.seq_i - 1)
    }

    fn verify_signature(&self, resp: &SeqResponse, sender: usize) -> Result<bool, String> {
        if let Some(verifying_key) = self.verifying_keys.get(&sender) {
            let message = format!("sequence:{}:{}", hex::encode(&resp.vc), resp.s);

            Ok(verify_signature_share(
                &resp.sigma,
                message.as_bytes(),
                verifying_key,
            ))
        } else {
            Err(format!("未找到节点 {} 的验证密钥", sender))
        }
    }

    fn verify_seq_order(&self, order: &SeqOrder) -> bool {
        order.s_vec.len() == 2 * self.f + 1
    }

    fn verify_threshold_share(&self, median: &SeqMedian, sender: usize) -> Result<bool, String> {
        if let Some(verifying_key) = self.verifying_keys.get(&sender) {
            let message = format!("median:{}:{}", median.s_tx, hex::encode(&median.vc));

            Ok(verify_signature_share(
                &median.sigma_seq,
                message.as_bytes(),
                verifying_key,
            ))
        } else {
            Err(format!("未找到节点 {} 的验证密钥", sender))
        }
    }

    fn verify_combined_signature(&self, final_msg: &SeqFinal) -> Result<bool, String> {
        let message = format!("median:{}:{}", final_msg.s_tx, hex::encode(&final_msg.vc));
        verify_combined_signature_bytes(
            &final_msg.sigma,
            message.as_bytes(),
            &self.verifying_keys,
            self.f + 1,
        )
    }

    fn calculate_median(&self, s_vec: &[u64]) -> u64 {
        let mut sorted = s_vec.to_vec();
        sorted.sort();
        sorted[sorted.len() / 2]
    }

    fn is_in_vc_ledger(&self, _vc: &[u8]) -> bool {
        false // Simplified
    }

    fn is_in_mi(&self, _vc: &[u8]) -> bool {
        false // Simplified
    }

    fn add_to_mi(&mut self, vc: &[u8], s_tx: &u64) {
        // Add (vc_tx, s_tx, Σ) to Mi for Consensus input
        self.pending_txs
            .insert(vc.to_vec(), Transaction { data: vc.to_vec() });
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
                tx_hash,
                transaction,
                sender_id: _,
                sequence_number,
            } => {
                let tx = Transaction {
                    data: tx_hash.into_bytes(),
                };
                let req = SeqRequest {
                    seq_num: sequence_number,
                    tx,
                };
                self.handle_seq_request(sender_id, req).await?;
                Ok(None)
            }
            SmrolMessage::SeqResponse {
                tx_hash,
                vector_commitment,
                signature,
                sender_id: _,
                sequence_number,
            } => {
                let resp = SeqResponse {
                    vc: vector_commitment,
                    s: sequence_number,
                    sigma: signature,
                };
                self.handle_seq_response(sender_id, resp).await?;
                Ok(None)
            }
            SmrolMessage::SeqOrder {
                tx_hash,
                median_sequence,
                proof,
                sender_id: _,
            } => {
                let order = SeqOrder {
                    vc: proof,
                    s_vec: vec![median_sequence], // 简化处理，实际应该从消息中获取完整s_vec
                };
                self.handle_seq_order(sender_id, order).await?;
                Ok(None)
            }
            SmrolMessage::SeqFinal {
                tx_hash,
                final_sequence,
                combined_proof,
                sender_id: _,
            } => {
                let final_msg = SeqFinal {
                    vc: tx_hash.into_bytes(),
                    s_tx: final_sequence,
                    sigma: combined_proof,
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
    use std::net::SocketAddr;
    use tokio::time::{sleep, Duration};

    async fn create_test_sequencer(process_id: usize) -> TransactionSequencing {
        let signing_key = SigningKey::from_bytes(&[process_id as u8 + 1; 32]);
        let verifying_key = signing_key.verifying_key();

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

        let network = Arc::new(SmrolTcpNetwork::new(process_id, peer_addrs.clone()));
        let pnfifo = PnfifoBc::new(
            process_id,
            4,
            signing_key.clone(),
            verifying_keys.clone(),
            peer_addrs,
        )
        .await
        .expect("Failed to create PNFIFO-BC");

        TransactionSequencing::new(
            process_id,
            4,
            1,
            network,
            pnfifo,
            signing_key,
            verifying_keys,
        )
    }

    #[tokio::test]
    async fn test_smrol_broadcast() {
        let mut sequencer = create_test_sequencer(0).await;
        let tx = Transaction {
            data: b"test_transaction".to_vec(),
        };

        let initial_seq = sequencer.seq_i;
        let result = sequencer.smrol_broadcast(tx).await;

        assert!(result.is_ok());
        assert_eq!(sequencer.seq_i, initial_seq + 1);
        println!("✓ SMROL broadcast increments sequence number");
    }

    #[tokio::test]
    async fn test_seq_request_handling() {
        let mut sequencer = create_test_sequencer(0).await;
        let tx = Transaction {
            data: b"test_tx".to_vec(),
        };
        let req = SeqRequest { seq_num: 1, tx };

        let initial_seq = sequencer.seq_i;
        let result = sequencer.handle_seq_request(1, req).await;

        assert!(result.is_ok());
        assert_eq!(sequencer.seq_i, initial_seq + 1);
        assert_eq!(sequencer.buf.len(), 1);
        println!("✓ SEQ-REQUEST handling works correctly");
    }

    #[tokio::test]
    async fn test_seq_response_collection() {
        let mut sequencer = create_test_sequencer(0).await;
        let vc = b"test_vc".to_vec();

        // Create valid signature for each response
        let message = format!("sequence:{}:10", hex::encode(&vc));
        let sigma = sequencer
            .signing_key
            .sign(message.as_bytes())
            .to_bytes()
            .to_vec();

        // Simulate collecting 2f+1 responses
        for i in 0..3 {
            // 2*1+1 = 3 responses for f=1
            let resp = SeqResponse {
                vc: vc.clone(),
                s: 10 + i as u64,
                sigma: sigma.clone(),
            };
            let result = sequencer.handle_seq_response(0, resp).await;
            assert!(result.is_ok());
        }

        assert_eq!(sequencer.s_vec_map.get(&vc).unwrap().len(), 3);
        println!("✓ SEQ-RESPONSE collection reaches 2f+1 threshold");
    }

    #[tokio::test]
    async fn test_median_calculation() {
        let sequencer = create_test_sequencer(0).await;

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
        let mut sequencer = create_test_sequencer(0).await;
        let tx = Transaction {
            data: b"complete_test".to_vec(),
        };

        println!("Starting complete sequencing flow test...");

        // Step 1: SMROL broadcast
        let result = sequencer.smrol_broadcast(tx.clone()).await;
        assert!(result.is_ok());
        assert_eq!(sequencer.seq_i, 2);

        // Step 2: Handle SEQ-REQUEST
        let req = SeqRequest { seq_num: 1, tx };
        let result = sequencer.handle_seq_request(1, req).await;
        assert!(result.is_ok());
        assert_eq!(sequencer.buf.len(), 1);

        println!("✓ Complete sequencing flow executed successfully");
    }

    #[tokio::test]
    async fn test_vector_commitment() {
        let sequencer = create_test_sequencer(0).await;
        let tx = Transaction {
            data: b"test_data".to_vec(),
        };

        let encoded = sequencer.encode_transaction(&tx, 3);
        assert_eq!(encoded.len(), 3);

        let vc = sequencer.create_vector_commitment(&encoded);
        assert!(!vc.is_empty());

        println!("✓ Vector commitment creation works");
    }

    #[tokio::test]
    async fn test_signature_verification() {
        let sequencer = create_test_sequencer(0).await;
        let vc = b"test_vc".to_vec();

        // Create valid signature
        let message = format!("sequence:{}:10", hex::encode(&vc));
        let sigma = sequencer
            .signing_key
            .sign(message.as_bytes())
            .to_bytes()
            .to_vec();

        let resp = SeqResponse { vc, s: 10, sigma };
        let result = sequencer.verify_signature(&resp, 0);

        assert!(result.is_ok());
        assert!(result.unwrap());

        println!("✓ Signature verification works");
    }
}
