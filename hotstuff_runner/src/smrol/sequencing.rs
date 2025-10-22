use crate::smrol::{
    consensus::TransactionEntry,
    crypto::ErasurePackage,
    finalization::OutputFinalization,
    message::{SmrolMessage, SmrolTransaction},
    network::SmrolTcpNetwork,
    pnfifo::PnfifoBc,
    ModuleMessage,
};
use dashmap::{mapref::entry::Entry, DashMap, DashSet};
use ed25519_dalek::{Signature as Ed25519Signature, Signer, SigningKey, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::Instant;
use std::{collections::HashMap, result};
use threshold_crypto::{
    PublicKeySet, SecretKeyShare, Signature as ThresholdSignature, SignatureShare, SIG_SIZE,
};
use tokio::{
    sync::{mpsc as async_mpsc, Mutex as AsyncMutex, RwLock},
    task,
    time::{timeout, Duration},
};
use tracing::{debug, error, info, warn};
use tracing_subscriber::field::debug;

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

pub struct TransactionSequencing {
    pub f: usize,
    pub n: usize,
    pnfifo_threshold: usize,
    pub process_id: usize, // node_id
    pub network: Arc<SmrolTcpNetwork>,
    pub pnfifo: Arc<PnfifoBc>,
    pub threshold_share: SecretKeyShare,
    pub threshold_public: PublicKeySet,
    signing_key: SigningKey,
    verifying_keys: Arc<HashMap<usize, VerifyingKey>>,
    pub finalization: Arc<RwLock<OutputFinalization>>,
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
    seq_payloads: DashMap<u64, Vec<u8>>,
    broadcast_tx: async_mpsc::UnboundedSender<SmrolMessage>,
    inbound_tx: async_mpsc::UnboundedSender<ModuleMessage>,
    inbound_rx: Arc<AsyncMutex<async_mpsc::UnboundedReceiver<ModuleMessage>>>,
    request_tx: async_mpsc::UnboundedSender<ModuleMessage>,
    request_rx: Arc<AsyncMutex<async_mpsc::UnboundedReceiver<ModuleMessage>>>,
    response_tx: async_mpsc::UnboundedSender<ModuleMessage>,
    response_rx: Arc<AsyncMutex<async_mpsc::UnboundedReceiver<ModuleMessage>>>,
    order_tx: async_mpsc::UnboundedSender<ModuleMessage>,
    order_rx: Arc<AsyncMutex<async_mpsc::UnboundedReceiver<ModuleMessage>>>,
    median_tx: async_mpsc::UnboundedSender<ModuleMessage>,
    median_rx: Arc<AsyncMutex<async_mpsc::UnboundedReceiver<ModuleMessage>>>,
    final_tx: async_mpsc::UnboundedSender<ModuleMessage>,
    final_rx: Arc<AsyncMutex<async_mpsc::UnboundedReceiver<ModuleMessage>>>,
    sequenced_entry_tx: async_mpsc::UnboundedSender<TransactionEntry>,
}

impl TransactionSequencing {
    pub fn new(
        process_id: usize,
        n: usize,
        f: usize,
        pnfifo_threshold: usize,
        network: Arc<SmrolTcpNetwork>,
        pnfifo: Arc<PnfifoBc>,
        threshold_share: SecretKeyShare,
        threshold_public: PublicKeySet,
        signing_key: SigningKey,
        verifying_keys: HashMap<usize, VerifyingKey>,
        finalization: Arc<RwLock<OutputFinalization>>,
        sequenced_entry_tx: async_mpsc::UnboundedSender<TransactionEntry>,
    ) -> Self {
        let (broadcast_tx, mut broadcast_rx) = async_mpsc::unbounded_channel::<SmrolMessage>();
        let network_clone = Arc::clone(&network);
        let node_id = process_id;
        network.spawn(async move {
            info!("📡 [Sequencing] Node {} 启动广播处理器", node_id);
            while let Some(msg) = broadcast_rx.recv().await {
                if let Err(e) = network_clone.broadcast(msg).await {
                    error!("❌ [Sequencing] 广播处理器失败: {}", e);
                }
            }
            warn!(
                "[Sequencing] Node {} 广播处理器退出 (channel closed)",
                node_id
            );
        });

        let (inbound_tx, inbound_rx_raw) = async_mpsc::unbounded_channel::<ModuleMessage>();
        let inbound_rx = Arc::new(AsyncMutex::new(inbound_rx_raw));

        let (request_tx, request_rx_raw) = async_mpsc::unbounded_channel::<ModuleMessage>();
        let request_rx = Arc::new(AsyncMutex::new(request_rx_raw));
        let (response_tx, response_rx_raw) = async_mpsc::unbounded_channel::<ModuleMessage>();
        let response_rx = Arc::new(AsyncMutex::new(response_rx_raw));
        let (order_tx, order_rx_raw) = async_mpsc::unbounded_channel::<ModuleMessage>();
        let order_rx = Arc::new(AsyncMutex::new(order_rx_raw));
        let (median_tx, median_rx_raw) = async_mpsc::unbounded_channel::<ModuleMessage>();
        let median_rx = Arc::new(AsyncMutex::new(median_rx_raw));
        let (final_tx, final_rx_raw) = async_mpsc::unbounded_channel::<ModuleMessage>();
        let final_rx = Arc::new(AsyncMutex::new(final_rx_raw));

        Self {
            f,
            n,
            pnfifo_threshold,
            process_id,
            network,
            pnfifo,
            threshold_share,
            threshold_public,
            signing_key,
            verifying_keys: Arc::new(verifying_keys),
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
            seq_payloads: DashMap::new(),
            broadcast_tx,
            inbound_tx,
            inbound_rx,
            request_tx,
            request_rx,
            response_tx,
            response_rx,
            order_tx,
            order_rx,
            median_tx,
            median_rx,
            final_tx,
            final_rx,
            sequenced_entry_tx,
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

        let tx_hash =
            hex::encode(&seq_request.tx.payload[..std::cmp::min(8, seq_request.tx.payload.len())]);
        let message = SmrolMessage::SeqRequest {
            tx_hash,
            transaction: tx.clone(),
            sender_id: self.process_id,
            sequence_number: s,
        };
        if let Err(e) = self.broadcast_tx.send(message) {
            return Err(format!("广播SEQ-REQUEST失败: {}", e));
        }
        tokio::task::yield_now().await;

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

    pub fn inbound_sender(&self) -> async_mpsc::UnboundedSender<ModuleMessage> {
        self.inbound_tx.clone()
    }

    pub fn start_processing(self: &Arc<Self>) {
        self.spawn_inbound_processor();
        self.spawn_request_processor();
        self.spawn_response_processor();
        self.spawn_order_processor();
        self.spawn_median_processor();
        self.spawn_final_processor();
    }

    fn spawn_inbound_processor(self: &Arc<Self>) {
        let inbound_rx = Arc::clone(&self.inbound_rx);
        let sequencing = Arc::clone(self);
        tokio::spawn(async move {
            info!(
                "[Sequencing] Node {} inbound processor started",
                sequencing.process_id
            );

            let mut count = 0;
            let mut total_time = Duration::ZERO;
            let mut max_time = Duration::ZERO;
            let mut last_log = Instant::now();
            while let Some((sender_id, message)) = {
                let mut rx = inbound_rx.lock().await;
                rx.recv().await
            } {
                count += 1;
                let start = Instant::now();
                if let Err(e) = sequencing.route_inbound_message(sender_id, message).await {
                    warn!(
                        "⚠️ [Sequencing] Node {} inbound routing failed: {}",
                        sequencing.process_id, e
                    );
                }
                let elapsed = start.elapsed();
                if elapsed > Duration::from_millis(10) {
                    warn!(
                        "🐌 [Sequencing] Inbound handler slow for node {}: {:?}",
                        sequencing.process_id, elapsed
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
                        "📊 [Critical] Inbound stats node {}: {} msgs, avg={:?}, max={:?}, rate={}/s",
                        sequencing.process_id, count, avg, max_time, count
                    );
                    count = 0;
                    total_time = Duration::ZERO;
                    max_time = Duration::ZERO;
                    last_log = Instant::now();
                }
            }

            warn!(
                "⚠️ [Sequencing] Node {} inbound processor exiting (channel closed)",
                sequencing.process_id
            );
        });
    }

    async fn route_inbound_message(
        &self,
        sender_id: usize,
        message: SmrolMessage,
    ) -> Result<(), String> {
        match message {
            SmrolMessage::SeqRequest {
                tx_hash,
                transaction,
                sender_id: msg_sender,
                sequence_number,
            } => {
                if msg_sender != sender_id {
                    warn!(
                        "⚠️ [Sequencing] SeqRequest sender mismatch: msg={} net={}",
                        msg_sender, sender_id
                    );
                }
                self.request_tx
                    .send((
                        sender_id,
                        SmrolMessage::SeqRequest {
                            tx_hash,
                            transaction,
                            sender_id: msg_sender,
                            sequence_number,
                        },
                    ))
                    .map_err(|e| format!("seq-request queue send failed: {}", e))
            }
            SmrolMessage::SeqResponse {
                vc,
                signature_share,
                sender_id: msg_sender,
                sequence_number,
            } => {
                if msg_sender != sender_id {
                    warn!(
                        "⚠️ [Sequencing] SeqResponse sender mismatch: msg={} net={}",
                        msg_sender, sender_id
                    );
                }
                self.response_tx
                    .send((
                        sender_id,
                        SmrolMessage::SeqResponse {
                            vc,
                            signature_share,
                            sender_id: msg_sender,
                            sequence_number,
                        },
                    ))
                    .map_err(|e| format!("seq-response queue send failed: {}", e))
            }
            SmrolMessage::SeqOrder {
                vc,
                responses,
                sender_id: msg_sender,
            } => {
                if msg_sender != sender_id {
                    warn!(
                        "⚠️ [Sequencing] SeqOrder sender mismatch: msg={} net={}",
                        msg_sender, sender_id
                    );
                }
                self.order_tx
                    .send((
                        sender_id,
                        SmrolMessage::SeqOrder {
                            vc,
                            responses,
                            sender_id: msg_sender,
                        },
                    ))
                    .map_err(|e| format!("seq-order queue send failed: {}", e))
            }
            SmrolMessage::SeqMedian {
                vc,
                median_sequence,
                proof,
                sender_id: msg_sender,
            } => {
                if msg_sender != sender_id {
                    warn!(
                        "⚠️ [Sequencing] SeqMedian sender mismatch: msg={} net={}",
                        msg_sender, sender_id
                    );
                }
                self.median_tx
                    .send((
                        sender_id,
                        SmrolMessage::SeqMedian {
                            vc,
                            median_sequence,
                            proof,
                            sender_id: msg_sender,
                        },
                    ))
                    .map_err(|e| format!("seq-median queue send failed: {}", e))
            }
            SmrolMessage::SeqFinal {
                vc,
                final_sequence,
                combined_signature,
                sender_id: msg_sender,
            } => {
                if msg_sender != sender_id {
                    warn!(
                        "⚠️ [Sequencing] SeqFinal sender mismatch: msg={} net={}",
                        msg_sender, sender_id
                    );
                }
                self.final_tx
                    .send((
                        sender_id,
                        SmrolMessage::SeqFinal {
                            vc,
                            final_sequence,
                            combined_signature,
                            sender_id: msg_sender,
                        },
                    ))
                    .map_err(|e| format!("seq-final queue send failed: {}", e))
            }
            other => {
                debug!(
                    "[Sequencing] Node {} ignoring non-sequencing message {:?}",
                    self.process_id,
                    std::mem::discriminant(&other)
                );
                Ok(())
            }
        }
    }

    fn spawn_request_processor(self: &Arc<Self>) {
        let request_rx = Arc::clone(&self.request_rx);
        let sequencing = Arc::clone(self);
        tokio::spawn(async move {
            info!(
                "[Sequencing] Node {} request processor started",
                sequencing.process_id
            );

            let mut count = 0;
            let mut total_time = Duration::ZERO;
            let mut max_time = Duration::ZERO;
            let mut last_log = Instant::now();
            while let Some((sender_id, message)) = {
                let mut rx = request_rx.lock().await;
                rx.recv().await
            } {
                count += 1;
                let start = Instant::now();
                if let SmrolMessage::SeqRequest {
                    tx_hash,
                    transaction,
                    sender_id: _msg_sender,
                    sequence_number,
                } = message
                {
                    if let Err(e) = sequencing
                        .process_seq_request_message(
                            sender_id,
                            tx_hash,
                            transaction,
                            sequence_number,
                        )
                        .await
                    {
                        warn!(
                            "⚠️ [Sequencing] Node {} request handling failed: {}",
                            sequencing.process_id, e
                        );
                    }
                } else {
                    warn!(
                        "⚠️ [Sequencing] Node {} unexpected message in request queue",
                        sequencing.process_id
                    );
                }
                let elapsed = start.elapsed();
                if elapsed > Duration::from_millis(10) {
                    warn!(
                        "🐌 [Sequencing] Request handler slow for node {}: {:?}",
                        sequencing.process_id, elapsed
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
                        "📊 [Critical] Request stats node {}: {} msgs, avg={:?}, max={:?}, rate={}/s",
                        sequencing.process_id, count, avg, max_time, count
                    );
                    count = 0;
                    total_time = Duration::ZERO;
                    max_time = Duration::ZERO;
                    last_log = Instant::now();
                }
            }

            warn!(
                "⚠️ [Sequencing] Node {} request processor exiting (channel closed)",
                sequencing.process_id
            );
        });
    }

    fn spawn_response_processor(self: &Arc<Self>) {
        let response_rx = Arc::clone(&self.response_rx);
        let sequencing = Arc::clone(self);
        tokio::spawn(async move {
            info!(
                "[Sequencing] Node {} response processor started",
                sequencing.process_id
            );

            let mut count = 0;
            let mut total_time = Duration::ZERO;
            let mut max_time = Duration::ZERO;
            let mut last_log = Instant::now();
            while let Some((sender_id, message)) = {
                let mut rx = response_rx.lock().await;
                rx.recv().await
            } {
                count += 1;
                let start = Instant::now();
                if let SmrolMessage::SeqResponse {
                    vc,
                    signature_share,
                    sender_id: _msg_sender,
                    sequence_number,
                } = message
                {
                    if let Err(e) = sequencing
                        .process_seq_response_message(
                            sender_id,
                            vc,
                            signature_share,
                            sequence_number,
                        )
                        .await
                    {
                        warn!(
                            "⚠️ [Sequencing] Node {} response handling failed: {}",
                            sequencing.process_id, e
                        );
                    }
                } else {
                    warn!(
                        "⚠️ [Sequencing] Node {} unexpected message in response queue",
                        sequencing.process_id
                    );
                }
                let elapsed = start.elapsed();
                if elapsed > Duration::from_millis(10) {
                    warn!(
                        "🐌 [Sequencing] Response handler slow for node {}: {:?}",
                        sequencing.process_id, elapsed
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
                        "📊 [Critical] Response stats node {}: {} msgs, avg={:?}, max={:?}, rate={}/s",
                        sequencing.process_id, count, avg, max_time, count
                    );
                    count = 0;
                    total_time = Duration::ZERO;
                    max_time = Duration::ZERO;
                    last_log = Instant::now();
                }
            }

            warn!(
                "⚠️ [Sequencing] Node {} response processor exiting (channel closed)",
                sequencing.process_id
            );
        });
    }

    fn spawn_order_processor(self: &Arc<Self>) {
        let order_rx = Arc::clone(&self.order_rx);
        let sequencing = Arc::clone(self);
        tokio::spawn(async move {
            info!(
                "[Sequencing] Node {} order processor started",
                sequencing.process_id
            );

            let mut count = 0;
            let mut total_time = Duration::ZERO;
            let mut max_time = Duration::ZERO;
            let mut last_log = Instant::now();
            while let Some((sender_id, message)) = {
                let mut rx = order_rx.lock().await;
                rx.recv().await
            } {
                count += 1;
                let start = Instant::now();
                if let SmrolMessage::SeqOrder {
                    vc,
                    responses,
                    sender_id: _msg_sender,
                } = message
                {
                    if let Err(e) = sequencing
                        .process_seq_order_message(sender_id, vc, responses)
                        .await
                    {
                        warn!(
                            "⚠️ [Sequencing] Node {} order handling failed: {}",
                            sequencing.process_id, e
                        );
                    }
                } else {
                    warn!(
                        "⚠️ [Sequencing] Node {} unexpected message in order queue",
                        sequencing.process_id
                    );
                }
                let elapsed = start.elapsed();
                if elapsed > Duration::from_millis(10) {
                    warn!(
                        "🐌 [Sequencing] Order handler slow for node {}: {:?}",
                        sequencing.process_id, elapsed
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
                        "📊 [Critical] Order stats node {}: {} msgs, avg={:?}, max={:?}, rate={}/s",
                        sequencing.process_id, count, avg, max_time, count
                    );
                    count = 0;
                    total_time = Duration::ZERO;
                    max_time = Duration::ZERO;
                    last_log = Instant::now();
                }
            }

            warn!(
                "⚠️ [Sequencing] Node {} order processor exiting (channel closed)",
                sequencing.process_id
            );
        });
    }

    fn spawn_median_processor(self: &Arc<Self>) {
        let median_rx = Arc::clone(&self.median_rx);
        let sequencing = Arc::clone(self);
        tokio::spawn(async move {
            info!(
                "[Sequencing] Node {} median processor started",
                sequencing.process_id
            );

            let mut count = 0;
            let mut total_time = Duration::ZERO;
            let mut max_time = Duration::ZERO;
            let mut last_log = Instant::now();
            while let Some((sender_id, message)) = {
                let mut rx = median_rx.lock().await;
                rx.recv().await
            } {
                count += 1;
                let start = Instant::now();
                if let SmrolMessage::SeqMedian {
                    vc,
                    median_sequence,
                    proof,
                    sender_id: _msg_sender,
                } = message
                {
                    if let Err(e) = sequencing
                        .process_seq_median_message(sender_id, vc, median_sequence, proof)
                        .await
                    {
                        warn!(
                            "⚠️ [Sequencing] Node {} median handling failed: {}",
                            sequencing.process_id, e
                        );
                    }
                } else {
                    warn!(
                        "⚠️ [Sequencing] Node {} unexpected message in median queue",
                        sequencing.process_id
                    );
                }
                let elapsed = start.elapsed();
                if elapsed > Duration::from_millis(10) {
                    warn!(
                        "🐌 [Sequencing] Median handler slow for node {}: {:?}",
                        sequencing.process_id, elapsed
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
                        "📊 [Critical] Median stats node {}: {} msgs, avg={:?}, max={:?}, rate={}/s",
                        sequencing.process_id, count, avg, max_time, count
                    );
                    count = 0;
                    total_time = Duration::ZERO;
                    max_time = Duration::ZERO;
                    last_log = Instant::now();
                }
            }

            warn!(
                "⚠️ [Sequencing] Node {} median processor exiting (channel closed)",
                sequencing.process_id
            );
        });
    }

    fn spawn_final_processor(self: &Arc<Self>) {
        let final_rx = Arc::clone(&self.final_rx);
        let sequencing = Arc::clone(self);
        tokio::spawn(async move {
            info!(
                "[Sequencing] Node {} final processor started",
                sequencing.process_id
            );

            let mut count = 0;
            let mut total_time = Duration::ZERO;
            let mut max_time = Duration::ZERO;
            let mut last_log = Instant::now();
            while let Some((sender_id, message)) = {
                let mut rx = final_rx.lock().await;
                rx.recv().await
            } {
                count += 1;
                let start = Instant::now();
                if let SmrolMessage::SeqFinal {
                    vc,
                    final_sequence,
                    combined_signature,
                    sender_id: _msg_sender,
                } = message
                {
                    if let Err(e) = sequencing
                        .process_seq_final_message(vc, final_sequence, combined_signature)
                        .await
                    {
                        warn!(
                            "⚠️ [Sequencing] Node {} final handling failed: {}",
                            sequencing.process_id, e
                        );
                    }
                } else {
                    warn!(
                        "⚠️ [Sequencing] Node {} unexpected message in final queue",
                        sequencing.process_id
                    );
                }
                let elapsed = start.elapsed();
                if elapsed > Duration::from_millis(10) {
                    warn!(
                        "🐌 [Sequencing] Final handler slow for node {}: {:?}",
                        sequencing.process_id, elapsed
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
                        "📊 [Critical] Final stats node {}: {} msgs, avg={:?}, max={:?}, rate={}/s",
                        sequencing.process_id, count, avg, max_time, count
                    );
                    count = 0;
                    total_time = Duration::ZERO;
                    max_time = Duration::ZERO;
                    last_log = Instant::now();
                }
            }

            warn!(
                "⚠️ [Sequencing] Node {} final processor exiting (channel closed)",
                sequencing.process_id
            );
        });
    }

    #[tracing::instrument(skip(self))]
    async fn process_seq_request_message(
        &self,
        sender_id: usize,
        tx_hash: String,
        transaction: SmrolTransaction,
        sequence_number: u64,
    ) -> Result<(), String> {
        let serialized =
            bincode::serialize(&transaction).map_err(|e| format!("序列化失败: {}", e))?;
        let data_shards = std::cmp::max(1, self.pnfifo_threshold);
        let total_shards = std::cmp::max(data_shards, self.n);
        let serialized_for_encode = serialized.clone();
        let encode_start = Instant::now();
        let encoded_package = task::spawn_blocking(move || {
            ErasurePackage::encode(&serialized_for_encode, data_shards, total_shards)
        })
        .await
        .map_err(|e| format!("纠删码编码任务失败: {}", e))??;
        let encode_duration = encode_start.elapsed();

        let seq_request = SeqRequest {
            seq_num: sequence_number,
            tx: Transaction {
                payload: serialized,
            },
        };

        let maybe_entry = self
            .handle_seq_request(
                sender_id,
                seq_request,
                encoded_package,
                Duration::from_millis(0),
                encode_duration,
            )
            .await?;

        if let Some(entry) = maybe_entry {
            self.emit_sequenced_entry(entry).await?;
        }

        debug!(
            "[Sequencing] Node {} processed SeqRequest {}, sender={} seq={}",
            self.process_id, tx_hash, sender_id, sequence_number
        );

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn process_seq_response_message(
        &self,
        sender_id: usize,
        vc: Vec<u8>,
        signature_share: Vec<u8>,
        sequence_number: u64,
    ) -> Result<(), String> {
        let response = SeqResponse {
            vc,
            s: sequence_number,
            sigma: signature_share,
        };
        self.handle_seq_response(sender_id, response).await
    }

    #[tracing::instrument(skip(self))]
    async fn process_seq_order_message(
        &self,
        sender_id: usize,
        vc: Vec<u8>,
        responses: Vec<(usize, u64, Vec<u8>)>,
    ) -> Result<(), String> {
        let records = responses
            .into_iter()
            .map(|(sender, sequence, signature)| SeqResponseRecord {
                sender,
                sequence,
                signature,
            })
            .collect();
        let order = SeqOrder { vc, records };
        self.handle_seq_order(sender_id, order).await
    }

    #[tracing::instrument(skip(self))]
    async fn process_seq_median_message(
        &self,
        sender_id: usize,
        vc: Vec<u8>,
        median_sequence: u64,
        proof: Vec<u8>,
    ) -> Result<(), String> {
        let median = SeqMedian {
            vc,
            s_tx: median_sequence,
            sigma_seq: proof,
        };
        self.handle_seq_median(sender_id, median).await
    }

    #[tracing::instrument(skip(self))]
    async fn process_seq_final_message(
        &self,
        vc: Vec<u8>,
        final_sequence: u64,
        combined_signature: Vec<u8>,
    ) -> Result<(), String> {
        let final_msg = SeqFinal {
            vc,
            s_tx: final_sequence,
            sigma: combined_signature,
        };
        let maybe_entry = self.handle_seq_final(final_msg).await?;
        if let Some(entry) = maybe_entry {
            self.emit_sequenced_entry(entry).await?;
        }
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn emit_sequenced_entry(&self, entry: TransactionEntry) -> Result<(), String> {
        let emit_start = Instant::now();
        self.sequenced_entry_tx
            .send(entry)
            .map_err(|e| format!("sequenced entry send failed: {}", e))?;
        let yield_start = Instant::now();
        tokio::task::yield_now().await;
        let yield_time = yield_start.elapsed();
        if yield_time > Duration::from_millis(1) {
            warn!("🐌 [Emit slow] yield_now took: {:?}", yield_time);
        }
        let emit_time = emit_start.elapsed();
        if emit_time > Duration::from_millis(1) {
            warn!("🐌 [Emit slow] emit took: {:?}", emit_time);
        }
        Ok(())
    }

    // Handle SEQ-REQUEST message - Lines 4-17
    #[tracing::instrument(skip(self))]
    pub async fn handle_seq_request(
        &self,
        sender: usize,
        req: SeqRequest,
        encoded_package: ErasurePackage,
        wait_duration: Duration,
        encode_duration: Duration,
    ) -> Result<Option<TransactionEntry>, String> {
        let total_start = Instant::now();
        info!(
            "📥 [Sequencing] Line 2:4: received SEQ-REQUEST, node {} seq_num: {} tx={}",
            sender,
            req.seq_num,
            hex::encode(&req.tx.payload[..std::cmp::min(8, req.tx.payload.len())])
        );

        let vc_root = encoded_package.merkle_root();
        let vc_tx = vc_root.to_vec();
        let vc_key = VC::from_slice(&vc_tx);

        let state_update_start = Instant::now();
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
        // store payload
        self.seq_payloads.insert(s, req.tx.payload.clone());
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
        let state_update_time = state_update_start.elapsed();

        // Input to PNFIFO-BC (Line 15)
        let enqueue_start = Instant::now();
        let t0 = Instant::now();
        let queue_result = self.pnfifo.broadcast(s, vc_tx.clone()).await;
        let since_t0 = t0.elapsed();
        if since_t0 > Duration::from_millis(10) {
            debug!("[Check] PNFIFO broadcast enqueue took {:?}.", since_t0);
        }
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
        let sign_send_start = Instant::now();
        let message = format!("sequence:{}:{}", hex::encode(&vc_tx), s);

        // should be digital signature
        let t0= Instant::now();
        let sigma = self
            .signing_key
            .sign(message.as_bytes())
            .to_bytes()
            .to_vec();
        let since_t0 = t0.elapsed();
        debug!("[DS:Signing_key.sign] took {:?}.", since_t0);
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
        let sign_send_time = sign_send_start.elapsed();

        // Check if we have deferred FINAL messages waiting for this vc
        let finalize_pending_start = Instant::now();
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
        let finalize_pending_time = finalize_pending_start.elapsed();

        let total_time = total_start.elapsed();
        if total_time > Duration::from_millis(10) {
            warn!(
                "🐌 handle_seq_request SLOW: total={:?}, state_update={:?}, broadcast={:?}, sign_and_send={:?}, finalize_pending={:?}, encode={:?}",
                total_time,
                state_update_time,
                enqueue_delay,
                sign_send_time,
                finalize_pending_time,
                encode_duration
            );
        }

        Ok(finalized_entry)
    }

    // Handle SEQ-RESPONSE message - Lines 18-23
    #[tracing::instrument(skip(self))]
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
        let total_start = Instant::now();
        let verify_start = Instant::now();
        // slow
        let verified = self.verify_seq_response_share(&resp, sender).await?;
        let verify_time = verify_start.elapsed();
        if verified {
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

            let map_update_start = Instant::now();
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
            let map_update_time = map_update_start.elapsed();

            if collected > 2 * self.f + 1 {
                let total_time = total_start.elapsed();
                if total_time > Duration::from_millis(10) {
                    warn!(
                        "🐌 handle_seq_response SLOW: total={:?}, verify={:?}, map_update={:?}, broadcast={:?}",
                        total_time,
                        verify_time,
                        map_update_time,
                        Duration::ZERO
                    );
                }
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
                let broadcast_start = Instant::now();
                if let Err(e) = self.broadcast_tx.send(order_msg) {
                    return Err(format!("广播SEQ-ORDER失败: {}", e));
                }
                tokio::task::yield_now().await;
                let broadcast_time = broadcast_start.elapsed();
                info!(
                    "📤 [Sequencing] node={} broadcasting *SEQ-ORDER* for vc={}, s={}",
                    self.process_id,
                    hex::encode(&resp.vc[..std::cmp::min(8, resp.vc.len())]),
                    resp.s
                );

                let total_time = total_start.elapsed();
                if total_time > Duration::from_millis(10) {
                    warn!(
                        "🐌 handle_seq_response SLOW: total={:?}, verify={:?}, map_update={:?}, broadcast={:?}",
                        total_time,
                        verify_time,
                        map_update_time,
                        broadcast_time
                    );
                }
            } else {
                let total_time = total_start.elapsed();
                if total_time > Duration::from_millis(10) {
                    warn!(
                        "🐌 handle_seq_response SLOW: total={:?}, verify={:?}, map_update={:?}, broadcast={:?}",
                        total_time,
                        verify_time,
                        map_update_time,
                        Duration::ZERO
                    );
                }
            }
        } else {
            warn!(
                "❌ [Sequencing] Invalid signature in SEQ-RESPONSE from Node {}",
                sender
            );
            let total_time = total_start.elapsed();
            if total_time > Duration::from_millis(10) {
                warn!(
                    "🐌 handle_seq_response SLOW (invalid share): total={:?}, verify={:?}",
                    total_time, verify_time
                );
            }
        }
        // }
        Ok(())
    }

    // Handle SEQ-ORDER message - Lines 24-28
    #[tracing::instrument(skip(self))]
    pub async fn handle_seq_order(&self, sender: usize, order: SeqOrder) -> Result<(), String> {
        info!("📥 [Sequencing] Node {} received SEQ-ORDER", sender);

        let total_start = Instant::now();
        let verify_start = Instant::now();
        // slow
        let verified = self.verify_seq_order_async(&order).await?;
        let verify_time = verify_start.elapsed();

        if verified {
            let sequences: Vec<u64> = order.records.iter().map(|r| r.sequence).collect();
            let median_start = Instant::now();
            let median = self.calculate_median(&sequences); // line 26
            let median_time = median_start.elapsed();
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
            let sign_send_start = Instant::now();
            // slow: sign and send
            let t0  = Instant::now();
            let sigma_seq = self
                .threshold_share
                .sign(message.as_bytes())
                .to_bytes()
                .to_vec();
            let since_t0 = t0.elapsed();
            warn!("[threshold_share.sign] Threshold sign took {:?}.", since_t0);
            
            let sign_time = sign_send_start.elapsed();

            let median_msg = SmrolMessage::SeqMedian {
                vc: order.vc.clone(),
                median_sequence: median,
                proof: sigma_seq,
                sender_id: self.process_id,
            };
            let send_to_start = Instant::now();
            if let Err(e) = self.network.send_to_node(sender, median_msg).await {
                return Err(format!("发送SEQ-MEDIAN失败: {}", e));
            }
            info!(
                "[Sequencing] Node={} sent *SEQ-MEDIAN* vc={} median={}",
                self.process_id,
                hex::encode(&order.vc[..std::cmp::min(8, order.vc.len())]),
                median,
            );
            let total_time = total_start.elapsed();
            if total_time > Duration::from_millis(10) {
                warn!(
                    "🐌 handle_seq_order SLOW: total={:?}, verify={:?}, median_calc={:?}, threshold_share.sign={:?}, send_to_node={:?}",
                    total_time,
                    verify_time,
                    median_time,
                    sign_time,
                    send_to_start.elapsed()
                );
            }
        } else {
            let total_time = total_start.elapsed();
            if total_time > Duration::from_millis(10) {
                warn!(
                    "🐌 handle_seq_order SLOW (invalid order): total={:?}, verify={:?}",
                    total_time, verify_time
                );
            }
        }
        Ok(())
    }

    // Handle SEQ-MEDIAN message - Lines 29-35
    #[tracing::instrument(skip(self))]
    pub async fn handle_seq_median(&self, sender: usize, median: SeqMedian) -> Result<(), String> {
        info!("📥 [Sequencing] received SEQ-MEDIAN from {}", sender);
        // point to point so skip the check
        // if self.originated_vcs.contains(&median.vc) {
        // Original SEQ-REQUEST sender gathers median shares (Algorithm 2, line 30)
        let total_start = Instant::now();
        let verify_start = Instant::now();
        // slow
        let maybe_share = self.verify_median_share_async(&median, sender).await?;
        let verify_time = verify_start.elapsed();

        if let Some(valid_share) = maybe_share {
            let vc_key = VC::from_slice(&median.vc);
            let threshold = self.f + 1;

            let mut ready_to_broadcast: Option<Vec<(usize, SignatureShare)>> = None;
            let mut share_slot = Some(valid_share);
            let map_update_start = Instant::now();
            let entry_len = match self.median_shares.entry(vc_key) {
                Entry::Occupied(mut occ) => {
                    let map = occ.get_mut();
                    if map.contains_key(&sender) {
                        map.len()
                    } else if map.len() >= threshold {
                        map.len()
                    } else {
                        if let Some(share) = share_slot.take() {
                            map.insert(sender, share);
                        }
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
                    if let Some(share) = share_slot.take() {
                        map.insert(sender, share);
                    }
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
            let map_update_time = map_update_start.elapsed();

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
                let combine_start = Instant::now();
                let collected = shares.len();
                let combined_sig = self.combine_median_shares_async(shares).await?;
                let combine_time = combine_start.elapsed();

                debug!(
                    "🔐 [Sequencing] node={} collected {} median shares for vc={} (s_tx={})",
                    self.process_id,
                    collected,
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
                let broadcast_start = Instant::now();
                if let Err(e) = self.broadcast_tx.send(final_msg) {
                    return Err(format!("广播SEQ-FINAL失败: {}", e));
                }
                tokio::task::yield_now().await;
                let broadcast_time = broadcast_start.elapsed();
                info!(
                    "[Sequencing] Node {} broadcast *SEQ-FINAL* {} for vc = {:?}",
                    self.process_id,
                    sender,
                    hex::encode(&median.vc[..std::cmp::min(8, median.vc.len())])
                );

                let total_time = total_start.elapsed();
                if total_time > Duration::from_millis(10) {
                    warn!(
                        "🐌 handle_seq_median SLOW: total={:?}, verify={:?}, map_update={:?}, combine={:?}, broadcast={:?}",
                        total_time,
                        verify_time,
                        map_update_time,
                        combine_time,
                        broadcast_time
                    );
                }
            } else {
                let total_time = total_start.elapsed();
                if total_time > Duration::from_millis(10) {
                    warn!(
                        "🐌 handle_seq_median SLOW: total={:?}, verify={:?}, map_update={:?}, combine={:?}, broadcast={:?}",
                        total_time,
                        verify_time,
                        map_update_time,
                        Duration::ZERO,
                        Duration::ZERO
                    );
                }
            }
        } else {
            warn!(
                "❌ [Sequencing] Invalid threshold share in SEQ-MEDIAN from node {}",
                sender
            );
            let total_time = total_start.elapsed();
            if total_time > Duration::from_millis(10) {
                warn!(
                    "🐌 handle_seq_median SLOW (invalid share): total={:?}, verify={:?}",
                    total_time, verify_time
                );
            }
        }
        // }
        Ok(())
    }

    // Handle SEQ-FINAL message - Lines 36-38
    #[tracing::instrument(skip(self))]
    pub async fn handle_seq_final(
        &self,
        final_msg: SeqFinal,
    ) -> Result<Option<TransactionEntry>, String> {
        let total_start = Instant::now();
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
        let verify_start = Instant::now();
        // slow
        let result = self.verify_combined_signature_async(&final_msg).await?;
        let verify_duration = verify_start.elapsed();
        if result {
            let check_start = Instant::now();
            let (in_vc_ledger, in_mi) = {
                let finalization = self.finalization.read().await;
                (
                    finalization.is_in_vc_ledger(&final_msg.vc),
                    finalization.is_in_mi(&final_msg.vc),
                )
            };
            let check_time = check_start.elapsed();

            if in_vc_ledger || in_mi {
                debug!(
                    "ℹ️ [Sequencing] SEQ-FINAL vc={} already finalized, ignoring",
                    hex::encode(&final_msg.vc[..std::cmp::min(8, final_msg.vc.len())])
                );
                return Ok(None);
            }

            let finalize_start = Instant::now();
            let result = self.finalize_ready_final(final_msg).await;
            let finalize_time = finalize_start.elapsed();
            let total_time = total_start.elapsed();
            if total_time > Duration::from_millis(10) {
                warn!(
                    "🐌 handle_seq_final SLOW: total={:?}, finalization check={:?}, verify_combined_signature={:?}, finalize_ready_final={:?}",
                    total_time, check_time, verify_duration, finalize_time
                );
            }
            Ok(result)
        } else {
            Ok(None)
        }
    }

    async fn verify_seq_response_share(
        &self,
        resp: &SeqResponse,
        sender: usize,
    ) -> Result<bool, String> {
        let verifying_keys = Arc::clone(&self.verifying_keys);
        let resp_clone = resp.clone();
        // task::spawn_blocking(move || {
        let Some(verifying_key) = verifying_keys.get(&sender) else {
            return Err(format!("missing verifying key for node {}", sender));
        };

        if resp_clone.sigma.len() != 64 {
            return Err(format!(
                "signature length invalid: {}",
                resp_clone.sigma.len()
            ));
        }

        let mut sig_bytes = [0u8; 64];
        sig_bytes.copy_from_slice(&resp_clone.sigma);
        let signature = Ed25519Signature::from_bytes(&sig_bytes);

        let message = format!("sequence:{}:{}", hex::encode(&resp_clone.vc), resp_clone.s);
        let t0 = Instant::now();
        let r = verifying_key
            .verify_strict(message.as_bytes(), &signature)
            .is_ok();
        let since_t0 = t0.elapsed();
        debug!("[DS:response:verifying_key.verify_strict] verify_seq_response_share took {:?}.", since_t0);
        Ok(r)
        // })
        // .await
        // .map_err(|e| format!("verify seq-response signature task失败: {}", e))?
    }

    async fn verify_seq_order_async(&self, order: &SeqOrder) -> Result<bool, String> {
        let verifying_keys = Arc::clone(&self.verifying_keys);
        let order_clone = order.clone();
        let required = 2 * self.f + 1;
        // task::spawn_blocking(move || {
        if order_clone.records.len() != required {
            return Ok(false);
        }

        for record in &order_clone.records {
            let Some(verifying_key) = verifying_keys.get(&record.sender) else {
                warn!(
                    "❌ [Sequencing] Missing verifying key for node {} in SeqOrder",
                    record.sender
                );
                return Ok(false);
            };

            if record.signature.len() != 64 {
                warn!(
                    "❌ [Sequencing] Invalid signature length in SeqOrder from node {}",
                    record.sender
                );
                return Ok(false);
            }

            let mut sig_bytes = [0u8; 64];
            sig_bytes.copy_from_slice(&record.signature);
            let signature = Ed25519Signature::from_bytes(&sig_bytes);

            let message = format!(
                "sequence:{}:{}",
                hex::encode(&order_clone.vc),
                record.sequence
            );
            // let t0 = Instant::now();
            // let r = verifying_key
            //     .verify_strict(message.as_bytes(), &signature)
            //     .is_err();
            // let since_t0 = t0.elapsed();
            // debug!("[DS:order:verifying_key.verify_strict] verify_seq_order_async took {:?}.", since_t0);
            if verifying_key
                .verify_strict(message.as_bytes(), &signature)
                .is_err()
            {
                warn!(
                    "❌ [Sequencing] Invalid signature from node {} in SeqOrder",
                    record.sender
                );
                return Ok(false);
            }
        }

        Ok(true)
        // })
        // .await
        // .map_err(|e| format!("verify seq-order task失败: {}", e))?
    }

    async fn verify_median_share_async(
        &self,
        median: &SeqMedian,
        sender: usize,
    ) -> Result<Option<SignatureShare>, String> {
        let threshold_public = self.threshold_public.clone();
        let median_clone = median.clone();
        task::spawn_blocking(move || {
            if median_clone.sigma_seq.len() != SIG_SIZE {
                return Err(format!(
                    "threshold signature share length invalid: {}",
                    median_clone.sigma_seq.len()
                ));
            }

            let mut share_bytes = [0u8; SIG_SIZE];
            share_bytes.copy_from_slice(&median_clone.sigma_seq);
            let share = SignatureShare::from_bytes(&share_bytes)
                .map_err(|e| format!("无法解析threshold share: {}", e))?;

            let pk_share = threshold_public.public_key_share(sender);
            let message = format!(
                "median:{}:{}",
                median_clone.s_tx,
                hex::encode(&median_clone.vc)
            );
            let t0 = Instant::now();
            let r = pk_share.verify(&share, message.as_bytes());
            let since_t0 = t0.elapsed();
            warn!("[pk_share.verify] verify_median_share took {:?}.", since_t0);
            if r {
                Ok(Some(share))
            } else {
                Ok(None)
            }
        })
        .await
        .map_err(|e| format!("verify seq-median share task失败: {}", e))?
    }

    async fn combine_median_shares_async(
        &self,
        shares: Vec<(usize, SignatureShare)>,
    ) -> Result<ThresholdSignature, String> {
        let threshold_public = self.threshold_public.clone();
        task::spawn_blocking(move || {
            threshold_public
                .combine_signatures(shares.iter().map(|(id, share)| (*id, share)))
                .map_err(|e| format!("阈值签名组合失败: {}", e))
        })
        .await
        .map_err(|e| format!("combine signature task失败: {}", e))?
    }

    async fn verify_combined_signature_async(&self, final_msg: &SeqFinal) -> Result<bool, String> {
        let threshold_public = self.threshold_public.clone();
        let final_clone = final_msg.clone();
        task::spawn_blocking(move || {
            if final_clone.sigma.len() != SIG_SIZE {
                return Err(format!(
                    "combined signature length invalid: {}",
                    final_clone.sigma.len()
                ));
            }

            let mut sig_bytes = [0u8; SIG_SIZE];
            sig_bytes.copy_from_slice(&final_clone.sigma);
            let signature = ThresholdSignature::from_bytes(&sig_bytes)
                .map_err(|e| format!("无法解析组合签名: {}", e))?;

            let message = format!(
                "median:{}:{}",
                final_clone.s_tx,
                hex::encode(&final_clone.vc)
            );
            let t0 = Instant::now();
            let r = threshold_public
                .public_key()
                .verify(&signature, message.as_bytes());
            let since_t0 = t0.elapsed();
            warn!("[public_key.verify] verify_combined_signature took {:?}.", since_t0);
            Ok(r)
        })
        .await
        .map_err(|e| format!("verify combined signature task失败: {}", e))?
    }

    async fn reconstruct_full_async(package: ErasurePackage) -> Result<Vec<u8>, String> {
        task::spawn_blocking(move || package.reconstruct_full())
            .await
            .map_err(|e| format!("erasure reconstruction task失败: {}", e))?
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

        if self.seq_payloads.len() > limit {
            let removed = self.seq_payloads.len();
            self.seq_payloads.clear();
            debug!(
                "{} cleared {} sequence payload entries",
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

        let payload = if let Some((_, bytes)) = self.seq_payloads.remove(&final_msg.s_tx) {
            self.pending_txs.remove(&vc_key);
            bytes
        } else if let Some((_, tx)) = self.pending_txs.remove(&vc_key) {
            tx.payload
        } else {
            debug!("Try Reconstructing...");
            let reconstructed = if let Some(pkg_entry) = self.erasure_store.get(&vc_key) {
                let package_copy = pkg_entry.clone();
                drop(pkg_entry);
                match Self::reconstruct_full_async(package_copy).await {
                    Ok(bytes) => Some(bytes),
                    Err(e) => {
                        warn!(
                            "❌ [Sequencing] Erasure reconstruction failed for vc {:?}: {}",
                            vc_key, e
                        );
                        None
                    }
                }
            } else {
                None
            };

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
