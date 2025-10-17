use crate::event::SystemEvent;
use crate::smrol::{
    adapter::SmrolHotStuffAdapter,
    consensus::{Consensus, TransactionEntry},
    crypto::derive_threshold_keys,
    finalization::OutputFinalization,
    message::{SmrolMessage, SmrolTransaction},
    network::SmrolTcpNetwork,
    pnfifo::PnfifoBc,
    sequencing::{
        SeqFinal, SeqMedian, SeqOrder, SeqRequest, SeqResponse, SeqResponseRecord, Transaction,
        TransactionSequencing,
    },
};
use crate::smrol::crypto::ErasurePackage;
use ed25519_dalek::{SigningKey, VerifyingKey};
use hex;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc, Mutex};
use tokio::time::interval;
use tracing::{debug, error, info, warn};

#[derive(Debug, Clone)]
pub struct SmrolConfig {
    pub enable: bool,
    pub f: usize,
    pub capital_k: usize,
    pub epoch_timeout_ms: u64,
    pub pnfifo_threshold: usize,
}

impl Default for SmrolConfig {
    fn default() -> Self {
        Self {
            enable: true,
            f: 1,
            // k: 3, // 🔥🔥 文中 k=O(n)
            capital_k: 1, // 🔥🔥 修改点
            epoch_timeout_ms: 100,
            pnfifo_threshold: 3,
        }
    }
}

#[derive(Debug)]
struct SeqRequestTask {
    sender_id: usize,
    request: SeqRequest,
}

#[derive(Debug)]
struct SeqResponseTask {
    sender_id: usize,
    response: SeqResponse,
}

#[derive(Debug)]
struct SeqOrderTask {
    sender_id: usize,
    order: SeqOrder,
}

#[derive(Debug)]
struct SeqMedianTask {
    sender_id: usize,
    median: SeqMedian,
}

#[derive(Debug)]
struct SeqFinalTask {
    sender_id: usize,
    final_msg: SeqFinal,
}

pub struct SmrolManager {
    pub node_id: usize,
    pub config: SmrolConfig,
    pub n: usize,

    pub network: Arc<SmrolTcpNetwork>,
    pub consensus: Arc<Mutex<Consensus>>,
    pub sequencing: Arc<TransactionSequencing>,
    pub finalization: Arc<Mutex<OutputFinalization>>,
    hotstuff_adapter: Mutex<Option<Arc<SmrolHotStuffAdapter>>>,
    pub event_tx: broadcast::Sender<SystemEvent>,

    pub signing_key: SigningKey,
    pub verifying_keys: HashMap<usize, VerifyingKey>,
    broadcast_tx: mpsc::Sender<SmrolTransaction>,
}

impl SmrolManager {
    pub async fn new(
        node_id: usize,
        config: SmrolConfig,
        signing_key: SigningKey,
        verifying_keys: HashMap<usize, VerifyingKey>,
        peer_addrs: HashMap<usize, SocketAddr>,
        event_tx: broadcast::Sender<SystemEvent>,
    ) -> Result<Self, String> {
        let n = verifying_keys.len();

        let (threshold_share, threshold_public) =
            derive_threshold_keys(node_id, config.f, &verifying_keys)
                .map_err(|e| format!("derive threshold keys failed: {}", e))?;

        let pnfifo = Arc::new(
            PnfifoBc::new(
                node_id,
                n,
                signing_key.clone(),
                verifying_keys.clone(),
                peer_addrs,
            )
            .await?,
        );

        // start PNFIFO network listener
        pnfifo.start().await?;
        info!("✅ [SMROL] PNFIFO自动启动完成");

        let network = pnfifo.network();
        network.warm_up_connections();

        let finalization = Arc::new(Mutex::new(OutputFinalization::new(
            node_id,
            config.f,
            Arc::clone(&network),
            signing_key.clone(),
        )));

        let sequencing = TransactionSequencing::new(
            node_id,
            n,
            config.f,
            Arc::clone(&network),
            Arc::clone(&pnfifo),
            threshold_share,
            threshold_public.clone(),
            Arc::clone(&finalization),
        );

        let consensus = Consensus::new(
            node_id,
            n,
            config.f,
            Arc::clone(&network),
            signing_key.clone(),
            verifying_keys.clone(),
            Arc::clone(&finalization),
            event_tx.clone(),
        );

        let sequencing_arc = Arc::new(sequencing);
        let consensus_arc = Arc::new(Mutex::new(consensus));

        let broadcast_capacity = Self::smrol_broadcast_queue_capacity();
        let broadcast_workers = Self::smrol_broadcast_worker_count();
        let (broadcast_tx, broadcast_rx) =
            mpsc::channel::<SmrolTransaction>(broadcast_capacity);
        let broadcast_rx = Arc::new(tokio::sync::Mutex::new(broadcast_rx));

        for worker_id in 0..broadcast_workers {
            let sequencing_for_worker = Arc::clone(&sequencing_arc);
            let rx = Arc::clone(&broadcast_rx);
            tokio::spawn(async move {
                info!(
                    "[SMROL] Broadcast worker {} started with capacity {}",
                    worker_id,
                    broadcast_capacity
                );

                loop {
                    let transaction = {
                        let mut guard = rx.lock().await;
                        guard.recv().await
                    };

                    let Some(transaction) = transaction else {
                        warn!(
                            "⚠️ [SMROL] Broadcast worker {} exited (channel closed)",
                            worker_id
                        );
                        break;
                    };

                    let tx_id = transaction.id;
                    let result = sequencing_for_worker.smrol_broadcast(transaction).await;

                    match result {
                        Ok(_) => debug!(
                            "✅ [SMROL] Broadcast worker {} dispatched tx_id={} to sequencing",
                            worker_id,
                            tx_id
                        ),
                        Err(e) => error!(
                            "[SMROL] Broadcast worker {} sequencing broadcast failed: {}",
                            worker_id,
                            e
                        ),
                    }
                }
            });
        }

        Ok(Self {
            node_id,
            config,
            n,
            network,
            consensus: consensus_arc,
            sequencing: sequencing_arc,
            finalization,
            hotstuff_adapter: Mutex::new(None),
            event_tx,
            signing_key,
            verifying_keys,
            broadcast_tx,
        })
    }

    pub async fn set_hotstuff_adapter(&self, adapter: Arc<SmrolHotStuffAdapter>) {
        {
            let mut consensus = self.consensus.lock().await;
            consensus.set_hotstuff_adapter(adapter.clone());
        }
        let mut slot = self.hotstuff_adapter.lock().await;
        *slot = Some(adapter);
        info!("🔗 [SMROL] Connected to HotStuff adapter");
    }

    // public interface to process incoming SMROL transactions
    pub async fn process_smrol_transaction(
        &self,
        transaction: SmrolTransaction,
    ) -> Result<(), String> {
        debug!(
            "📥 [SMROL] Processing transaction: {}:{}->{}:{}",
            transaction.id, transaction.from, transaction.to, transaction.amount
        );

        if let Err(e) = self.broadcast_tx.send(transaction).await {
            return Err(format!("SMROL broadcast queue send failed: {}", e));
        }

        Ok(())
    }

    pub async fn start_message_loop(self: Arc<Self>) -> Result<(), String> {
        info!("🔄 [SMROL] Message loop starting for node {}", self.node_id);

        let sequencing_rx = self.network.get_sequencing_receiver();

        let request_capacity = Self::seq_request_queue_capacity();
        let response_capacity = Self::seq_response_queue_capacity();
        let order_capacity = Self::seq_order_queue_capacity();
        let median_capacity = Self::seq_median_queue_capacity();
        let final_capacity = Self::seq_final_queue_capacity();

        let request_workers = Self::seq_request_worker_count();
        let response_workers = Self::seq_response_worker_count();
        let order_workers = Self::seq_order_worker_count();
        let median_workers = Self::seq_median_worker_count();
        let final_workers = Self::seq_final_worker_count();

        let (request_tx, mut request_rx) = mpsc::channel::<SeqRequestTask>(request_capacity);
        let (response_tx, mut response_rx) = mpsc::channel::<SeqResponseTask>(response_capacity);
        let (order_tx, mut order_rx) = mpsc::channel::<SeqOrderTask>(order_capacity);
        let (median_tx, mut median_rx) = mpsc::channel::<SeqMedianTask>(median_capacity);
        let (final_tx, mut final_rx) = mpsc::channel::<SeqFinalTask>(final_capacity);

        {
            let sequencing_handle = Arc::clone(&self.sequencing);
            let interval_secs = Self::seq_cleanup_interval_secs();
            tokio::spawn(async move {
                let mut ticker = interval(Duration::from_secs(interval_secs));
                loop {
                    ticker.tick().await;
                    sequencing_handle.cleanup_expired_state().await;
                }
            });
        }

        // Request workers
        let mut request_worker_txs = Vec::with_capacity(request_workers);
        let f_param = self.config.f;
        let n_param = self.n;
        for worker_id in 0..request_workers {
            let (worker_tx, mut worker_rx) = mpsc::channel::<SeqRequestTask>(request_capacity);
            request_worker_txs.push(worker_tx);

            let sequencing_handle = Arc::clone(&self.sequencing);
            let manager_clone = Arc::clone(&self);
            tokio::spawn(async move {
                info!(
                    "[Sequencing] Request worker {} started (capacity {})",
                    worker_id,
                    request_capacity
                );

                while let Some(SeqRequestTask { sender_id, request }) = worker_rx.recv().await {
                    let data_shards = std::cmp::max(1, f_param + 1);
                    let total_shards = std::cmp::max(data_shards, n_param);
                    let encode_start = Instant::now();
                    let encoded = match ErasurePackage::encode(
                        &request.tx.payload,
                        data_shards,
                        total_shards,
                    ) {
                        Ok(pkg) => pkg,
                        Err(e) => {
                            error!("纠删码编码失败: {}", e);
                            continue;
                        }
                    };
                    let encode_elapsed = encode_start.elapsed();

                    let pnfifo_clone = Arc::clone(&sequencing_handle.pnfifo);
                    let process_id = sequencing_handle.process_id;

                    let (wait_ok, wait_duration) =
                        TransactionSequencing::wait_for_log_condition_static(
                            &pnfifo_clone,
                            process_id,
                            sender_id,
                            request.seq_num,
                        )
                        .await;

                    if !wait_ok {
                        warn!(
                            "❌ [Sequencing] Request worker {} log condition failed sender {} seq {}",
                            worker_id,
                            sender_id,
                            request.seq_num
                        );
                        continue;
                    }

                    let result = sequencing_handle
                        .handle_seq_request(
                            sender_id,
                            request,
                            encoded,
                            wait_duration,
                            encode_elapsed,
                        )
                        .await;

                    match result {
                        Ok(Some(entry)) => {
                            info!(
                                "[Manager] Sequencing output ready add_sequenced_transaction: vc_tx={} s_tx={}",
                                hex::encode(&entry.vc_tx[..std::cmp::min(8, entry.vc_tx.len())]),
                                entry.s_tx
                            );
                            if let Err(e) = manager_clone.add_sequenced_transaction(entry).await {
                                error!("[SMROL] 共识输入登记失败: {}", e);
                            }
                        }
                        Ok(None) => {}
                        Err(e) => error!(
                            "处理 Sequencing 请求消息失败 (worker {}): {}",
                            worker_id,
                            e
                        ),
                    }
                }

                warn!(
                    "⚠️ [Sequencing] Request worker {} exited (channel closed)",
                    worker_id
                );
            });
        }
        let request_senders = Arc::new(request_worker_txs);
        let request_index = Arc::new(AtomicUsize::new(0));
        {
            let request_senders = Arc::clone(&request_senders);
            let request_index = Arc::clone(&request_index);
            tokio::spawn(async move {
                while let Some(task) = request_rx.recv().await {
                    let len = request_senders.len();
                    let idx = request_index.fetch_add(1, Ordering::Relaxed) % len.max(1);
                    if let Err(e) = request_senders[idx].send(task).await {
                        warn!(
                            "[Sequencing] Request dispatcher failed to enqueue to worker {}: {}",
                            idx,
                            e
                        );
                    }
                }

                warn!("⚠️ [Sequencing] Request dispatcher exited (channel closed)");
            });
        }

        // Response workers
        let mut response_worker_txs = Vec::with_capacity(response_workers);
        for worker_id in 0..response_workers {
            let (worker_tx, mut worker_rx) = mpsc::channel::<SeqResponseTask>(response_capacity);
            response_worker_txs.push(worker_tx);

            let sequencing_handle = Arc::clone(&self.sequencing);
            tokio::spawn(async move {
                info!(
                    "[Sequencing] Response worker {} started (capacity {})",
                    worker_id,
                    response_capacity
                );

                while let Some(SeqResponseTask { sender_id, response }) = worker_rx.recv().await {
                    if let Err(e) = sequencing_handle
                        .handle_seq_response(sender_id, response)
                        .await
                    {
                        error!(
                            "处理 Sequencing 响应消息失败 (worker {}): {}",
                            worker_id,
                            e
                        );
                    }
                }

                warn!(
                    "⚠️ [Sequencing] Response worker {} exited (channel closed)",
                    worker_id
                );
            });
        }
        let response_senders = Arc::new(response_worker_txs);
        let response_index = Arc::new(AtomicUsize::new(0));
        {
            let response_senders = Arc::clone(&response_senders);
            let response_index = Arc::clone(&response_index);
            tokio::spawn(async move {
                while let Some(task) = response_rx.recv().await {
                    let len = response_senders.len();
                    let idx = response_index.fetch_add(1, Ordering::Relaxed) % len.max(1);
                    if let Err(e) = response_senders[idx].send(task).await {
                        warn!(
                            "[Sequencing] Response dispatcher failed to enqueue to worker {}: {}",
                            idx,
                            e
                        );
                    }
                }

                warn!("⚠️ [Sequencing] Response dispatcher exited (channel closed)");
            });
        }

        // Order workers
        let mut order_worker_txs = Vec::with_capacity(order_workers);
        for worker_id in 0..order_workers {
            let (worker_tx, mut worker_rx) = mpsc::channel::<SeqOrderTask>(order_capacity);
            order_worker_txs.push(worker_tx);

            let sequencing_handle = Arc::clone(&self.sequencing);
            tokio::spawn(async move {
                info!(
                    "[Sequencing] Order worker {} started (capacity {})",
                    worker_id,
                    order_capacity
                );

                while let Some(SeqOrderTask { sender_id, order }) = worker_rx.recv().await {
                    if let Err(e) = sequencing_handle
                        .handle_seq_order(sender_id, order)
                        .await
                    {
                        error!(
                            "处理 Sequencing Order 消息失败 (worker {}): {}",
                            worker_id,
                            e
                        );
                    }
                }

                warn!(
                    "⚠️ [Sequencing] Order worker {} exited (channel closed)",
                    worker_id
                );
            });
        }
        let order_senders = Arc::new(order_worker_txs);
        let order_index = Arc::new(AtomicUsize::new(0));
        {
            let order_senders = Arc::clone(&order_senders);
            let order_index = Arc::clone(&order_index);
            tokio::spawn(async move {
                while let Some(task) = order_rx.recv().await {
                    let len = order_senders.len();
                    let idx = order_index.fetch_add(1, Ordering::Relaxed) % len.max(1);
                    if let Err(e) = order_senders[idx].send(task).await {
                        warn!(
                            "[Sequencing] Order dispatcher failed to enqueue to worker {}: {}",
                            idx,
                            e
                        );
                    }
                }

                warn!("⚠️ [Sequencing] Order dispatcher exited (channel closed)");
            });
        }

        // Median workers
        let mut median_worker_txs = Vec::with_capacity(median_workers);
        for worker_id in 0..median_workers {
            let (worker_tx, mut worker_rx) = mpsc::channel::<SeqMedianTask>(median_capacity);
            median_worker_txs.push(worker_tx);

            let sequencing_handle = Arc::clone(&self.sequencing);
            tokio::spawn(async move {
                info!(
                    "[Sequencing] Median worker {} started (capacity {})",
                    worker_id,
                    median_capacity
                );

                while let Some(SeqMedianTask { sender_id, median }) = worker_rx.recv().await {
                    if let Err(e) = sequencing_handle
                        .handle_seq_median(sender_id, median)
                        .await
                    {
                        error!(
                            "处理 Sequencing Median 消息失败 (worker {}): {}",
                            worker_id,
                            e
                        );
                    }
                }

                warn!(
                    "⚠️ [Sequencing] Median worker {} exited (channel closed)",
                    worker_id
                );
            });
        }
        let median_senders = Arc::new(median_worker_txs);
        let median_index = Arc::new(AtomicUsize::new(0));
        {
            let median_senders = Arc::clone(&median_senders);
            let median_index = Arc::clone(&median_index);
            tokio::spawn(async move {
                while let Some(task) = median_rx.recv().await {
                    let len = median_senders.len();
                    let idx = median_index.fetch_add(1, Ordering::Relaxed) % len.max(1);
                    if let Err(e) = median_senders[idx].send(task).await {
                        warn!(
                            "[Sequencing] Median dispatcher failed to enqueue to worker {}: {}",
                            idx,
                            e
                        );
                    }
                }

                warn!("⚠️ [Sequencing] Median dispatcher exited (channel closed)");
            });
        }

        // Final workers
        let mut final_worker_txs = Vec::with_capacity(final_workers);
        for worker_id in 0..final_workers {
            let (worker_tx, mut worker_rx) = mpsc::channel::<SeqFinalTask>(final_capacity);
            final_worker_txs.push(worker_tx);

            let sequencing_handle = Arc::clone(&self.sequencing);
            let manager_clone = Arc::clone(&self);
            tokio::spawn(async move {
                info!(
                    "[Sequencing] Final worker {} started (capacity {})",
                    worker_id,
                    final_capacity
                );

                while let Some(SeqFinalTask { sender_id: _, final_msg }) = worker_rx.recv().await {
                    let result = sequencing_handle.handle_seq_final(final_msg).await;

                    match result {
                        Ok(Some(entry)) => {
                            info!(
                                "[Manager] Sequencing FINAL output ready vc_tx={} s_tx={}",
                                hex::encode(&entry.vc_tx[..std::cmp::min(8, entry.vc_tx.len())]),
                                entry.s_tx
                            );
                            if let Err(e) = manager_clone.add_sequenced_transaction(entry).await {
                                error!("[SMROL] 共识输入登记失败: {}", e);
                            }
                        }
                        Ok(None) => {}
                        Err(e) => error!(
                            "处理 Sequencing Final 消息失败 (worker {}): {}",
                            worker_id,
                            e
                        ),
                    }
                }

                warn!(
                    "⚠️ [Sequencing] Final worker {} exited (channel closed)",
                    worker_id
                );
            });
        }
        let final_senders = Arc::new(final_worker_txs);
        let final_index = Arc::new(AtomicUsize::new(0));
        {
            let final_senders = Arc::clone(&final_senders);
            let final_index = Arc::clone(&final_index);
            tokio::spawn(async move {
                while let Some(task) = final_rx.recv().await {
                    let len = final_senders.len();
                    let idx = final_index.fetch_add(1, Ordering::Relaxed) % len.max(1);
                    if let Err(e) = final_senders[idx].send(task).await {
                        warn!(
                            "[Sequencing] Final dispatcher failed to enqueue to worker {}: {}",
                            idx,
                            e
                        );
                    }
                }

                warn!("⚠️ [Sequencing] Final dispatcher exited (channel closed)");
            });
        }

        // Fan-out incoming sequencing messages
        // {
        //     let sequencing_rx = Arc::clone(&sequencing_rx);
        //     let request_tx = request_tx.clone();
        //     let response_tx = response_tx.clone();
        //     let order_tx = order_tx.clone();
        //     let median_tx = median_tx.clone();
        //     let final_tx = final_tx.clone();
        //     tokio::spawn(async move {
        //         let mut rx = sequencing_rx.lock().await;
        //         while let Some((sender_id, message)) = rx.recv().await {
        //             match message {
        //                 SmrolMessage::SeqRequest {
        //                     transaction,
        //                     sequence_number,
        //                     ..
        //                 } => {
        //                     match bincode::serialize(&transaction) {
        //                         Ok(payload) => {
        //                             let task = SeqRequestTask {
        //                                 sender_id,
        //                                 request: SeqRequest {
        //                                     seq_num: sequence_number,
        //                                     tx: Transaction { payload },
        //                                 },
        //                             };
        //                             if let Err(e) = request_tx.send(task).await {
        //                                 warn!("无法入队 SeqRequest 消息: {}", e);
        //                             }
        //                         }
        //                         Err(e) => error!("序列化 SmrolTransaction 失败: {}", e),
        //                     }
        //                 }
        //                 SmrolMessage::SeqResponse {
        //                     vc,
        //                     signature_share,
        //                     sequence_number,
        //                     ..
        //                 } => {
        //                     let task = SeqResponseTask {
        //                         sender_id,
        //                         response: SeqResponse {
        //                             vc,
        //                             s: sequence_number,
        //                             sigma: signature_share,
        //                         },
        //                     };
        //                     if let Err(e) = response_tx.send(task).await {
        //                         warn!("无法入队 SeqResponse 消息: {}", e);
        //                     }
        //                 }
        //                 SmrolMessage::SeqOrder { vc, responses, .. } => {
        //                     let records = responses
        //                         .into_iter()
        //                         .map(|(sender, sequence, signature)| SeqResponseRecord {
        //                             sender,
        //                             sequence,
        //                             signature,
        //                         })
        //                         .collect();
        //                     let task = SeqOrderTask {
        //                         sender_id,
        //                         order: SeqOrder { vc, records },
        //                     };
        //                     if let Err(e) = order_tx.send(task).await {
        //                         warn!("无法入队 SeqOrder 消息: {}", e);
        //                     }
        //                 }
        //                 SmrolMessage::SeqMedian {
        //                     vc,
        //                     median_sequence,
        //                     proof,
        //                     ..
        //                 } => {
        //                     let task = SeqMedianTask {
        //                         sender_id,
        //                         median: SeqMedian {
        //                             vc,
        //                             s_tx: median_sequence,
        //                             sigma_seq: proof,
        //                         },
        //                     };
        //                     if let Err(e) = median_tx.send(task).await {
        //                         warn!("无法入队 SeqMedian 消息: {}", e);
        //                     }
        //                 }
        //                 SmrolMessage::SeqFinal {
        //                     vc,
        //                     final_sequence,
        //                     combined_signature,
        //                     ..
        //                 } => {
        //                     let task = SeqFinalTask {
        //                         sender_id,
        //                         final_msg: SeqFinal {
        //                             vc,
        //                             s_tx: final_sequence,
        //                             sigma: combined_signature,
        //                         },
        //                     };
        //                     if let Err(e) = final_tx.send(task).await {
        //                         warn!("无法入队 SeqFinal 消息: {}", e);
        //                     }
        //                 }
        //                 other => {
        //                     debug!(
        //                         "[SMROL] Node {} 收到非 Sequencing 消息: {:?}",
        //                         sender_id,
        //                         std::mem::discriminant(&other)
        //                     );
        //                 }
        //             }
        //         }

        //         debug!("ℹ️ [SMROL] Sequencing input loop exited");
        //     });
        // }

        let request_tx_clone = request_tx.clone();
        let response_tx_clone = response_tx.clone();
        let order_tx_clone = order_tx.clone();
        let median_tx_clone = median_tx.clone();
        let final_tx_clone = final_tx.clone();

        tokio::spawn(async move {
            let mut rx = sequencing_rx.lock().await;
            while let Some((sender_id, message)) = rx.recv().await {
                match message {
                    SmrolMessage::SeqRequest {
                        transaction,
                        sequence_number,
                        ..
                    } => {
                        match bincode::serialize(&transaction) {
                            Ok(payload) => {
                                let task = SeqRequestTask {
                                    sender_id,
                                    request: SeqRequest {
                                        seq_num: sequence_number,
                                        tx: Transaction { payload },
                                    },
                                };

                                if let Err(e) = request_tx_clone.send(task).await {
                                    error!(
                                        "无法入队 SeqRequest 消息: {}",
                                        e
                                    );
                                }
                            }
                            Err(e) => error!(
                                "序列化 SmrolTransaction 失败: {}",
                                e
                            ),
                        }
                    }
                    SmrolMessage::SeqResponse {
                        vc,
                        signature_share,
                        sequence_number,
                        ..
                    } => {
                        let task = SeqResponseTask {
                            sender_id,
                            response: SeqResponse {
                                vc,
                                s: sequence_number,
                                sigma: signature_share,
                            },
                        };

                        if let Err(e) = response_tx_clone.send(task).await {
                            error!("无法入队 SeqResponse 消息: {}", e);
                        }
                    }
                    SmrolMessage::SeqOrder { vc, responses, .. } => {
                        let records = responses
                            .into_iter()
                            .map(|(sender, sequence, signature)| SeqResponseRecord {
                                sender,
                                sequence,
                                signature,
                            })
                            .collect();
                        let task = SeqOrderTask {
                            sender_id,
                            order: SeqOrder { vc, records },
                        };

                        if let Err(e) = order_tx_clone.send(task).await {
                            error!("无法入队 SeqOrder 消息: {}", e);
                        }
                    }
                    SmrolMessage::SeqMedian {
                        vc,
                        median_sequence,
                        proof,
                        ..
                    } => {
                        let task = SeqMedianTask {
                            sender_id,
                            median: SeqMedian {
                                vc,
                                s_tx: median_sequence,
                                sigma_seq: proof,
                            },
                        };

                        if let Err(e) = median_tx_clone.send(task).await {
                            error!("无法入队 SeqMedian 消息: {}", e);
                        }
                    }
                    SmrolMessage::SeqFinal {
                        vc,
                        final_sequence,
                        combined_signature,
                        ..
                    } => {
                        let task = SeqFinalTask {
                            sender_id,
                            final_msg: SeqFinal {
                                vc,
                                s_tx: final_sequence,
                                sigma: combined_signature,
                            },
                        };

                        if let Err(e) = final_tx_clone.send(task).await {
                            error!("无法入队 SeqFinal 消息: {}", e);
                        }
                    }
                    _ => {
                        debug!(
                            "[SMROL] Node {} 收到非 Sequencing 消息: {:?}",
                            sender_id,
                            std::mem::discriminant(&message)
                        );
                    }
                }
            }
            debug!("ℹ️ [SMROL] Sequencing input loop exited");
        });

        // Note: consensus/Finalization pipeline temporarily disabled; sequencing outputs
        // push directly into HotStuff via adapter. Keep the legacy consensus listener
        // disabled to avoid spawning redundant tasks and holding extra locks.
        // If SMROL consensus is re-enabled, restore the loop below.
        // let consensus_rx = self.network.get_consensus_receiver();
        // let consensus_handle = Arc::clone(&self.consensus);
        // let manager_for_consensus = Arc::clone(&self);
        // info!("🔄 [Manager] Node {} start_message_loop spawning handle_consensus_message", self.node_id);
        // tokio::spawn(async move {
        //     info!(
        //         "🔄 [Manager] handle_consensus_message started for node {}",
        //         manager_for_consensus.node_id
        //     );
        //     let mut rx = consensus_rx.lock().await;
        //     while let Some((sender_id, message)) = rx.recv().await {
        //         let mut consensus = consensus_handle.lock().await;
        //         if let Err(e) = consensus.handle_consensus_message(sender_id, message).await {
        //             error!("处理Consensus消息失败: {}", e);
        //         }
        //     }
        //     debug!(
        //         "ℹ️ [SMROL] Consensus loop exited for node {}",
        //         manager_for_consensus.node_id
        //     );
        // });

        info!("✅ [SMROL] 消息处理循环已启动");
        Ok(())
    }

    pub async fn invoke_consensus_and_finalize(&self, epoch: u64) -> Result<(), String> {
        info!("🏛️ [SMROL] Starting consensus for epoch {}", epoch);

        {
            let mut consensus = self.consensus.lock().await;
            consensus.run_consensus(epoch).await?;
        }
        Ok(())
    }

    pub async fn should_invoke_consensus(&self, epoch: u64) -> bool {
        let consensus = self.consensus.lock().await;
        consensus.get_mi_size(epoch) >= self.config.capital_k // line 39
    }

    // line 38: take over sequenced transaction and add to Mi and finalization
    pub async fn add_sequenced_transaction(&self, entry: TransactionEntry) -> Result<(), String> {
        let entry_meta = (entry.vc_tx.len(), entry.s_tx);

        let epoch = {
            let consensus = self.consensus.lock().await;
            consensus.get_current_epoch()
        };

        if let Some(tx_id) = Self::extract_tx_id(&entry) {
            if let Err(e) = self
                .event_tx
                .send(SystemEvent::SmrolOrderingCompleted { tx_ids: vec![tx_id] })
            {
                warn!(
                    "⚠️ [SMROL] Node {} failed to emit ordering completion for tx {}: {}",
                    self.node_id, tx_id, e
                );
            }
        }

        let hotstuff_string = if let Ok(tx) = bincode::deserialize::<SmrolTransaction>(&entry.payload) {
            tx.to_hotstuff_format(entry.s_tx)
        } else {
            format!(
                "{}:{}",
                entry.s_tx,
                hex::encode(&entry.vc_tx[..std::cmp::min(8, entry.vc_tx.len())])
            )
        };

        let adapter_opt = {
            let guard = self.hotstuff_adapter.lock().await;
            guard.clone()
        };

        if let Some(adapter) = adapter_opt {
            adapter.output_to_hotstuff(vec![hotstuff_string.clone()], epoch);
            info!(
                "🚀 [SMROL→HotStuff] Node {} delivered tx for slot {} epoch {}",
                self.node_id,
                entry_meta.1,
                epoch
            );
        } else {
            warn!(
                "⚠️ [SMROL] HotStuff adapter not connected, dropping tx slot {}",
                entry_meta.1
            );
        }

        // 临时关闭 finalization，专注测试 HotStuff push 性能
        // {
        //     let mut finalization = self.finalization.lock().await;
        //     finalization.add_to_mi(epoch, entry_for_finalization);
        // }

        // info!(
        //     "🧮 [Manager] 记录Sequencing输出并推给HotStuff: epoch={} vc_bytes={} s_tx={}",
        //     epoch, entry_meta.0, entry_meta.1
        // );

        Ok(())
    }

    fn seq_request_queue_capacity() -> usize {
        std::env::var("SMROL_SEQ_REQUEST_QUEUE_CAP")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(512)
    }

    fn seq_request_worker_count() -> usize {
        std::env::var("SMROL_SEQ_REQUEST_WORKERS")
            .ok()
            .and_then(|v| v.parse().ok())
            .map(|n: usize| n.max(1))
            .unwrap_or(2)
    }

    fn seq_response_queue_capacity() -> usize {
        std::env::var("SMROL_SEQ_RESPONSE_QUEUE_CAP")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(2048)
    }

    fn seq_response_worker_count() -> usize {
        std::env::var("SMROL_SEQ_RESPONSE_WORKERS")
            .ok()
            .and_then(|v| v.parse().ok())
            .map(|n: usize| n.max(1))
            .unwrap_or(2)
    }

    fn seq_order_queue_capacity() -> usize {
        std::env::var("SMROL_SEQ_ORDER_QUEUE_CAP")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(512)
    }

    fn seq_order_worker_count() -> usize {
        std::env::var("SMROL_SEQ_ORDER_WORKERS")
            .ok()
            .and_then(|v| v.parse().ok())
            .map(|n: usize| n.max(1))
            .unwrap_or(2)
    }

    fn seq_median_queue_capacity() -> usize {
        std::env::var("SMROL_SEQ_MEDIAN_QUEUE_CAP")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(512)
    }

    fn seq_median_worker_count() -> usize {
        std::env::var("SMROL_SEQ_MEDIAN_WORKERS")
            .ok()
            .and_then(|v| v.parse().ok())
            .map(|n: usize| n.max(1))
            .unwrap_or(2)
    }

    fn seq_final_queue_capacity() -> usize {
        std::env::var("SMROL_SEQ_FINAL_QUEUE_CAP")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(512)
    }

    fn seq_final_worker_count() -> usize {
        std::env::var("SMROL_SEQ_FINAL_WORKERS")
            .ok()
            .and_then(|v| v.parse().ok())
            .map(|n: usize| n.max(1))
            .unwrap_or(2)
    }

    fn seq_cleanup_interval_secs() -> u64 {
        std::env::var("SMROL_SEQ_CLEANUP_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|&v| v > 0)
            .unwrap_or(30)
    }

    fn smrol_broadcast_queue_capacity() -> usize {
        std::env::var("SMROL_BROADCAST_QUEUE_CAP")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1024)
    }

    fn smrol_broadcast_worker_count() -> usize {
        std::env::var("SMROL_BROADCAST_WORKERS")
            .ok()
            .and_then(|v| v.parse().ok())
            .map(|n: usize| n.max(1))
            .unwrap_or(2)
    }

    fn extract_tx_id(entry: &TransactionEntry) -> Option<u64> {
        if let Ok(tx) = bincode::deserialize::<SmrolTransaction>(&entry.payload) {
            Some(tx.id)
        } else if let Ok(text) = std::str::from_utf8(&entry.payload) {
            let parts: Vec<&str> = text.split(':').collect();
            if parts.len() >= 3 && parts[0] == "smrol" {
                return parts[2].parse().ok();
            }
            if !parts.is_empty() {
                return parts[0].parse().ok();
            }
            None
        } else {
            None
        }
    }

    pub async fn debug_status(&self) {
        let consensus = self.consensus.lock().await;
        info!("📊 [SMROL Status] Node {}", self.node_id);
        info!("  - Current epoch: {}", consensus.get_current_epoch());
        drop(consensus);
        let adapter_connected = self.hotstuff_adapter.lock().await.is_some();
        info!("  - HotStuff adapter connected: {}", adapter_connected);
        info!(
            "  - Network strong refs: {}",
            Arc::strong_count(&self.network)
        );
    }
}

pub fn load_smrol_config() -> SmrolConfig {
    use std::env;

    SmrolConfig {
        enable: env::var("SMROL_ENABLE")
            .unwrap_or_else(|_| "true".to_string())
            .parse()
            .unwrap_or(true),
        f: env::var("SMROL_F")
            .unwrap_or_else(|_| "1".to_string())
            .parse()
            .unwrap_or(1),
        capital_k: env::var("SMROL_K")
            .unwrap_or_else(|_| "1".to_string())
            .parse()
            .unwrap_or(1),
        epoch_timeout_ms: env::var("SMROL_EPOCH_TIMEOUT_MS")
            .unwrap_or_else(|_| "100".to_string())
            .parse()
            .unwrap_or(100),
        pnfifo_threshold: env::var("SMROL_PNFIFO_THRESHOLD")
            .unwrap_or_else(|_| "3".to_string())
            .parse()
            .unwrap_or(3),
    }
}
