use crate::smrol::{
    consensus::TransactionEntry,
    crypto::ErasurePackage,
    finalization::OutputFinalization,
    message::{SmrolMessage, SmrolTransaction},
    network::SmrolTcpNetwork,
    pnfifo::PnfifoBc,
    ModuleMessage,
};
use blsttc::{
    PublicKeySet, SecretKeyShare, Signature as ThresholdSignature, SignatureShare, SIG_SIZE,
};
use crossbeam::channel::{unbounded, Sender};
use dashmap::{mapref::entry::Entry, DashMap, DashSet};
use ed25519_dalek::{Signature as Ed25519Signature, Signer, SigningKey, Verifier, VerifyingKey};
use futures::task::AtomicWaker;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::task::{Context, Poll};
use std::thread;
use std::time::Instant;
use std::{
    collections::{HashMap, HashSet},
    convert::TryFrom,
    result,
};
use tokio::{
    sync::{mpsc as async_mpsc, Mutex as AsyncMutex, RwLock, Semaphore},
    time::{timeout, Duration},
};
use tracing::{debug, error, info, warn};
use tracing_subscriber::field::debug;

static DISABLE_THRESHOLD_SIG_VERIFICATION: Lazy<bool> =
    Lazy::new(|| match std::env::var("SMROL_DISABLE_THRESHOLD_SIG") {
        Ok(val) => match val.trim().to_ascii_lowercase().as_str() {
            "0" | "false" | "no" | "off" => false,
            "1" | "true" | "yes" | "on" => true,
            _ => true,
        },
        Err(_) => true,
    });

trait ThresholdJob: Send {
    fn run(self: Box<Self>);
}

struct ThresholdThreadPool {
    sender: Sender<Box<dyn ThresholdJob>>,
    pending_jobs: Arc<AtomicUsize>,
}

impl ThresholdThreadPool {
    fn new(worker_count: usize) -> Self {
        let (sender, receiver) = unbounded::<Box<dyn ThresholdJob>>();
        let pending_jobs = Arc::new(AtomicUsize::new(0));
        metrics::gauge!(
            "smrol.threshold_pending_jobs",
            "pool" => "threshold"
        )
        .set(0.0);
        for idx in 0..worker_count {
            let thread_receiver = receiver.clone();
            thread::Builder::new()
                .name(format!("threshold-worker-{}", idx))
                .spawn(move || {
                    for job in thread_receiver.iter() {
                        job.run();
                    }
                })
                .expect("failed to spawn threshold worker thread");
        }
        drop(receiver);
        Self {
            sender,
            pending_jobs,
        }
    }

    fn submit<R, F>(&self, label: &'static str, task: F) -> Result<ThresholdTaskFuture<R>, String>
    where
        R: Send + 'static,
        F: FnOnce() -> Result<R, String> + Send + 'static,
    {
        let state = Arc::new(TaskState::new());
        let pending_after = self.pending_jobs.fetch_add(1, Ordering::Relaxed) + 1;
        metrics::gauge!(
            "smrol.threshold_pending_jobs",
            "pool" => "threshold"
        )
        .set(pending_after as f64);
        let job = Box::new(ConcreteThresholdJob {
            task: Some(task),
            state: state.clone(),
            label,
            submitted_at: Instant::now(),
            pending: Arc::clone(&self.pending_jobs),
        });
        if let Err(_e) = self.sender.send(job) {
            let prev = self.pending_jobs.fetch_sub(1, Ordering::Relaxed);
            let remaining = prev.saturating_sub(1);
            metrics::gauge!(
                "smrol.threshold_pending_jobs",
                "pool" => "threshold"
            )
            .set(remaining as f64);
            return Err("threshold worker pool shut down".to_string());
        }
        Ok(ThresholdTaskFuture { state })
    }
}

static THRESHOLD_POOL: Lazy<ThresholdThreadPool> = Lazy::new(|| {
    let workers = thread::available_parallelism()
        .map(|n| (n.get() * 2).max(8))
        .unwrap_or(8);
    ThresholdThreadPool::new(workers)
});

const PNFIFO_BROADCAST_CONCURRENCY: usize = 4;
const ORDER_VERIFY_CONCURRENCY: usize = 4;
const REQUEST_WORKER_COUNT: usize = 4;
const MEDIAN_WORKER_COUNT: usize = 4;
const FINAL_WORKER_COUNT: usize = 4;
const MSG_TAG_SEQUENCE: u8 = 0x01;
const MSG_TAG_MEDIAN: u8 = 0x02;
const MSG_TAG_FINAL: u8 = 0x03;

async fn run_threshold_task<R, F>(label: &'static str, task: F) -> Result<R, String>
where
    R: Send + 'static,
    F: FnOnce() -> Result<R, String> + Send + 'static,
{
    THRESHOLD_POOL.submit(label, task)?.await
}

struct TaskState<R> {
    result: Mutex<Option<Result<R, String>>>,
    waker: AtomicWaker,
}

impl<R> TaskState<R> {
    fn new() -> Self {
        Self {
            result: Mutex::new(None),
            waker: AtomicWaker::new(),
        }
    }

    fn complete(&self, value: Result<R, String>) {
        let mut guard = self.result.lock().expect("task state poisoned");
        *guard = Some(value);
        drop(guard);
        self.waker.wake();
    }

    fn take_result(&self) -> Option<Result<R, String>> {
        self.result.lock().expect("task state poisoned").take()
    }

    fn register(&self, waker: &std::task::Waker) {
        self.waker.register(waker);
    }
}

struct ThresholdTaskFuture<R> {
    state: Arc<TaskState<R>>,
}

impl<R> std::future::Future for ThresholdTaskFuture<R>
where
    R: Send + 'static,
{
    type Output = Result<R, String>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(result) = self.state.take_result() {
            return Poll::Ready(result);
        }

        self.state.register(cx.waker());

        if let Some(result) = self.state.take_result() {
            Poll::Ready(result)
        } else {
            Poll::Pending
        }
    }
}

struct ConcreteThresholdJob<R, F>
where
    R: Send + 'static,
    F: FnOnce() -> Result<R, String> + Send + 'static,
{
    task: Option<F>,
    state: Arc<TaskState<R>>,
    label: &'static str,
    submitted_at: Instant,
    pending: Arc<AtomicUsize>,
}

impl<R, F> ThresholdJob for ConcreteThresholdJob<R, F>
where
    R: Send + 'static,
    F: FnOnce() -> Result<R, String> + Send + 'static,
{
    fn run(mut self: Box<Self>) {
        if let Some(task) = self.task.take() {
            let start = Instant::now();
            let wait = start.duration_since(self.submitted_at);
            metrics::histogram!(
                "smrol.threshold_task_wait_ms",
                "task" => self.label
            )
            .record(wait.as_secs_f64() * 1000.0);

            let result = task();
            let exec = start.elapsed();
            metrics::histogram!(
                "smrol.threshold_task_exec_ms",
                "task" => self.label
            )
            .record(exec.as_secs_f64() * 1000.0);

            let prev_pending = self.pending.fetch_sub(1, Ordering::Relaxed);
            let remaining = prev_pending.saturating_sub(1);
            metrics::gauge!(
                "smrol.threshold_pending_jobs",
                "pool" => "threshold"
            )
            .set(remaining as f64);
            self.state.complete(result);
        } else {
            let prev_pending = self.pending.fetch_sub(1, Ordering::Relaxed);
            let remaining = prev_pending.saturating_sub(1);
            metrics::gauge!(
                "smrol.threshold_pending_jobs",
                "pool" => "threshold"
            )
            .set(remaining as f64);
        }
    }
}

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
    pub tx_id: u64,
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
    pnfifo_broadcast_semaphore: Arc<Semaphore>,
    order_verify_semaphore: Arc<Semaphore>,
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
    response_shares: Arc<DashMap<VC, HashMap<usize, SeqResponseRecord>>>,
    completed_responses: DashSet<VC>,
    median_shares: DashMap<VC, HashMap<usize, SignatureShare>>,
    median_waiters: DashMap<VC, HashSet<usize>>,
    final_broadcasted: DashSet<VC>,
    finalized_vcs: DashSet<VC>,
    tx_sequence_map: DashMap<VC, u64>,
    seq_payloads: DashMap<u64, Vec<u8>>,
    broadcast_tx: async_mpsc::UnboundedSender<SmrolMessage>,
    request_tx: async_mpsc::UnboundedSender<ModuleMessage>,
    request_rx: Arc<AsyncMutex<async_mpsc::UnboundedReceiver<ModuleMessage>>>,
    response_tx: async_mpsc::UnboundedSender<ModuleMessage>,
    response_rx: Arc<AsyncMutex<async_mpsc::UnboundedReceiver<ModuleMessage>>>,
    order_tx: async_mpsc::UnboundedSender<ModuleMessage>,
    order_rx: Arc<AsyncMutex<async_mpsc::UnboundedReceiver<ModuleMessage>>>,
    order_verify_tx: async_mpsc::UnboundedSender<(usize, SeqOrder)>,
    order_verify_rx: Arc<AsyncMutex<async_mpsc::UnboundedReceiver<(usize, SeqOrder)>>>,
    order_verify_backlog: Arc<AtomicUsize>,
    order_backlog: Arc<AtomicUsize>,
    order_finalize_backlog: Arc<AtomicUsize>,
    order_finalize_tx: async_mpsc::UnboundedSender<(usize, SeqOrder)>,
    order_finalize_rx: Arc<AsyncMutex<async_mpsc::UnboundedReceiver<(usize, SeqOrder)>>>,
    median_tx: async_mpsc::UnboundedSender<ModuleMessage>,
    median_rx: Arc<AsyncMutex<async_mpsc::UnboundedReceiver<ModuleMessage>>>,
    final_tx: async_mpsc::UnboundedSender<ModuleMessage>,
    final_rx: Arc<AsyncMutex<async_mpsc::UnboundedReceiver<ModuleMessage>>>,
    request_backlog: Arc<AtomicUsize>,
    response_backlog: Arc<AtomicUsize>,
    median_backlog: Arc<AtomicUsize>,
    final_backlog: Arc<AtomicUsize>,
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
            info!("[sequencing] node {} started broadcast worker", node_id);
            while let Some(msg) = broadcast_rx.recv().await {
                if let Err(e) = network_clone.broadcast(msg).await {
                    error!("[sequencing] broadcast worker failed: {}", e);
                }
            }
            warn!(
                "[sequencing] node {} broadcast worker exited (channel closed)",
                node_id
            );
        });

        let (request_tx, request_rx_raw) = async_mpsc::unbounded_channel::<ModuleMessage>();
        let request_rx = Arc::new(AsyncMutex::new(request_rx_raw));
        let request_backlog = Arc::new(AtomicUsize::new(0));
        metrics::gauge!("smrol.channel_backlog", "channel" => "request").set(0.0);
        let (response_tx, response_rx_raw) = async_mpsc::unbounded_channel::<ModuleMessage>();
        let response_rx = Arc::new(AsyncMutex::new(response_rx_raw));
        let response_backlog = Arc::new(AtomicUsize::new(0));
        metrics::gauge!("smrol.channel_backlog", "channel" => "response").set(0.0);
        let (order_tx, order_rx_raw) = async_mpsc::unbounded_channel::<ModuleMessage>();
        let order_rx = Arc::new(AsyncMutex::new(order_rx_raw));
        let order_backlog = Arc::new(AtomicUsize::new(0));
        metrics::gauge!("smrol.channel_backlog", "channel" => "order").set(0.0);
        let (order_verify_tx, order_verify_rx_raw) =
            async_mpsc::unbounded_channel::<(usize, SeqOrder)>();
        let order_verify_rx = Arc::new(AsyncMutex::new(order_verify_rx_raw));
        let order_verify_backlog = Arc::new(AtomicUsize::new(0));
        metrics::gauge!(
            "smrol.channel_backlog",
            "channel" => "order_verify"
        )
        .set(0.0);
        let (order_finalize_tx, order_finalize_rx_raw) =
            async_mpsc::unbounded_channel::<(usize, SeqOrder)>();
        let order_finalize_rx = Arc::new(AsyncMutex::new(order_finalize_rx_raw));
        let order_finalize_backlog = Arc::new(AtomicUsize::new(0));
        metrics::gauge!("smrol.channel_backlog", "channel" => "order_finalize").set(0.0);
        let (median_tx, median_rx_raw) = async_mpsc::unbounded_channel::<ModuleMessage>();
        let median_rx = Arc::new(AsyncMutex::new(median_rx_raw));
        let median_backlog = Arc::new(AtomicUsize::new(0));
        metrics::gauge!("smrol.channel_backlog", "channel" => "median").set(0.0);
        let (final_tx, final_rx_raw) = async_mpsc::unbounded_channel::<ModuleMessage>();
        let final_rx = Arc::new(AsyncMutex::new(final_rx_raw));
        let final_backlog = Arc::new(AtomicUsize::new(0));
        metrics::gauge!("smrol.channel_backlog", "channel" => "final").set(0.0);

        let pnfifo_broadcast_semaphore = Arc::new(Semaphore::new(PNFIFO_BROADCAST_CONCURRENCY));
        let order_verify_semaphore = Arc::new(Semaphore::new(ORDER_VERIFY_CONCURRENCY));

        Self {
            f,
            n,
            pnfifo_threshold,
            process_id,
            network,
            pnfifo,
            pnfifo_broadcast_semaphore,
            order_verify_semaphore,
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
            response_shares: Arc::new(DashMap::new()),
            completed_responses: DashSet::new(),
            median_shares: DashMap::new(),
            median_waiters: DashMap::new(),
            final_broadcasted: DashSet::new(),
            finalized_vcs: DashSet::new(),
            tx_sequence_map: DashMap::new(),
            seq_payloads: DashMap::new(),
            broadcast_tx,
            request_tx,
            request_rx,
            response_tx,
            response_rx,
            order_tx,
            order_rx,
            order_verify_tx,
            order_verify_rx,
            order_verify_backlog,
            order_backlog,
            order_finalize_backlog,
            order_finalize_tx,
            order_finalize_rx,
            median_tx,
            median_rx,
            final_tx,
            final_rx,
            request_backlog,
            response_backlog,
            median_backlog,
            final_backlog,
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

        let payload = bincode::serialize(&tx)
            .map_err(|e| format!("Failed to serialize SmrolTransaction: {}", e))?;
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
            return Err(format!("Failed to broadcast SEQ-REQUEST: {}", e));
        }
        // tokio::task::yield_now().await;

        //debug: insert to originated vc earlier but with additional computation
        // let data_shards = std::cmp::max(1, self.f + 1);
        // let total_shards = std::cmp::max(data_shards, self.n);
        // let encoded = ErasurePackage::encode(&payload_clone, data_shards, total_shards)
        //     .map_err(|e| format!("Erasure coding failed: {}", e))?;
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

    pub fn start_processing(self: &Arc<Self>) {
        warn!(
            "[Sequencing] Node {} Threshold signature verification disabled: {}",
            self.process_id, *DISABLE_THRESHOLD_SIG_VERIFICATION
        );
        self.spawn_request_processor(); // workers
        self.spawn_response_processor();
        self.spawn_order_receiver(); // send to verify
        self.spawn_order_verifier(); // workers: semaphore
        self.spawn_order_finalizer(); // send median to node
        self.spawn_median_processor(); // workers
        self.spawn_final_processor(); // workers
    }

    pub fn request_sender(&self) -> async_mpsc::UnboundedSender<ModuleMessage> {
        self.request_tx.clone()
    }

    pub fn response_sender(&self) -> async_mpsc::UnboundedSender<ModuleMessage> {
        self.response_tx.clone()
    }

    pub fn order_sender(&self) -> async_mpsc::UnboundedSender<ModuleMessage> {
        self.order_tx.clone()
    }

    pub fn median_sender(&self) -> async_mpsc::UnboundedSender<ModuleMessage> {
        self.median_tx.clone()
    }

    pub fn final_sender(&self) -> async_mpsc::UnboundedSender<ModuleMessage> {
        self.final_tx.clone()
    }

    pub fn request_backlog_counter(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.request_backlog)
    }

    pub fn response_backlog_counter(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.response_backlog)
    }

    pub fn order_backlog_counter(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.order_backlog)
    }

    pub fn order_finalize_backlog_counter(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.order_finalize_backlog)
    }

    pub fn median_backlog_counter(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.median_backlog)
    }

    pub fn final_backlog_counter(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.final_backlog)
    }

    fn build_sequence_signature_message(vc: &[u8], sequence: u64) -> [u8; 40] {
        debug_assert!(vc.len() >= 32, "vc must contain at least 32 bytes");
        let mut buf = [0u8; 40];
        buf[..32].copy_from_slice(&vc[..32]);
        buf[32..].copy_from_slice(&sequence.to_be_bytes());
        buf
    }

    fn build_tagged_vc_message(tag: u8, vc: &[u8], value: u64) -> Vec<u8> {
        let mut msg = Vec::with_capacity(1 + vc.len() + 8);
        msg.push(tag);
        msg.extend_from_slice(vc);
        msg.extend_from_slice(&value.to_be_bytes());
        msg
    }

    fn build_median_signature_message(vc: &[u8], value: u64) -> Vec<u8> {
        Self::build_tagged_vc_message(MSG_TAG_MEDIAN, vc, value)
    }

    fn build_final_signature_message(vc: &[u8], value: u64) -> Vec<u8> {
        Self::build_tagged_vc_message(MSG_TAG_FINAL, vc, value)
    }
    fn spawn_request_processor(self: &Arc<Self>) {
        let worker_count = REQUEST_WORKER_COUNT.max(1);
        let mut worker_senders = Vec::with_capacity(worker_count);

        for worker_id in 0..worker_count {
            let (worker_tx, mut worker_rx) = async_mpsc::unbounded_channel::<ModuleMessage>();
            worker_senders.push(worker_tx);

            let backlog_counter = Arc::clone(&self.request_backlog);
            let sequencing = Arc::clone(self);
            tokio::spawn(async move {
                info!(
                    "[Sequencing] Node {} request worker-{} started",
                    sequencing.process_id, worker_id
                );

                let mut count = 0u32;
                let mut total_time = Duration::ZERO;
                let mut max_time = Duration::ZERO;
                let mut last_log = Instant::now();

                while let Some((sender_id, message)) = worker_rx.recv().await {
                    let previous = backlog_counter.fetch_sub(1, Ordering::Relaxed);
                    let backlog = previous.saturating_sub(1);
                    metrics::gauge!("smrol.channel_backlog", "channel" => "request")
                        .set(backlog as f64);

                    let start = Instant::now();
                    match message {
                        SmrolMessage::SeqRequest {
                            tx_hash,
                            transaction,
                            sender_id: _msg_sender,
                            sequence_number,
                        } => {
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
                                    "⚠️ [Sequencing] Node {} request worker-{} failed: {}",
                                    sequencing.process_id, worker_id, e
                                );
                            }
                        }
                        _ => {
                            warn!(
                                "⚠️ [Sequencing] Node {} request worker-{} received unexpected message",
                                sequencing.process_id, worker_id
                            );
                        }
                    }

                    let elapsed = start.elapsed();
                    if elapsed > Duration::from_millis(10) {
                        warn!(
                            "🐌 [Sequencing] Request worker-{} slow for node {}: {:?}",
                            worker_id, sequencing.process_id, elapsed
                        );
                    }

                    count += 1;
                    total_time += elapsed;
                    max_time = max_time.max(elapsed);
                    if last_log.elapsed() > Duration::from_secs(1) {
                        let avg = if count > 0 {
                            total_time / count
                        } else {
                            Duration::ZERO
                        };
                        warn!(
                            "📊 [Critical] Request worker-{} stats node {}: {} msgs, avg={:?}, max={:?}, rate={}/s",
                            worker_id,
                            sequencing.process_id,
                            count,
                            avg,
                            max_time,
                            count
                        );
                        count = 0;
                        total_time = Duration::ZERO;
                        max_time = Duration::ZERO;
                        last_log = Instant::now();
                    }
                }

                warn!(
                    "⚠️ [Sequencing] Node {} request worker-{} exiting (channel closed)",
                    sequencing.process_id, worker_id
                );
            });
        }

        let request_rx = Arc::clone(&self.request_rx);
        let process_id = self.process_id;
        tokio::spawn(async move {
            let mut next_idx = 0usize;
            let worker_count = worker_senders.len();
            if worker_count == 0 {
                return;
            }

            loop {
                let maybe_message = {
                    let mut rx = request_rx.lock().await;
                    rx.recv().await
                };

                let Some(mut item) = maybe_message else {
                    break;
                };

                let mut attempts = 0usize;
                loop {
                    let idx = next_idx % worker_count;
                    next_idx = next_idx.wrapping_add(1);
                    attempts += 1;

                    match worker_senders[idx].send(item) {
                        Ok(_) => break,
                        Err(err) => {
                            item = err.0;
                            warn!(
                                "⚠️ [Sequencing] Node {} request dispatcher failed to deliver to worker {}",
                                process_id,
                                idx
                            );

                            if attempts >= worker_count {
                                warn!(
                                    "⚠️ [Sequencing] Node {} request dispatcher dropping request after exhausting workers",
                                    process_id
                                );
                                break;
                            }
                        }
                    }
                }
            }

            info!(
                "[Sequencing] Node {} request dispatcher exiting (channel closed)",
                process_id
            );
        });
    }

    fn spawn_response_processor(self: &Arc<Self>) {
        let response_rx = Arc::clone(&self.response_rx);
        let backlog_counter = Arc::clone(&self.response_backlog);
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
                let previous = backlog_counter.fetch_sub(1, Ordering::Relaxed);
                let backlog = previous.saturating_sub(1);
                metrics::gauge!("smrol.channel_backlog", "channel" => "response")
                    .set(backlog as f64);
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

    fn spawn_order_receiver(self: &Arc<Self>) {
        let order_rx = Arc::clone(&self.order_rx);
        let backlog_counter = Arc::clone(&self.order_backlog);
        let sequencing = Arc::clone(self);
        tokio::spawn(async move {
            info!(
                "[Sequencing] Node {} order receiver started",
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
                let previous = backlog_counter.fetch_sub(1, Ordering::Relaxed);
                let backlog = previous.saturating_sub(1);
                metrics::gauge!("smrol.channel_backlog", "channel" => "order").set(backlog as f64);
                count += 1;
                let start = Instant::now();
                if let SmrolMessage::SeqOrder {
                    vc,
                    responses,
                    sender_id: _msg_sender,
                } = message
                {
                    if let Err(e) = sequencing.process_seq_order_message(sender_id, vc, responses) {
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
                "⚠️ [Sequencing] Node {} order receiver exiting (channel closed)",
                sequencing.process_id
            );
        });
    }

    fn spawn_order_verifier(self: &Arc<Self>) {
        let verify_rx = Arc::clone(&self.order_verify_rx);
        let finalize_tx = self.order_finalize_tx.clone();
        let sequencing = Arc::clone(self);
        let backlog_counter = Arc::clone(&self.order_verify_backlog);
        let finalize_backlog = Arc::clone(&self.order_finalize_backlog);
        let verify_semaphore = Arc::clone(&self.order_verify_semaphore);

        struct OrderVerifyStats {
            count: u32,
            total_time: Duration,
            max_time: Duration,
            last_log: Instant,
        }

        impl OrderVerifyStats {
            fn new() -> Self {
                Self {
                    count: 0,
                    total_time: Duration::ZERO,
                    max_time: Duration::ZERO,
                    last_log: Instant::now(),
                }
            }
        }

        let stats = Arc::new(Mutex::new(OrderVerifyStats::new()));

        tokio::spawn(async move {
            info!(
                "[Sequencing] Node {} order verify processor started",
                sequencing.process_id
            );

            while let Some((sender_id, order)) = {
                let mut rx = verify_rx.lock().await;
                rx.recv().await
            } {
                let previous = backlog_counter.fetch_sub(1, Ordering::Relaxed);
                let backlog = previous.saturating_sub(1);
                metrics::gauge!(
                    "smrol.channel_backlog",
                    "channel" => "order_verify"
                )
                .set(backlog as f64);

                let order_arc = Arc::new(order);
                let sequencing_for_task = Arc::clone(&sequencing);
                let verifying_keys = Arc::clone(&sequencing.verifying_keys);
                let response_cache = Arc::clone(&sequencing.response_shares);
                let finalize_tx = finalize_tx.clone();
                let finalize_backlog = Arc::clone(&finalize_backlog);
                let stats = Arc::clone(&stats);
                let verify_semaphore = Arc::clone(&verify_semaphore);
                let order_task = Arc::clone(&order_arc);

                tokio::spawn(async move {
                    let permit = match verify_semaphore.acquire_owned().await {
                        Ok(permit) => permit,
                        Err(_) => {
                            warn!(
                                "⚠️ [Sequencing] Node {} order verify semaphore closed",
                                sequencing_for_task.process_id
                            );
                            return;
                        }
                    };

                    let start = Instant::now();
                    let order_for_check = Arc::clone(&order_task);
                    let required = 2 * sequencing_for_task.f + 1;
                    let verify_result = match verify_seq_order_records(
                        order_for_check,
                        verifying_keys,
                        required,
                        response_cache,
                    )
                    .await
                    {
                        Ok(res) => res,
                        Err(e) => {
                            warn!(
                                "⚠️ [Sequencing] Node {} order verify task failed: {}",
                                sequencing_for_task.process_id, e
                            );
                            false
                        }
                    };

                    if verify_result {
                        let pending = finalize_backlog.fetch_add(1, Ordering::Relaxed) + 1;
                        metrics::gauge!(
                            "smrol.channel_backlog",
                            "channel" => "order_finalize"
                        )
                        .set(pending as f64);
                        if let Err(e) = finalize_tx.send((sender_id, (*order_task).clone())) {
                            warn!(
                                "⚠️ [Sequencing] Node {} enqueue verified order failed: {}",
                                sequencing_for_task.process_id, e
                            );
                        }
                    } else {
                        debug!(
                            "⚠️ [Sequencing] Node {} dropping invalid SeqOrder from {}",
                            sequencing_for_task.process_id, sender_id
                        );
                    }

                    let elapsed = start.elapsed();
                    if elapsed > Duration::from_millis(10) {
                        warn!(
                            "🐌 [Sequencing] Order verify processor slow for node {}: {:?}",
                            sequencing_for_task.process_id, elapsed
                        );
                    }

                    {
                        let mut stats_guard = stats.lock().unwrap();
                        stats_guard.count += 1;
                        stats_guard.total_time += elapsed;
                        stats_guard.max_time = stats_guard.max_time.max(elapsed);
                        if stats_guard.last_log.elapsed() > Duration::from_secs(1) {
                            let count = stats_guard.count;
                            let total_time = stats_guard.total_time;
                            let max_time = stats_guard.max_time;
                            let avg = if count > 0 {
                                total_time / count
                            } else {
                                Duration::ZERO
                            };
                            debug!(
                                "📊 [Sequencing] Order verify stats node {}: {} msgs, avg={:?}, max={:?}, rate={}/s",
                                sequencing_for_task.process_id,
                                count,
                                avg,
                                max_time,
                                count
                            );
                            stats_guard.count = 0;
                            stats_guard.total_time = Duration::ZERO;
                            stats_guard.max_time = Duration::ZERO;
                            stats_guard.last_log = Instant::now();
                        }
                    }

                    drop(permit);
                });
            }

            warn!(
                "⚠️ [Sequencing] Node {} order verify processor exiting (channel closed)",
                sequencing.process_id
            );
        });
    }

    fn spawn_order_finalizer(self: &Arc<Self>) {
        let worker_count = FINAL_WORKER_COUNT.max(1);

        let mut worker_senders = Vec::with_capacity(worker_count);

        for worker_id in 0..worker_count {
            let (worker_tx, mut worker_rx) = async_mpsc::unbounded_channel::<(usize, SeqOrder)>();
            worker_senders.push(worker_tx);

            let backlog_counter = Arc::clone(&self.order_finalize_backlog);
            let sequencing = Arc::clone(self);
            tokio::spawn(async move {
                info!(
                    "[Sequencing] Node {} order finalizer worker-{} started",
                    sequencing.process_id, worker_id
                );

                let mut count = 0u32;
                let mut total_time = Duration::ZERO;
                let mut max_time = Duration::ZERO;
                let mut last_log = Instant::now();

                while let Some((sender_id, order)) = worker_rx.recv().await {
                    let previous = backlog_counter.fetch_sub(1, Ordering::Relaxed);
                    let backlog = previous.saturating_sub(1);
                    metrics::gauge!(
                        "smrol.channel_backlog",
                        "channel" => "order_finalize"
                    )
                    .set(backlog as f64);

                    let start = Instant::now();
                    if let Err(e) = sequencing.finalize_seq_order(sender_id, order).await {
                        warn!(
                            "⚠️ [Sequencing] Node {} order finalizer worker-{} failed: {}",
                            sequencing.process_id, worker_id, e
                        );
                    }
                    let elapsed = start.elapsed();
                    if elapsed > Duration::from_millis(10) {
                        warn!(
                            "🐌 [Sequencing] Order finalizer worker-{} slow for node {}: {:?}",
                            worker_id, sequencing.process_id, elapsed
                        );
                    }

                    count += 1;
                    total_time += elapsed;
                    max_time = max_time.max(elapsed);
                    if last_log.elapsed() > Duration::from_secs(1) {
                        let avg = if count > 0 {
                            total_time / count
                        } else {
                            Duration::ZERO
                        };
                        debug!(
                            "📊 [Sequencing] Order finalizer worker-{} stats node {}: {} msgs, avg={:?}, max={:?}, rate={}/s",
                            worker_id,
                            sequencing.process_id,
                            count,
                            avg,
                            max_time,
                            count
                        );
                        count = 0;
                        total_time = Duration::ZERO;
                        max_time = Duration::ZERO;
                        last_log = Instant::now();
                    }
                }

                warn!(
                    "⚠️ [Sequencing] Node {} order finalizer worker-{} exiting (channel closed)",
                    sequencing.process_id, worker_id
                );
            });
        }

        let finalize_rx = Arc::clone(&self.order_finalize_rx);
        let process_id = self.process_id;
        tokio::spawn(async move {
            let mut next_idx = 0usize;
            let worker_count = worker_senders.len();
            if worker_count == 0 {
                return;
            }

            loop {
                let maybe_message = {
                    let mut rx = finalize_rx.lock().await;
                    rx.recv().await
                };

                let Some(mut item) = maybe_message else {
                    break;
                };

                let mut attempts = 0usize;
                loop {
                    let idx = next_idx % worker_count;
                    next_idx = next_idx.wrapping_add(1);
                    attempts += 1;

                    match worker_senders[idx].send(item) {
                        Ok(_) => break,
                        Err(err) => {
                            item = err.0;
                            warn!(
                                "⚠️ [Sequencing] Node {} order finalizer dispatcher failed to deliver to worker {}",
                                process_id,
                                idx
                            );

                            if attempts >= worker_count {
                                warn!(
                                    "⚠️ [Sequencing] Node {} order finalizer dispatcher dropping order after exhausting workers",
                                    process_id
                                );
                                break;
                            }
                        }
                    }
                }
            }

            info!(
                "[Sequencing] Node {} order finalizer dispatcher exiting (channel closed)",
                process_id
            );
        });
    }

    fn spawn_median_processor(self: &Arc<Self>) {
        let worker_count = MEDIAN_WORKER_COUNT.max(1);

        let mut worker_senders = Vec::with_capacity(worker_count);

        for worker_id in 0..worker_count {
            let (worker_tx, mut worker_rx) = async_mpsc::unbounded_channel::<ModuleMessage>();
            worker_senders.push(worker_tx);

            let backlog_counter = Arc::clone(&self.median_backlog);
            let sequencing = Arc::clone(self);
            tokio::spawn(async move {
                info!(
                    "[Sequencing] Node {} median worker-{} started",
                    sequencing.process_id, worker_id
                );

                let mut count = 0u32;
                let mut total_time = Duration::ZERO;
                let mut max_time = Duration::ZERO;
                let mut last_log = Instant::now();

                while let Some((sender_id, message)) = worker_rx.recv().await {
                    let previous = backlog_counter.fetch_sub(1, Ordering::Relaxed);
                    let backlog = previous.saturating_sub(1);
                    metrics::gauge!("smrol.channel_backlog", "channel" => "median")
                        .set(backlog as f64);

                    let start = Instant::now();
                    match message {
                        SmrolMessage::SeqMedian {
                            vc,
                            median_sequence,
                            proof,
                            sender_id: _msg_sender,
                        } => {
                            if let Err(e) = sequencing
                                .process_seq_median_message(sender_id, vc, median_sequence, proof)
                                .await
                            {
                                warn!(
                                    "⚠️ [Sequencing] Node {} median worker-{} failed: {}",
                                    sequencing.process_id, worker_id, e
                                );
                            }
                        }
                        _ => {
                            warn!(
                                "⚠️ [Sequencing] Node {} median worker-{} received unexpected message",
                                sequencing.process_id, worker_id
                            );
                        }
                    }

                    let elapsed = start.elapsed();
                    if elapsed > Duration::from_millis(10) {
                        warn!(
                            "🐌 [Sequencing] Median worker-{} slow for node {}: {:?}",
                            worker_id, sequencing.process_id, elapsed
                        );
                    }

                    count += 1;
                    total_time += elapsed;
                    max_time = max_time.max(elapsed);
                    if last_log.elapsed() > Duration::from_secs(1) {
                        let avg = if count > 0 {
                            total_time / count
                        } else {
                            Duration::ZERO
                        };
                        warn!(
                            "📊 [Critical] Median worker-{} stats node {}: {} msgs, avg={:?}, max={:?}, rate={}/s",
                            worker_id,
                            sequencing.process_id,
                            count,
                            avg,
                            max_time,
                            count
                        );
                        count = 0;
                        total_time = Duration::ZERO;
                        max_time = Duration::ZERO;
                        last_log = Instant::now();
                    }
                }

                warn!(
                    "⚠️ [Sequencing] Node {} median worker-{} exiting (channel closed)",
                    sequencing.process_id, worker_id
                );
            });
        }

        let median_rx = Arc::clone(&self.median_rx);
        let process_id = self.process_id;
        tokio::spawn(async move {
            let mut next_idx = 0usize;
            let worker_count = worker_senders.len();
            if worker_count == 0 {
                return;
            }

            loop {
                let maybe_message = {
                    let mut rx = median_rx.lock().await;
                    rx.recv().await
                };

                let Some(mut item) = maybe_message else {
                    break;
                };

                let mut attempts = 0usize;
                loop {
                    let idx = next_idx % worker_count;
                    next_idx = next_idx.wrapping_add(1);
                    attempts += 1;

                    match worker_senders[idx].send(item) {
                        Ok(_) => break,
                        Err(err) => {
                            item = err.0;
                            warn!(
                                "⚠️ [Sequencing] Node {} median dispatcher failed to deliver to worker {}",
                                process_id, idx
                            );

                            if attempts >= worker_count {
                                warn!(
                                    "⚠️ [Sequencing] Node {} median dispatcher dropping median after exhausting workers",
                                    process_id
                                );
                                break;
                            }
                        }
                    }
                }
            }

            info!(
                "[Sequencing] Node {} median dispatcher exiting (channel closed)",
                process_id
            );
        });
    }

    fn spawn_final_processor(self: &Arc<Self>) {
        let worker_count = FINAL_WORKER_COUNT.max(1);

        let mut worker_senders = Vec::with_capacity(worker_count);

        for worker_id in 0..worker_count {
            let (worker_tx, mut worker_rx) = async_mpsc::unbounded_channel::<ModuleMessage>();
            worker_senders.push(worker_tx);

            let backlog_counter = Arc::clone(&self.final_backlog);
            let sequencing = Arc::clone(self);
            tokio::spawn(async move {
                info!(
                    "[Sequencing] Node {} final worker-{} started",
                    sequencing.process_id, worker_id
                );

                let mut count = 0u32;
                let mut total_time = Duration::ZERO;
                let mut max_time = Duration::ZERO;
                let mut last_log = Instant::now();

                while let Some((sender_id, message)) = worker_rx.recv().await {
                    let previous = backlog_counter.fetch_sub(1, Ordering::Relaxed);
                    let backlog = previous.saturating_sub(1);
                    metrics::gauge!("smrol.channel_backlog", "channel" => "final")
                        .set(backlog as f64);

                    let start = Instant::now();
                    match message {
                        SmrolMessage::SeqFinal {
                            vc,
                            final_sequence,
                            combined_signature,
                            tx_id,
                            sender_id: _msg_sender,
                        } => {
                            let final_msg = SeqFinal {
                                vc,
                                s_tx: final_sequence,
                                sigma: combined_signature,
                                tx_id,
                            };
                            if let Err(e) = sequencing.process_seq_final_message(final_msg).await {
                                warn!(
                                    "⚠️ [Sequencing] Node {} final worker-{} failed: {}",
                                    sequencing.process_id, worker_id, e
                                );
                            }
                        }
                        _ => {
                            warn!(
                                "⚠️ [Sequencing] Node {} final worker-{} received unexpected message",
                                sequencing.process_id, worker_id
                            );
                        }
                    }

                    let elapsed = start.elapsed();
                    if elapsed > Duration::from_millis(10) {
                        warn!(
                            "🐌 [Sequencing] Final worker-{} slow for node {}: {:?}",
                            worker_id, sequencing.process_id, elapsed
                        );
                    }

                    count += 1;
                    total_time += elapsed;
                    max_time = max_time.max(elapsed);
                    if last_log.elapsed() > Duration::from_secs(1) {
                        let avg = if count > 0 {
                            total_time / count
                        } else {
                            Duration::ZERO
                        };
                        warn!(
                            "📊 [Critical] Final worker-{} stats node {}: {} msgs, avg={:?}, max={:?}, rate={}/s",
                            worker_id,
                            sequencing.process_id,
                            count,
                            avg,
                            max_time,
                            count
                        );
                        count = 0;
                        total_time = Duration::ZERO;
                        max_time = Duration::ZERO;
                        last_log = Instant::now();
                    }
                }

                warn!(
                    "⚠️ [Sequencing] Node {} final worker-{} exiting (channel closed)",
                    sequencing.process_id, worker_id
                );
            });
        }

        let final_rx = Arc::clone(&self.final_rx);
        let process_id = self.process_id;
        tokio::spawn(async move {
            let mut next_idx = 0usize;
            let worker_count = worker_senders.len();
            if worker_count == 0 {
                return;
            }

            loop {
                let maybe_message = {
                    let mut rx = final_rx.lock().await;
                    rx.recv().await
                };

                let Some(mut item) = maybe_message else {
                    break;
                };

                let mut attempts = 0usize;
                loop {
                    let idx = next_idx % worker_count;
                    next_idx = next_idx.wrapping_add(1);
                    attempts += 1;

                    match worker_senders[idx].send(item) {
                        Ok(_) => break,
                        Err(err) => {
                            item = err.0;
                            warn!(
                                "⚠️ [Sequencing] Node {} final dispatcher failed to deliver to worker {}",
                                process_id, idx
                            );

                            if attempts >= worker_count {
                                warn!(
                                    "⚠️ [Sequencing] Node {} final dispatcher dropping final message after exhausting workers",
                                    process_id
                                );
                                break;
                            }
                        }
                    }
                }
            }

            info!(
                "[Sequencing] Node {} final dispatcher exiting (channel closed)",
                process_id
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
            bincode::serialize(&transaction).map_err(|e| format!("Serialization failed: {}", e))?;
        let data_shards = std::cmp::max(1, self.pnfifo_threshold);
        let total_shards = std::cmp::max(data_shards, self.n);
        let serialized_for_encode = serialized.clone();
        let encode_start = Instant::now();
        let encoded_package = run_threshold_task("erasure_encode", move || {
            ErasurePackage::encode(&serialized_for_encode, data_shards, total_shards)
                .map_err(|e| format!("Erasure coding task failed: {}", e))
        })
        .await?;
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
        let vc_key = VC::from_slice(&vc);
        if self.completed_responses.contains(&vc_key) {
            return Ok(());
        }
        let response = SeqResponse {
            vc,
            s: sequence_number,
            sigma: signature_share,
        };
        self.handle_seq_response(sender_id, response).await
    }

    #[tracing::instrument(skip(self))]
    fn process_seq_order_message(
        &self,
        sender_id: usize,
        vc: Vec<u8>,
        responses: Vec<(usize, u64, Vec<u8>)>,
    ) -> Result<(), String> {
        if responses.len() != 2 * self.f + 1 {
            return Ok(());
        }
        let records = responses
            .into_iter()
            .map(|(sender, sequence, signature)| SeqResponseRecord {
                sender,
                sequence,
                signature,
            })
            .collect();
        let order = SeqOrder { vc, records };
        self.order_verify_tx
            .send((sender_id, order))
            .map_err(|e| format!("enqueue seq-order failed: {}", e))?;

        let backlog = self.order_verify_backlog.fetch_add(1, Ordering::Relaxed) + 1;
        metrics::gauge!(
            "smrol.channel_backlog",
            "channel" => "order_verify"
        )
        .set(backlog as f64);

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn process_seq_median_message(
        &self,
        sender_id: usize,
        vc: Vec<u8>,
        median_sequence: u64,
        proof: Vec<u8>,
    ) -> Result<(), String> {
        let vc_key = VC::from_slice(&vc);
        if self.finalized_vcs.contains(&vc_key) || self.final_broadcasted.contains(&vc_key) {
            return Ok(());
        }
        let median = SeqMedian {
            vc,
            s_tx: median_sequence,
            sigma_seq: proof,
        };
        self.handle_seq_median(sender_id, median).await
    }

    #[tracing::instrument(skip(self))]
    async fn process_seq_final_message(&self, final_msg: SeqFinal) -> Result<(), String> {
        let vc_key = VC::from_slice(&final_msg.vc);
        if self.finalized_vcs.contains(&vc_key) {
            return Ok(());
        }
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
        // tokio::task::yield_now().await;
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
        // info!(
        //     "📥 [Sequencing] Line 2:4: received SEQ-REQUEST, node {} seq_num: {} tx={}",
        //     sender,
        //     req.seq_num,
        //     hex::encode(&req.tx.payload[..std::cmp::min(8, req.tx.payload.len())])
        // );

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

        // debug!(
        //     "🧮 [Sequencing] Line 2:7-11 node={} assigned sequence {} for vc={} (req_seq={} from {})",
        //     self.process_id,
        //     s,
        //     hex::encode(&vc_tx[..std::cmp::min(8, vc_tx.len())]),
        //     req.seq_num,
        //     sender
        // );
        let process_id = self.process_id;

        // Persist mapping for later finalize broadcast
        self.pending_txs
            .entry(vc_key)
            .or_insert_with(|| req.tx.clone());
        debug!(
            "[Sequencing] Node {} stored pending_tx for vc={}, now has {} pending_txs",
            self.process_id,
            hex::encode(&vc_tx[..std::cmp::min(8, vc_tx.len())]),
            self.pending_txs.len()
        );
        // payload will travel with SeqFinal; skip extra caches for now

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
        let slot = s;
        let req_seq_num = req.seq_num;
        let node_id = self.process_id;
        let pnfifo = Arc::clone(&self.pnfifo);
        let semaphore = Arc::clone(&self.pnfifo_broadcast_semaphore);
        let vc_for_broadcast = vc_tx.clone();
        tokio::spawn(async move {
            let permit = match semaphore.acquire_owned().await {
                Ok(permit) => permit,
                Err(_) => {
                    warn!(
                        "❌ [Sequencing] PNFIFO broadcast semaphore closed for node {} slot {}",
                        node_id, slot
                    );
                    return;
                }
            };
            let t0 = Instant::now();
            match pnfifo.broadcast(slot, vc_for_broadcast).await {
                Ok(_) => {
                    let since_t0 = t0.elapsed();
                    if since_t0 > Duration::from_millis(10) {
                        debug!("[Check] PNFIFO broadcast enqueue took {:?}.", since_t0);
                    }
                    debug!(
                        "📡 [Sequencing] Line 2:15 node={} forwarded req.seq_num {} vc to PNFIFO slot {}",
                        node_id, req_seq_num, slot
                    );
                }
                Err(err) => {
                    warn!(
                        "❌ [Sequencing] PNFIFO broadcast enqueue failed for node {} slot {}: {}",
                        node_id, slot, err
                    );
                }
            }
            drop(permit);
        });
        let enqueue_delay = enqueue_start.elapsed();

        debug!(
            "[Sequencing-Timing] ⏰ FIFO broadcast dispatch took {:?}, wait {:?}, encoding {:?}.",
            enqueue_delay, wait_duration, encode_duration,
        );

        // Sign and respond (Lines 16-17)
        let sign_send_start = Instant::now();
        let message = Self::build_sequence_signature_message(&vc_tx, s);

        // should be digital signature
        let t0 = Instant::now();
        let sigma = self.signing_key.sign(&message).to_bytes().to_vec();
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
            .map_err(|e| format!("Failed to send SEQ-RESPONSE: {}", e))?;
        // info!(
        //     "[Sequencing] Sent *SEQ-RESPONSE* to {}, s={}, tx={}",
        //     sender,
        //     s,
        //     hex::encode(&req.tx.payload[..std::cmp::min(8, req.tx.payload.len())])
        // );
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
        let verified = self.verify_seq_response_sig(&resp, sender).await?;
        let verify_time = verify_start.elapsed();
        if verified {
            let vc_key = VC::from_slice(&resp.vc);
            if self.completed_responses.contains(&vc_key) {
                // debug!(
                //     "🧾 [Sequencing] node={} already satisfied response threshold for vc={}, ignoring duplicate",
                //     self.process_id,
                //     hex::encode(&resp.vc[..std::cmp::min(8, resp.vc.len())])
                // );
                return Ok(());
            }
            let threshold = 2 * self.f + 1;
            let mut maybe_records: Option<Vec<SeqResponseRecord>> = None;
            let collected;

            let map_update_start = Instant::now();
            match self.response_shares.entry(vc_key) {
                Entry::Occupied(mut occ) => {
                    let map = occ.get_mut();
                    if let Some(cached) = map.get(&sender) {
                        if cached.sequence == resp.s && cached.signature == resp.sigma {
                            collected = map.len();
                        } else {
                            warn!(
                                "⚠️ [Sequencing] cached response mismatch for vc {:?} sender {}",
                                vc_key, sender
                            );
                            return Ok(());
                        }
                    } else if map.len() >= threshold {
                        let taken_map = occ.remove();
                        maybe_records = Some(taken_map.into_values().collect());
                        collected = threshold;
                    } else {
                        map.insert(
                            sender,
                            SeqResponseRecord {
                                sender,
                                sequence: resp.s,
                                signature: resp.sigma.clone(),
                            },
                        );
                        let len = map.len();
                        if len >= threshold {
                            let taken_map = occ.remove();
                            maybe_records = Some(taken_map.into_values().collect());
                            collected = threshold;
                        } else {
                            collected = len;
                        }
                    }
                }
                Entry::Vacant(vac) => {
                    let mut map = HashMap::with_capacity(threshold);
                    map.insert(
                        sender,
                        SeqResponseRecord {
                            sender,
                            sequence: resp.s,
                            signature: resp.sigma.clone(),
                        },
                    );
                    let len = map.len();
                    if len >= threshold {
                        maybe_records = Some(map.into_values().collect());
                        collected = threshold;
                    } else {
                        vac.insert(map);
                        collected = len;
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
            // debug!(
            //     "🧾 [Sequencing] node={} collected {} / {} responses for vc={}",
            //     self.process_id,
            //     collected,
            //     2 * self.f + 1,
            //     hex::encode(&resp.vc[..std::cmp::min(8, resp.vc.len())])
            // );

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
                    return Err(format!("Failed to broadcast SEQ-ORDER: {}", e));
                }
                // tokio::task::yield_now().await;
                let broadcast_time = broadcast_start.elapsed();
                // info!(
                //     "📤 [Sequencing] node={} broadcasting *SEQ-ORDER* for vc={}, s={}",
                //     self.process_id,
                //     hex::encode(&resp.vc[..std::cmp::min(8, resp.vc.len())]),
                //     resp.s
                // );

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
    pub async fn finalize_seq_order(&self, sender: usize, order: SeqOrder) -> Result<(), String> {
        info!(
            "📥 [Sequencing] Node {} finalizing verified SEQ-ORDER",
            sender
        );

        let total_start = Instant::now();

        let sequences: Vec<u64> = order.records.iter().map(|r| r.sequence).collect();
        let median_start = Instant::now();
        let median = self.calculate_median(&sequences);
        let median_time = median_start.elapsed();

        let message = Self::build_median_signature_message(&order.vc, median);
        let (sigma_seq, sign_exec_time, sign_time) = if *DISABLE_THRESHOLD_SIG_VERIFICATION {
            (Vec::new(), Duration::ZERO, Duration::ZERO)
        } else {
            let sign_send_start = Instant::now();
            let threshold_share = self.threshold_share.clone();
            let message_for_sign = message.clone();
            let (sigma_seq, sign_exec_time) = run_threshold_task("threshold_sign", move || {
                let sign_start = Instant::now();
                let sigma = threshold_share.sign(&message_for_sign).to_bytes().to_vec();
                Ok((sigma, sign_start.elapsed()))
            })
            .await?;

            if sign_exec_time > Duration::from_millis(5) {
                warn!(
                    "[threshold_worker] Threshold sign took {:?}",
                    sign_exec_time
                );
            }

            let sign_time = sign_send_start.elapsed();
            let scheduling_overhead = sign_time.saturating_sub(sign_exec_time);
            if scheduling_overhead > Duration::from_millis(1) {
                warn!(
                    "[threshold_worker] threshold_sign scheduling overhead {:?}",
                    scheduling_overhead
                );
            }

            (sigma_seq, sign_exec_time, sign_time)
        };

        let median_msg = SmrolMessage::SeqMedian {
            vc: order.vc.clone(),
            median_sequence: median,
            proof: sigma_seq,
            sender_id: self.process_id,
        };
        let send_to_start = Instant::now();
        if let Err(e) = self.network.send_to_node(sender, median_msg).await {
            return Err(format!("Failed to send SEQ-MEDIAN: {}", e));
        }
        let send_time = send_to_start.elapsed();

        let total_time = total_start.elapsed();
        if total_time > Duration::from_millis(10) {
            warn!(
                "🐌 finalize_seq_order SLOW: total={:?}, median_calc={:?}, threshold_share.sign={:?}, send_to_node={:?}",
                total_time,
                median_time,
                sign_time,
                send_time
            );
        }

        Ok(())
    }

    // Handle SEQ-MEDIAN message - Lines 29-35
    #[tracing::instrument(skip(self))]
    pub async fn handle_seq_median(&self, sender: usize, median: SeqMedian) -> Result<(), String> {
        info!("📥 [Sequencing] received SEQ-MEDIAN from {}", sender);
        let total_start = Instant::now();
        // point to point so skip the check
        // if self.originated_vcs.contains(&median.vc) {
        // Original SEQ-REQUEST sender gathers median shares (Algorithm 2, line 30)
        if *DISABLE_THRESHOLD_SIG_VERIFICATION {
            let vc_key = VC::from_slice(&median.vc);
            let threshold = self.f + 1;
            let map_update_start = Instant::now();
            let mut ready_to_broadcast = false;
            let entry_len = match self.median_waiters.entry(vc_key) {
                Entry::Occupied(mut occ) => {
                    let set: &mut HashSet<usize> = occ.get_mut();
                    if set.insert(sender) && set.len() >= threshold {
                        ready_to_broadcast = true;
                        occ.remove();
                        threshold
                    } else {
                        set.len()
                    }
                }
                Entry::Vacant(vac) => {
                    if threshold == 1 {
                        ready_to_broadcast = true;
                        1
                    } else {
                        let mut set: HashSet<usize> = HashSet::with_capacity(threshold);
                        set.insert(sender);
                        vac.insert(set);
                        1
                    }
                }
            };
            let map_update_time = map_update_start.elapsed();

            if ready_to_broadcast && self.final_broadcasted.insert(vc_key) {
                let combine_start = Instant::now();
                let tx_id = self.resolve_tx_id(&vc_key);
                let combine_time = combine_start.elapsed();

                let final_msg = SmrolMessage::SeqFinal {
                    vc: median.vc.clone(),
                    final_sequence: median.s_tx,
                    combined_signature: Vec::new(),
                    sender_id: self.process_id,
                    tx_id,
                };
                if let Err(e) = self.broadcast_tx.send(final_msg) {
                    return Err(format!("Failed to broadcast SEQ-FINAL: {}", e));
                }

                let total_time = total_start.elapsed();
                if total_time > Duration::from_millis(10) {
                    warn!(
                        "🐌 handle_seq_median (no-threshold) SLOW: total={:?}, map_update={:?}, combine={:?}",
                        total_time,
                        map_update_time,
                        combine_time
                    );
                }
            } else if total_start.elapsed() > Duration::from_millis(10) {
                warn!(
                    "🐌 handle_seq_median (no-threshold) pending: total={:?}, map_update={:?}, collected={}/{}",
                    total_start.elapsed(),
                    map_update_time,
                    entry_len,
                    threshold
                );
            }
            return Ok(());
        }
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

            // debug!(
            //     "🔑 [Sequencing] node={} stored median share {}/{} for vc={} s_tx={} from {}",
            //     self.process_id,
            //     entry_len,
            //     self.f + 1,
            //     hex::encode(&median.vc[..std::cmp::min(8, median.vc.len())]),
            //     median.s_tx,
            //     sender
            // );

            if let Some(shares) = ready_to_broadcast {
                let combine_start = Instant::now();
                let collected = shares.len();
                let combined_sig = self.combine_median_shares_async(shares).await?;
                let combine_time = combine_start.elapsed();

                // debug!(
                //     "🔐 [Sequencing] node={} collected {} median shares for vc={} (s_tx={})",
                //     self.process_id,
                //     collected,
                //     hex::encode(&median.vc[..std::cmp::min(8, median.vc.len())]),
                //     median.s_tx
                // );

                let sigma_bytes = combined_sig.to_bytes().to_vec();
                let tx_id = self.resolve_tx_id(&vc_key);
                let final_msg = SmrolMessage::SeqFinal {
                    vc: median.vc.clone(),
                    final_sequence: median.s_tx,
                    combined_signature: sigma_bytes,
                    sender_id: self.process_id,
                    tx_id,
                };
                let broadcast_start = Instant::now();
                if let Err(e) = self.broadcast_tx.send(final_msg) {
                    return Err(format!("Failed to broadcast SEQ-FINAL: {}", e));
                }
                // tokio::task::yield_now().await;
                let broadcast_time = broadcast_start.elapsed();
                // info!(
                //     "[Sequencing] Node {} broadcast *SEQ-FINAL* {} for vc = {:?}",
                //     self.process_id,
                //     sender,
                //     hex::encode(&median.vc[..std::cmp::min(8, median.vc.len())])
                // );

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

        // NOTE: should this use atomics?
        let verify_start = Instant::now();
        // slow
        let result = self.verify_combined_signature_async(&final_msg).await?;
        let verify_duration = verify_start.elapsed();
        if result {
            if self.finalized_vcs.contains(&vc_key) {
                debug!(
                    "[Sequencing] Node {} ignoring duplicate SEQ-FINAL for vc {}",
                    self.process_id,
                    hex::encode(&final_msg.vc[..std::cmp::min(8, final_msg.vc.len())])
                );
                return Ok(None);
            }

            let check_start = Instant::now();
            let (in_vc_ledger, in_mi) = {
                let finalization = self.finalization.read().await;
                (
                    finalization.is_in_vc_ledger(&final_msg.vc),
                    finalization.is_in_mi(&final_msg.vc),
                )
            };
            let check_time = check_start.elapsed();
            // if check_time > Duration::from_micros(1000) {
            //     warn!(
            //         "[finalization] Read check took {:?} (vc={})",
            //         check_time,
            //         hex::encode(&final_msg.vc[..std::cmp::min(8, final_msg.vc.len())])
            //     );
            // }

            if in_vc_ledger || in_mi {
                // debug!(
                //     "ℹ️ [Sequencing] SEQ-FINAL vc={} already finalized, ignoring",
                //     hex::encode(&final_msg.vc[..std::cmp::min(8, final_msg.vc.len())])
                // );
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

    async fn verify_seq_response_sig(
        &self,
        resp: &SeqResponse,
        sender: usize,
    ) -> Result<bool, String> {
        let Some(verifying_key) = self.verifying_keys.get(&sender).cloned() else {
            return Err(format!("missing verifying key for node {}", sender));
        };

        if resp.sigma.len() != 64 {
            return Ok(false);
        }

        let signature = match Ed25519Signature::try_from(resp.sigma.as_slice()) {
            Ok(sig) => sig,
            Err(e) => {
                warn!(
                    "❌ [Sequencing] Invalid signature encoding in SeqResponse from {}: {}",
                    sender, e
                );
                return Ok(false);
            }
        };

        let message = TransactionSequencing::build_sequence_signature_message(&resp.vc, resp.s);

        Ok(verifying_key.verify_strict(&message, &signature).is_ok())
    }

    async fn verify_median_share_async(
        &self,
        median: &SeqMedian,
        sender: usize,
    ) -> Result<Option<SignatureShare>, String> {
        if *DISABLE_THRESHOLD_SIG_VERIFICATION {
            if median.sigma_seq.len() != SIG_SIZE {
                return Err(format!(
                    "threshold signature share length invalid: {}",
                    median.sigma_seq.len()
                ));
            }

            let mut share_bytes = [0u8; SIG_SIZE];
            share_bytes.copy_from_slice(&median.sigma_seq);
            let share = SignatureShare::from_bytes(share_bytes)
                .map_err(|e| format!("Failed to parse threshold share: {}", e))?;
            return Ok(Some(share));
        }

        if median.sigma_seq.len() != SIG_SIZE {
            return Err(format!(
                "threshold signature share length invalid: {}",
                median.sigma_seq.len()
            ));
        }

        let mut share_bytes = [0u8; SIG_SIZE];
        share_bytes.copy_from_slice(&median.sigma_seq);
        let share = SignatureShare::from_bytes(share_bytes)
            .map_err(|e| format!("Failed to parse threshold share: {}", e))?;

        let threshold_public = self.threshold_public.clone();
        let message_bytes = Self::build_median_signature_message(&median.vc, median.s_tx);
        let share_for_task = share.clone();

        let is_valid = run_threshold_task("verify_median_share", move || {
            let pk_share = threshold_public.public_key_share(sender);
            Ok(pk_share.verify(&share_for_task, &message_bytes))
        })
        .await?;

        Ok(if is_valid { Some(share) } else { None })
    }

    async fn combine_median_shares_async(
        &self,
        shares: Vec<(usize, SignatureShare)>,
    ) -> Result<ThresholdSignature, String> {
        if *DISABLE_THRESHOLD_SIG_VERIFICATION {
            return Err(
                "combine_median_shares_async called while threshold verification disabled".into(),
            );
        }

        let threshold_public = self.threshold_public.clone();
        let spawn_start = Instant::now();

        let (signature, worker_time) = run_threshold_task("combine_median_shares", move || {
            let work_start = Instant::now();
            let outcome = threshold_public
                .combine_signatures(shares.iter().map(|(id, share)| (*id, share)))
                .map_err(|e| format!("Threshold signature combine failed: {}", e));
            let elapsed = work_start.elapsed();
            outcome.map(|sig| (sig, elapsed))
        })
        .await?;

        let total_time = spawn_start.elapsed();
        let scheduling_overhead = total_time.saturating_sub(worker_time);
        if scheduling_overhead > Duration::from_millis(1) {
            warn!(
                "[threshold_worker] combine_median_shares_async scheduling overhead {:?}",
                scheduling_overhead
            );
        }

        Ok(signature)
    }

    async fn verify_combined_signature_async(&self, final_msg: &SeqFinal) -> Result<bool, String> {
        if *DISABLE_THRESHOLD_SIG_VERIFICATION {
            return Ok(true);
        }

        if final_msg.sigma.len() != SIG_SIZE {
            return Err(format!(
                "combined signature length invalid: {}",
                final_msg.sigma.len()
            ));
        }

        let mut sig_bytes = [0u8; SIG_SIZE];
        sig_bytes.copy_from_slice(&final_msg.sigma);
        let signature = ThresholdSignature::from_bytes(sig_bytes)
            .map_err(|e| format!("Failed to parse combined signature: {}", e))?;

        let threshold_public = self.threshold_public.clone();
        let message_bytes = Self::build_final_signature_message(&final_msg.vc, final_msg.s_tx);

        run_threshold_task("verify_combined_signature", move || {
            Ok(threshold_public
                .public_key()
                .verify(&signature, &message_bytes))
        })
        .await
    }

    async fn reconstruct_full_async(package: ErasurePackage) -> Result<Vec<u8>, String> {
        run_threshold_task("erasure_reconstruct", move || package.reconstruct_full()).await
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
            .unwrap_or(1000)
    }

    fn cleanup_log_prefix() -> &'static str {
        "🧹 [Sequencing] cleanup"
    }

    fn resolve_tx_id(&self, vc_key: &VC) -> u64 {
        self.pending_txs
            .get(vc_key)
            .and_then(|entry| {
                bincode::deserialize::<SmrolTransaction>(&entry.payload)
                    .map(|tx| tx.id)
                    .ok()
            })
            .unwrap_or(0)
    }

    fn retain_latest_seq_payloads(&self, limit: usize) {
        if self.seq_payloads.len() <= limit {
            return;
        }

        let mut keys: Vec<u64> = self.seq_payloads.iter().map(|entry| *entry.key()).collect();
        keys.sort_unstable();

        let remove_count = keys.len().saturating_sub(limit);
        for key in keys.into_iter().take(remove_count) {
            self.seq_payloads.remove(&key);
        }

        debug!(
            "{} trimmed seq_payloads down to {} entries",
            Self::cleanup_log_prefix(),
            limit
        );
    }

    fn retain_latest_pending_txs<T>(&self, map: &DashMap<VC, T>, limit: usize) {
        if map.len() <= limit {
            return;
        }

        let mut entries: Vec<(u64, VC)> = map
            .iter()
            .filter_map(|entry| {
                let vc = *entry.key();
                self.tx_sequence_map
                    .get(&vc)
                    .map(|seq_entry| (*seq_entry.value(), vc))
            })
            .collect();

        if entries.is_empty() {
            return;
        }

        entries.sort_by_key(|(seq, _)| *seq);
        let remove_count = entries.len().saturating_sub(limit);
        for (_, vc) in entries.into_iter().take(remove_count) {
            map.remove(&vc);
        }

        debug!(
            "{} trimmed pending_txs down to {} entries",
            Self::cleanup_log_prefix(),
            limit
        );
    }

    fn retain_latest_finals(map: &DashMap<VC, Vec<SeqFinal>>, limit: usize) {
        if map.len() <= limit {
            return;
        }

        let mut entries: Vec<(u64, VC)> = map
            .iter()
            .filter_map(|entry| {
                let vc = *entry.key();
                entry
                    .value()
                    .iter()
                    .map(|final_msg| (final_msg.s_tx, vc))
                    .max_by_key(|(s_tx, _)| *s_tx)
            })
            .collect();

        if entries.is_empty() {
            return;
        }

        entries.sort_by_key(|(seq, _)| *seq);

        let keep = limit.min(entries.len());
        for (_, vc) in entries.into_iter().rev().skip(keep) {
            map.remove(&vc);
        }

        debug!(
            "{} trimmed pending_seq_finals to last {} entries",
            Self::cleanup_log_prefix(),
            keep
        );
    }

    pub async fn cleanup_expired_state(&self) {
        let limit = Self::state_limit();

        self.retain_latest_pending_txs(&self.pending_txs, limit);

        self.retain_latest_seq_payloads(limit);

        if self.erasure_store.len() > limit {
            let removed = self.erasure_store.len();
            self.erasure_store.clear();
            debug!(
                "{} cleared {} erasure_store entries",
                Self::cleanup_log_prefix(),
                removed
            );
        }

        Self::retain_latest_finals(&self.pending_seq_finals, limit);

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

        if self.median_waiters.len() > limit {
            let removed = self.median_waiters.len();
            self.median_waiters.clear();
            debug!(
                "{} cleared {} median waiter entries",
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
        // info!(
        //     "[sequencing] node={} cached SEQ-FINAL awaiting payload: vc={} s_tx={}",
        //     self.process_id,
        //     hex::encode(&final_msg.vc[..std::cmp::min(8, final_msg.vc.len())]),
        //     final_msg.s_tx
        // );
        let vc_key = VC::from_slice(&final_msg.vc);
        self.pending_seq_finals
            .entry(vc_key)
            .or_insert_with(Vec::new)
            .push(final_msg);
    }

    async fn finalize_ready_final(&self, final_msg: SeqFinal) -> Option<TransactionEntry> {
        let finalize_total_start = Instant::now();
        let vc_key = VC::from_slice(&final_msg.vc);

        let mut payload = if let Some((_, tx)) = self.pending_txs.remove(&vc_key) {
            tx.payload
        } else if let Some((_, bytes)) = self.seq_payloads.remove(&final_msg.s_tx) {
            bytes
        } else if let Some(pkg_entry) = self.erasure_store.get(&vc_key) {
            warn!(
                "Try Reconstructing... s_tx={}, vc={:?}",
                final_msg.s_tx,
                hex::encode(&vc_key.0[..std::cmp::min(8, vc_key.0.len())])
            );
            let package_copy = pkg_entry.clone();
            drop(pkg_entry);
            match Self::reconstruct_full_async(package_copy).await {
                Ok(bytes) => {
                    self.erasure_store.remove(&vc_key);
                    bytes
                }
                Err(e) => {
                    warn!(
                        "❌ [Sequencing] Erasure reconstruction failed for vc {:?}: {}",
                        vc_key, e
                    );
                    return None;
                }
            }
        } else {
            // Still waiting for request context; cache for later retry
            self.store_pending_final(final_msg).await;
            return None;
        };

        self.seq_payloads.remove(&final_msg.s_tx);
        self.erasure_store.remove(&vc_key);

        self.pending_seq_finals.remove(&vc_key);
        self.originated_vcs.remove(&vc_key);
        self.completed_responses.remove(&vc_key);
        self.final_broadcasted.remove(&vc_key);
        self.finalized_vcs.insert(vc_key);

        let entry = TransactionEntry {
            vc_tx: final_msg.vc,
            s_tx: final_msg.s_tx,
            sigma: final_msg.sigma,
            payload,
        };

        // debug!(
        //     "✅ [Sequencing] Finalized VC forwarded to consensus: vc_len={}, s_tx={}",
        //     entry.vc_tx.len(),
        //     entry.s_tx
        // );
        // debug!(
        //     "🎯 [Sequencing] node={} finalizing vc={} s_tx={}",
        //     self.process_id,
        //     hex::encode(&entry.vc_tx[..std::cmp::min(8, entry.vc_tx.len())]),
        //     entry.s_tx
        // );

        let total = finalize_total_start.elapsed();
        // if total > Duration::from_micros(1000) {
        //     warn!(
        //         "[finalization] finalize_ready_final total {:?} (vc={})",
        //         total,
        //         hex::encode(&entry.vc_tx[..std::cmp::min(8, entry.vc_tx.len())])
        //     );
        // }

        Some(entry)
    }
}

async fn verify_seq_order_records(
    order: Arc<SeqOrder>,
    verifying_keys: Arc<HashMap<usize, VerifyingKey>>,
    required: usize,
    cached_responses: Arc<DashMap<VC, HashMap<usize, SeqResponseRecord>>>,
) -> Result<bool, String> {
    if order.records.len() != required {
        return Ok(false);
    }

    let vc_key = VC::from_slice(&order.vc);
    let mut verify_inputs = Vec::with_capacity(order.records.len());
    {
        let cache_entry = cached_responses.get(&vc_key);

        for record in &order.records {
            let mut need_verify = true;
            if let Some(cache) = cache_entry.as_ref() {
                if let Some(cached) = cache.get(&record.sender) {
                    if cached.sequence == record.sequence && cached.signature == record.signature {
                        need_verify = false;
                    } else {
                        warn!(
                            "⚠️ [Sequencing] cached order record mismatch for vc {:?} sender {}",
                            vc_key, record.sender
                        );
                        return Ok(false);
                    }
                }
            }

            if need_verify {
                let verifying_key = verifying_keys
                    .get(&record.sender)
                    .ok_or_else(|| format!("missing verifying key for node {}", record.sender))?
                    .clone();

                if record.signature.len() != 64 {
                    return Ok(false);
                }

                let signature =
                    Ed25519Signature::try_from(record.signature.as_slice()).map_err(|_| {
                        format!("invalid signature bytes in SeqOrder from {}", record.sender)
                    })?;
                let message = TransactionSequencing::build_sequence_signature_message(
                    &order.vc,
                    record.sequence,
                );

                verify_inputs.push((record.sender, verifying_key, message, signature));
            }
        }
    }

    if verify_inputs.is_empty() {
        return Ok(true);
    }

    let verify_ok = run_threshold_task("verify_seq_order_batch", move || {
        for (sender, verifying_key, message, signature) in verify_inputs {
            if verifying_key.verify_strict(&message, &signature).is_err() {
                warn!(
                    "⚠️ [Sequencing] verify_seq_order_records signature invalid from node {}",
                    sender
                );
                return Ok(false);
            }
        }
        Ok(true)
    })
    .await?;

    Ok(verify_ok)
}
