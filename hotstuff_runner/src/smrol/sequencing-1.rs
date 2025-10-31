use crate::affinity::{affinity_from_env, bind_current_thread};
use crate::smrol::{
    consensus::TransactionEntry,
    crypto::{ErasurePackage, MultiSigContext},
    finalization::OutputFinalization,
    message::{SmrolMessage, SmrolTransaction},
    network::SmrolTcpNetwork,
    pnfifo::PnfifoBc,
    ModuleMessage,
};
use blst::min_sig::{
    AggregatePublicKey as BlstAggregatePublicKey, AggregateSignature as BlstAggregateSignature,
    PublicKey as BlstPublicKey, Signature as BlstSignature,
};
use blst::BLST_ERROR;
use core_affinity::CoreId;
use crossbeam::channel::{unbounded, Sender};
use dashmap::{mapref::entry::Entry, DashMap, DashSet};
use ed25519_dalek::{Signature as Ed25519Signature, Signer, SigningKey, Verifier, VerifyingKey};
use futures::task::AtomicWaker;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    mpsc as std_mpsc, Arc, Mutex,
};
use std::task::{Context, Poll};
use std::thread;
use std::time::Instant;
use std::{
    collections::{HashMap, HashSet},
    convert::TryFrom,
    pin::Pin,
};
use tokio::{
    sync::{mpsc as async_mpsc, Mutex as AsyncMutex, RwLock, Semaphore},
    time::{timeout, Duration},
};
use tracing::{debug, error, info, warn};

const BATCH_VERIFIER_WINDOW: Duration = Duration::from_micros(200);
const BATCH_VERIFIER_SIZE: usize = 16;

static DISABLE_MULTISIG_VERIFICATION: Lazy<bool> =
    Lazy::new(|| match std::env::var("SMROL_DISABLE_MULTISIG") {
        Ok(val) => matches!(
            val.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => false,
    });

// Thread pool infrastructure (unchanged)
trait ThresholdJob: Send {
    fn run(self: Box<Self>);
}

struct ThresholdThreadPool {
    sender: Sender<Box<dyn ThresholdJob>>,
    pending_jobs: Arc<AtomicUsize>,
    metrics_pool: &'static str,
}

impl ThresholdThreadPool {
    fn new(worker_count: usize, metrics_pool: &'static str, affinity: Option<Vec<CoreId>>) -> Self {
        let (sender, receiver) = unbounded::<Box<dyn ThresholdJob>>();
        let pending_jobs = Arc::new(AtomicUsize::new(0));
        metrics::gauge!("smrol.threshold_pending_jobs", "pool" => metrics_pool).set(0.0);
        let affinity = affinity.map(Arc::new);
        for idx in 0..worker_count {
            let thread_receiver = receiver.clone();
            let worker_name = format!("{}-worker-{}", metrics_pool, idx);
            let core_assignment = affinity
                .as_ref()
                .and_then(|cores| cores.get(idx % cores.len()))
                .copied();
            thread::Builder::new()
                .name(worker_name.clone())
                .spawn(move || {
                    if let Some(core_id) = core_assignment {
                        bind_current_thread(&worker_name, core_id);
                    }
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
            metrics_pool,
        }
    }

    fn submit<R, F>(&self, label: &'static str, task: F) -> Result<ThresholdTaskFuture<R>, String>
    where
        R: Send + 'static,
        F: FnOnce() -> Result<R, String> + Send + 'static,
    {
        let state = Arc::new(TaskState::new());
        let pending_after = self.pending_jobs.fetch_add(1, Ordering::Relaxed) + 1;
        metrics::gauge!("smrol.threshold_pending_jobs", "pool" => self.metrics_pool)
            .set(pending_after as f64);
        let job = Box::new(ConcreteThresholdJob {
            task: Some(task),
            state: state.clone(),
            label,
            submitted_at: Instant::now(),
            pending: Arc::clone(&self.pending_jobs),
            pool_label: self.metrics_pool,
        });
        if self.sender.send(job).is_err() {
            let prev = self.pending_jobs.fetch_sub(1, Ordering::Relaxed);
            let remaining = prev.saturating_sub(1);
            metrics::gauge!("smrol.threshold_pending_jobs", "pool" => self.metrics_pool)
                .set(remaining as f64);
            return Err("threshold worker pool shut down".to_string());
        }
        Ok(ThresholdTaskFuture { state })
    }
}

fn parse_worker_count(var: &str, default: usize) -> usize {
    std::env::var(var)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|&count| count > 0)
        .unwrap_or(default)
}

fn default_worker_count() -> usize {
    thread::available_parallelism()
        .map(|n| n.get().max(4))
        .unwrap_or(4)
}

fn read_inflight(var: &str, default: usize) -> usize {
    std::env::var(var)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|&count| count > 0)
        .unwrap_or(default)
}

static MULTISIG_SIGN_COMBINE_POOL: Lazy<ThresholdThreadPool> = Lazy::new(|| {
    let default_workers = (default_worker_count() * 2).min(32);
    let requested_workers = parse_worker_count("SMROL_MULTISIG_CRYPTO_WORKERS", default_workers);
    let affinity = affinity_from_env("SMROL_MULTISIG_COMBINE_CORES", "multisig_sign_combine");
    let workers = match affinity.as_ref() {
        Some(cores) => {
            if cores.len() != requested_workers {
                warn!(
                    "[affinity] multisig_sign_combine: overriding worker count {} -> {}",
                    requested_workers,
                    cores.len()
                );
            }
            cores.len()
        }
        None => requested_workers,
    };
    ThresholdThreadPool::new(workers, "multisig_sign_combine", affinity)
});

static MULTISIG_VERIFY_POOL: Lazy<ThresholdThreadPool> = Lazy::new(|| {
    let default_workers = default_worker_count().min(32);
    let requested_workers = parse_worker_count("SMROL_MULTISIG_VERIFY_WORKERS", default_workers);
    let affinity = affinity_from_env("SMROL_MULTISIG_VERIFY_CORES", "multisig_verify");
    let workers = match affinity.as_ref() {
        Some(cores) => {
            if cores.len() != requested_workers {
                info!(
                    "[affinity] multisig_verify: overriding worker count {} -> {}",
                    requested_workers,
                    cores.len()
                );
            }
            cores.len()
        }
        None => requested_workers,
    };
    ThresholdThreadPool::new(workers, "multisig_verify", affinity)
});

static SEQ_OFFLOAD_POOL: Lazy<ThresholdThreadPool> = Lazy::new(|| {
    let default_workers = 4;
    let workers = parse_worker_count("SMROL_SEQ_OFFLOAD_WORKERS", default_workers);
    ThresholdThreadPool::new(workers, "seq_offload", None)
});

const PNFIFO_BROADCAST_CONCURRENCY: usize = 8;
const ORDER_VERIFY_CONCURRENCY: usize = 8;
const REQUEST_WORKER_COUNT: usize = 8;
const MEDIAN_WORKER_COUNT: usize = 8;
const MEDIAN_COMBINE_WORKER_COUNT: usize = 8;
const FINAL_WORKER_COUNT: usize = 8;
const MSG_TAG_SEQUENCE: u8 = 0x01;
const MSG_TAG_MEDIAN: u8 = 0x02;
const MSG_TAG_FINAL: u8 = 0x03;

async fn run_multisig_sign_task<R, F>(label: &'static str, task: F) -> Result<R, String>
where
    R: Send + 'static,
    F: FnOnce() -> Result<R, String> + Send + 'static,
{
    MULTISIG_SIGN_COMBINE_POOL.submit(label, task)?.await
}

async fn run_multisig_verify_task<R, F>(label: &'static str, task: F) -> Result<R, String>
where
    R: Send + 'static,
    F: FnOnce() -> Result<R, String> + Send + 'static,
{
    MULTISIG_VERIFY_POOL.submit(label, task)?.await
}

async fn run_seq_offload_task<R, F>(label: &'static str, task: F) -> Result<R, String>
where
    R: Send + 'static,
    F: FnOnce() -> Result<R, String> + Send + 'static,
{
    SEQ_OFFLOAD_POOL.submit(label, task)?.await
}

fn set_inflight(stage: &'static str, value: usize) {
    metrics::gauge!("smrol.inflight", "stage" => stage).set(value as f64);
}

fn spawn_detached<F>(stage: &'static str, semaphore: Arc<Semaphore>, capacity: usize, fut: F)
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        let semaphore_metrics = Arc::clone(&semaphore);
        let permit = match semaphore.acquire_owned().await {
            Ok(p) => p,
            Err(_) => return,
        };
        let inflight = capacity.saturating_sub(semaphore_metrics.available_permits());
        set_inflight(stage, inflight);
        fut.await;
        drop(permit);
        let inflight = capacity.saturating_sub(semaphore_metrics.available_permits());
        set_inflight(stage, inflight);
    });
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

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> std::task::Poll<Self::Output> {
        if let Some(result) = self.state.take_result() {
            return std::task::Poll::Ready(result);
        }
        self.state.register(cx.waker());
        if let Some(result) = self.state.take_result() {
            std::task::Poll::Ready(result)
        } else {
            std::task::Poll::Pending
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
    pool_label: &'static str,
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
            metrics::histogram!("smrol.threshold_task_wait_ms", "task" => self.label, "pool" => self.pool_label)
                .record(wait.as_secs_f64() * 1000.0);
            let result = task();
            let exec = start.elapsed();
            metrics::histogram!("smrol.threshold_task_exec_ms", "task" => self.label, "pool" => self.pool_label)
                .record(exec.as_secs_f64() * 1000.0);
            let prev_pending = self.pending.fetch_sub(1, Ordering::Relaxed);
            let remaining = prev_pending.saturating_sub(1);
            metrics::gauge!("smrol.threshold_pending_jobs", "pool" => self.pool_label)
                .set(remaining as f64);
            self.state.complete(result);
        } else {
            let prev_pending = self.pending.fetch_sub(1, Ordering::Relaxed);
            let remaining = prev_pending.saturating_sub(1);
            metrics::gauge!("smrol.threshold_pending_jobs", "pool" => self.pool_label)
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
    pub sigma: Vec<u8>,
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
    pub sigma_seq: Vec<u8>, // BLS signature bytes
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeqFinal {
    pub vc: Vec<u8>,
    pub s_tx: u64,
    pub sigma: Vec<u8>, // Aggregated BLS signature bytes
    pub signers: Vec<usize>,
    pub tx_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeqResponseRecord {
    pub sender: usize,
    pub sequence: u64,
    pub signature: Vec<u8>,
}

#[derive(Debug)]
struct MedianCombineTask {
    vc: Vec<u8>,
    s_tx: u64,
    sigs: Vec<(usize, BlstSignature)>, // Changed from SignatureShare
}

#[derive(Debug)]
struct FinalVerifyTask {
    final_msg: SeqFinal,
}

#[derive(Debug)]
struct FinalSignTask {
    sender: usize,
    vc: Vec<u8>,
    median_sequence: u64,
    message: Vec<u8>,
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
    pub process_id: usize,
    pub network: Arc<SmrolTcpNetwork>,
    pub pnfifo: Arc<PnfifoBc>,
    pnfifo_broadcast_semaphore: Arc<Semaphore>,
    order_verify_semaphore: Arc<Semaphore>,
    median_semaphore: Arc<Semaphore>,
    median_concurrency: usize,
    order_finalize_semaphore: Arc<Semaphore>,
    order_finalize_concurrency: usize,
    final_sign_semaphore: Arc<Semaphore>,
    final_sign_concurrency: usize,
    final_verify_semaphore: Arc<Semaphore>,
    final_verify_concurrency: usize,

    // CHANGED: Multi-sig context replaces threshold keys
    pub multisig_ctx: Arc<MultiSigContext>,

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

    // CHANGED: Store BLS signatures instead of threshold shares
    median_sigs: DashMap<VC, HashMap<usize, BlstSignature>>,
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
    median_combine_tx: async_mpsc::UnboundedSender<MedianCombineTask>,
    median_combine_rx: Arc<AsyncMutex<async_mpsc::UnboundedReceiver<MedianCombineTask>>>,
    final_tx: async_mpsc::UnboundedSender<ModuleMessage>,
    final_rx: Arc<AsyncMutex<async_mpsc::UnboundedReceiver<ModuleMessage>>>,
    final_sign_tx: async_mpsc::UnboundedSender<FinalSignTask>,
    final_sign_rx: Arc<AsyncMutex<async_mpsc::UnboundedReceiver<FinalSignTask>>>,
    final_verify_tx: async_mpsc::UnboundedSender<FinalVerifyTask>,
    final_verify_rx: Arc<AsyncMutex<async_mpsc::UnboundedReceiver<FinalVerifyTask>>>,
    request_backlog: Arc<AtomicUsize>,
    response_backlog: Arc<AtomicUsize>,
    median_backlog: Arc<AtomicUsize>,
    median_combine_backlog: Arc<AtomicUsize>,
    final_sign_backlog: Arc<AtomicUsize>,
    final_backlog: Arc<AtomicUsize>,
    final_verify_backlog: Arc<AtomicUsize>,
    sequenced_entry_tx: async_mpsc::UnboundedSender<TransactionEntry>,
    final_verify_inflight: DashSet<VC>,
}

impl TransactionSequencing {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        process_id: usize,
        n: usize,
        f: usize,
        pnfifo_threshold: usize,
        network: Arc<SmrolTcpNetwork>,
        pnfifo: Arc<PnfifoBc>,
        multisig_ctx: Arc<MultiSigContext>, // CHANGED: parameter
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
            warn!("[sequencing] node {} broadcast worker exited", node_id);
        });

        // Initialize channels (same as original)
        let (request_tx, request_rx_raw) = async_mpsc::unbounded_channel();
        let request_rx = Arc::new(AsyncMutex::new(request_rx_raw));
        let request_backlog = Arc::new(AtomicUsize::new(0));
        metrics::gauge!("smrol.channel_backlog", "channel" => "request").set(0.0);

        let (response_tx, response_rx_raw) = async_mpsc::unbounded_channel();
        let response_rx = Arc::new(AsyncMutex::new(response_rx_raw));
        let response_backlog = Arc::new(AtomicUsize::new(0));
        metrics::gauge!("smrol.channel_backlog", "channel" => "response").set(0.0);

        let (order_tx, order_rx_raw) = async_mpsc::unbounded_channel();
        let order_rx = Arc::new(AsyncMutex::new(order_rx_raw));
        let order_backlog = Arc::new(AtomicUsize::new(0));
        metrics::gauge!("smrol.channel_backlog", "channel" => "order").set(0.0);

        let (order_verify_tx, order_verify_rx_raw) = async_mpsc::unbounded_channel();
        let order_verify_rx = Arc::new(AsyncMutex::new(order_verify_rx_raw));
        let order_verify_backlog = Arc::new(AtomicUsize::new(0));
        metrics::gauge!("smrol.channel_backlog", "channel" => "order_verify").set(0.0);

        let (order_finalize_tx, order_finalize_rx_raw) = async_mpsc::unbounded_channel();
        let order_finalize_rx = Arc::new(AsyncMutex::new(order_finalize_rx_raw));
        let order_finalize_backlog = Arc::new(AtomicUsize::new(0));
        metrics::gauge!("smrol.channel_backlog", "channel" => "order_finalize").set(0.0);

        let (median_tx, median_rx_raw) = async_mpsc::unbounded_channel();
        let median_rx = Arc::new(AsyncMutex::new(median_rx_raw));
        let median_backlog = Arc::new(AtomicUsize::new(0));
        metrics::gauge!("smrol.channel_backlog", "channel" => "median").set(0.0);

        let (median_combine_tx, median_combine_rx_raw) = async_mpsc::unbounded_channel();
        let median_combine_rx = Arc::new(AsyncMutex::new(median_combine_rx_raw));
        let median_combine_backlog = Arc::new(AtomicUsize::new(0));
        metrics::gauge!("smrol.channel_backlog", "channel" => "median_combine").set(0.0);

        let (final_tx, final_rx_raw) = async_mpsc::unbounded_channel();
        let final_rx = Arc::new(AsyncMutex::new(final_rx_raw));
        let final_backlog = Arc::new(AtomicUsize::new(0));
        metrics::gauge!("smrol.channel_backlog", "channel" => "final").set(0.0);

        let (final_sign_tx, final_sign_rx_raw) = async_mpsc::unbounded_channel();
        let final_sign_rx = Arc::new(AsyncMutex::new(final_sign_rx_raw));
        let final_sign_backlog = Arc::new(AtomicUsize::new(0));
        metrics::gauge!("smrol.channel_backlog", "channel" => "final_sign").set(0.0);

        let (final_verify_tx, final_verify_rx_raw) = async_mpsc::unbounded_channel();
        let final_verify_rx = Arc::new(AsyncMutex::new(final_verify_rx_raw));
        let final_verify_backlog = Arc::new(AtomicUsize::new(0));
        metrics::gauge!("smrol.channel_backlog", "channel" => "final_verify").set(0.0);

        let pnfifo_broadcast_semaphore = Arc::new(Semaphore::new(PNFIFO_BROADCAST_CONCURRENCY));
        let order_verify_semaphore = Arc::new(Semaphore::new(ORDER_VERIFY_CONCURRENCY));
        let median_concurrency = read_inflight("SMROL_MEDIAN_INFLIGHT", 64).max(1);
        let median_semaphore = Arc::new(Semaphore::new(median_concurrency));
        let order_finalize_concurrency = read_inflight("SMROL_ORDER_FINALIZE_INFLIGHT", 64).max(1);
        let order_finalize_semaphore = Arc::new(Semaphore::new(order_finalize_concurrency));
        set_inflight("order_finalize", 0);
        let default_final_sign = (default_worker_count() * 4).max(64);
        let final_sign_concurrency =
            read_inflight("SMROL_FINAL_SIGN_INFLIGHT", default_final_sign).max(1);
        let final_sign_semaphore = Arc::new(Semaphore::new(final_sign_concurrency));
        set_inflight("final_sign", 0);
        let final_verify_concurrency = read_inflight("SMROL_FINAL_VERIFY_INFLIGHT", 64).max(1);
        let final_verify_semaphore = Arc::new(Semaphore::new(final_verify_concurrency));
        set_inflight("final_verify", 0);

        warn!("SMROL_MEDIAN_INFLIGHT={}, SMROL_ORDER_FINALIZE_INFLIGHT={}, SMROL_FINAL_SIGN_INFLIGHT={}, SMROL_FINAL_VERIFY_INFLIGHT={}", 
            median_concurrency, order_finalize_concurrency, final_sign_concurrency, final_verify_concurrency);

        Self {
            f,
            n,
            pnfifo_threshold,
            process_id,
            network,
            pnfifo,
            pnfifo_broadcast_semaphore,
            order_verify_semaphore,
            median_semaphore,
            median_concurrency,
            order_finalize_semaphore,
            order_finalize_concurrency,
            final_sign_semaphore,
            final_sign_concurrency,
            final_verify_semaphore,
            final_verify_concurrency,
            multisig_ctx, // CHANGED: store multi-sig context
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
            median_sigs: DashMap::new(), // CHANGED: store BLS signatures
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
            median_combine_tx,
            median_combine_rx,
            final_tx,
            final_rx,
            final_sign_tx,
            final_sign_rx,
            final_verify_tx,
            final_verify_rx,
            request_backlog,
            response_backlog,
            median_backlog,
            median_combine_backlog,
            final_sign_backlog,
            final_backlog,
            final_verify_backlog,
            sequenced_entry_tx,
            final_verify_inflight: DashSet::new(),
        }
    }

    // Message building helpers
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

    // CHANGED: Multi-sig signing (Line 27)
    async fn sign_median_async(&self, median: u64, vc: &[u8]) -> Result<BlstSignature, String> {
        if *DISABLE_MULTISIG_VERIFICATION {
            return Ok(unsafe { std::mem::zeroed() });
        }

        let msg = Self::build_median_signature_message(vc, median);
        let multisig_ctx = Arc::clone(&self.multisig_ctx);
        let node_id = self.process_id;

        run_multisig_sign_task("sign_median", move || {
            multisig_ctx
                .sign(node_id, &msg)
                .ok_or_else(|| "failed to sign median".to_string())
        })
        .await
    }

    // CHANGED: Verify individual median signature (Line 31)
    async fn verify_median_sig_async(
        &self,
        median: &SeqMedian,
        sender: usize,
    ) -> Result<Option<BlstSignature>, String> {
        if *DISABLE_MULTISIG_VERIFICATION {
            // In bypass mode, return dummy signature
            return Ok(Some(unsafe { std::mem::zeroed() }));
        }

        let sig = BlstSignature::from_bytes(&median.sigma_seq)
            .map_err(|e| format!("invalid signature bytes: {:?}", e))?;

        let msg = Self::build_median_signature_message(&median.vc, median.s_tx);
        let multisig_ctx = Arc::clone(&self.multisig_ctx);

        let is_valid = run_multisig_verify_task("verify_median_sig", move || {
            let pk = multisig_ctx
                .public_key(sender)
                .ok_or_else(|| format!("no public key for node {}", sender))?;

            let dst = b"BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_NUL_";
            match sig.verify(true, &msg, dst, &[], pk, true) {
                BLST_ERROR::BLST_SUCCESS => Ok(true),
                _ => Ok(false),
            }
        })
        .await?;

        Ok(if is_valid { Some(sig) } else { None })
    }

    // CHANGED: Aggregate f+1 signatures (Line 34)
    async fn aggregate_median_sigs_async(
        &self,
        sigs: Vec<(usize, BlstSignature)>,
    ) -> Result<BlstAggregateSignature, String> {
        if *DISABLE_MULTISIG_VERIFICATION {
            return Ok(unsafe { std::mem::zeroed() });
        }

        if sigs.len() < self.f + 1 {
            return Err(format!(
                "not enough signatures: {} < {}",
                sigs.len(),
                self.f + 1
            ));
        }

        let multisig_ctx = Arc::clone(&self.multisig_ctx);

        run_multisig_sign_task("aggregate_sigs", move || {
            let (agg_sig, _agg_pk) = multisig_ctx
                .aggregate(&sigs)
                .map_err(|e| format!("aggregation failed: {:?}", e))?;
            Ok(agg_sig)
        })
        .await
    }

    // CHANGED: Verify aggregated signature (Line 36)
    async fn verify_aggregated_sig_async(
        &self,
        final_msg: &SeqFinal,
        signers: &[usize],
    ) -> Result<bool, String> {
        if *DISABLE_MULTISIG_VERIFICATION {
            return Ok(true);
        }

        let agg_sig = BlstAggregateSignature::from_bytes(&final_msg.sigma)
            .map_err(|e| format!("invalid aggregate signature: {:?}", e))?;

        let msg = Self::build_median_signature_message(&final_msg.vc, final_msg.s_tx);
        let multisig_ctx = Arc::clone(&self.multisig_ctx);
        let signers = signers.to_vec();

        run_multisig_verify_task("verify_aggregated_sig", move || {
            multisig_ctx
                .verify(&msg, &agg_sig, &signers)
                .map_err(|e| format!("verification failed: {:?}", e))
        })
        .await
    }

    // Line 1-3: SMROL-broadcast (unchanged)
    pub async fn smrol_broadcast(&self, tx: SmrolTransaction) -> Result<(), String> {
        let s = self.broadcast_seq.fetch_add(1, Ordering::SeqCst);
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

        let originated_count = self.originated_vcs.len();
        info!(
            "[Sequencing] Node {} broadcast *SEQ-REQUEST* k={}, originated_vcs.len() = {}",
            self.process_id, s, originated_count
        );

        Ok(())
    }

    // Lines 4-17: handle_seq_request (unchanged, no threshold signatures here)
    pub async fn handle_seq_request(
        &self,
        sender: usize,
        req: SeqRequest,
        encoded_package: ErasurePackage,
        wait_duration: Duration,
        encode_duration: Duration,
    ) -> Result<Option<TransactionEntry>, String> {
        let total_start = Instant::now();
        let vc_root = encoded_package.merkle_root();
        let vc_tx = vc_root.to_vec();
        let vc_key = VC::from_slice(&vc_tx);

        let state_update_start = Instant::now();
        self.buf.insert(vc_key);

        let s = match self.tx_sequence_map.entry(vc_key) {
            Entry::Occupied(entry) => *entry.get(),
            Entry::Vacant(entry) => {
                let assigned_s = self.local_seq.fetch_add(1, Ordering::SeqCst);
                debug!(
                    "🧮 [Sequencing] node={} assigned new sequence {}",
                    self.process_id, assigned_s
                );
                entry.insert(assigned_s);
                assigned_s
            }
        };

        self.erasure_store.insert(vc_key, encoded_package.clone());
        self.pending_txs
            .entry(vc_key)
            .or_insert_with(|| req.tx.clone());

        if sender == self.process_id {
            self.originated_vcs.insert(vc_key);
        }

        let pending_finals = self
            .pending_seq_finals
            .remove(&vc_key)
            .map(|(_, finals)| finals);
        let state_update_time = state_update_start.elapsed();

        // Input to PNFIFO-BC (Line 15)
        let enqueue_start = Instant::now();
        let slot = s;
        let node_id = self.process_id;
        let pnfifo = Arc::clone(&self.pnfifo);
        let semaphore = Arc::clone(&self.pnfifo_broadcast_semaphore);
        let vc_for_broadcast = vc_tx.clone();
        tokio::spawn(async move {
            let permit = match semaphore.acquire_owned().await {
                Ok(permit) => permit,
                Err(_) => return,
            };
            if let Err(err) = pnfifo.broadcast(slot, vc_for_broadcast).await {
                warn!("❌ [Sequencing] PNFIFO broadcast failed: {}", err);
            }
            drop(permit);
        });
        let enqueue_delay = enqueue_start.elapsed();

        // Sign and respond (Lines 16-17)
        let sign_send_start = Instant::now();
        let message = Self::build_sequence_signature_message(&vc_tx, s);
        let sigma = self.signing_key.sign(&message).to_bytes().to_vec();
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
        let sign_send_time = sign_send_start.elapsed();

        // Check deferred FINAL messages
        let finalize_pending_start = Instant::now();
        let mut finalized_entry: Option<TransactionEntry> = None;
        if let Some(mut pending_finals) = pending_finals {
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
            warn!("🐌 handle_seq_request SLOW: total={:?}", total_time);
        }

        Ok(finalized_entry)
    }

    // Lines 18-23: handle_seq_response (unchanged, no threshold signatures)
    pub async fn handle_seq_response(
        &self,
        sender: usize,
        resp: SeqResponse,
    ) -> Result<(), String> {
        info!("📥 [Sequencing] received SEQ-RESPONSE from Node {}", sender);
        let total_start = Instant::now();
        let verify_start = Instant::now();
        let verified = self.verify_seq_response_sig(&resp, sender).await?;
        let verify_time = verify_start.elapsed();

        if verified {
            let vc_key = VC::from_slice(&resp.vc);
            if self.completed_responses.contains(&vc_key) {
                return Ok(());
            }
            let threshold = 2 * self.f + 1;
            let mut maybe_records: Option<Vec<SeqResponseRecord>> = None;

            match self.response_shares.entry(vc_key) {
                Entry::Occupied(mut occ) => {
                    let map = occ.get_mut();
                    if let Some(cached) = map.get(&sender) {
                        if cached.sequence == resp.s && cached.signature == resp.sigma {
                            // Already have this
                        } else {
                            warn!("⚠️ cached response mismatch");
                            return Ok(());
                        }
                    } else if map.len() >= threshold {
                        let taken_map = occ.remove();
                        maybe_records = Some(taken_map.into_values().collect());
                    } else {
                        map.insert(
                            sender,
                            SeqResponseRecord {
                                sender,
                                sequence: resp.s,
                                signature: resp.sigma.clone(),
                            },
                        );
                        if map.len() >= threshold {
                            let taken_map = occ.remove();
                            maybe_records = Some(taken_map.into_values().collect());
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
                    if map.len() >= threshold {
                        maybe_records = Some(map.into_values().collect());
                    } else {
                        vac.insert(map);
                    }
                }
            }

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
                if let Err(e) = self.broadcast_tx.send(order_msg) {
                    return Err(format!("Failed to broadcast SEQ-ORDER: {}", e));
                }
            }
        }
        Ok(())
    }

    // Lines 24-28: finalize_seq_order - CHANGED to use multi-sig signing
    pub async fn finalize_seq_order(&self, sender: usize, order: SeqOrder) -> Result<(), String> {
        info!(
            "📥 [Sequencing] Node {} finalizing verified SEQ-ORDER",
            sender
        );
        let total_start = Instant::now();

        let sequences: Vec<u64> = order.records.iter().map(|r| r.sequence).collect();
        let median = self.calculate_median(&sequences);

        if *DISABLE_MULTISIG_VERIFICATION {
            let median_msg = SmrolMessage::SeqMedian {
                vc: order.vc.clone(),
                median_sequence: median,
                proof: Vec::new(),
                sender_id: self.process_id,
            };
            if let Err(e) = self.network.send_to_node(sender, median_msg).await {
                return Err(format!("Failed to send SEQ-MEDIAN: {}", e));
            }
            return Ok(());
        }

        // CHANGED: Use multi-sig signing
        let message = Self::build_median_signature_message(&order.vc, median);
        let task = FinalSignTask {
            sender,
            vc: order.vc.clone(),
            median_sequence: median,
            message,
        };
        self.enqueue_final_sign(task)?;

        let total_time = total_start.elapsed();
        if total_time > Duration::from_millis(10) {
            warn!("🐌 finalize_seq_order slow: {:?}", total_time);
        }

        Ok(())
    }

    // Lines 29-35: handle_seq_median - CHANGED to use multi-sig verification and aggregation
    pub async fn handle_seq_median(&self, sender: usize, median: SeqMedian) -> Result<(), String> {
        info!("📥 [Sequencing] received SEQ-MEDIAN from {}", sender);
        let total_start = Instant::now();

        if *DISABLE_MULTISIG_VERIFICATION {
            let vc_key = VC::from_slice(&median.vc);
            let threshold = self.f + 1;
            let mut ready_to_broadcast = false;

            match self.median_waiters.entry(vc_key) {
                Entry::Occupied(mut occ) => {
                    let set = occ.get_mut();
                    if set.insert(sender) && set.len() >= threshold {
                        ready_to_broadcast = true;
                        occ.remove();
                    }
                }
                Entry::Vacant(vac) => {
                    if threshold == 1 {
                        ready_to_broadcast = true;
                    } else {
                        let mut set = HashSet::with_capacity(threshold);
                        set.insert(sender);
                        vac.insert(set);
                    }
                }
            }

            if ready_to_broadcast && self.final_broadcasted.insert(vc_key) {
                let tx_id = self.resolve_tx_id(&vc_key);
                let final_msg = SmrolMessage::SeqFinal {
                    vc: median.vc.clone(),
                    final_sequence: median.s_tx,
                    combined_signature: Vec::new(),
                    signers: Vec::new(),
                    sender_id: self.process_id,
                    tx_id,
                };
                if let Err(e) = self.broadcast_tx.send(final_msg) {
                    return Err(format!("Failed to broadcast SEQ-FINAL: {}", e));
                }
            }
            return Ok(());
        }

        // CHANGED: Verify individual multi-sig signature (Line 31)
        let verify_start = Instant::now();
        let maybe_sig = self.verify_median_sig_async(&median, sender).await?;
        let verify_time = verify_start.elapsed();

        if let Some(valid_sig) = maybe_sig {
            let vc_key = VC::from_slice(&median.vc);
            let threshold = self.f + 1;

            // CHANGED: Collect BLS signatures instead of threshold shares (Line 32)
            let mut ready_to_broadcast: Option<Vec<(usize, BlstSignature)>> = None;

            match self.median_sigs.entry(vc_key) {
                Entry::Occupied(mut occ) => {
                    let map = occ.get_mut();
                    if map.contains_key(&sender) {
                        // Already have this signature
                    } else if map.len() >= threshold {
                        // Already have enough
                    } else {
                        map.insert(sender, valid_sig);
                        if map.len() == threshold {
                            let taken_map = std::mem::take(map);
                            occ.remove();
                            ready_to_broadcast = Some(taken_map.into_iter().collect());
                        }
                    }
                }
                Entry::Vacant(vac) => {
                    let mut map = HashMap::with_capacity(threshold);
                    map.insert(sender, valid_sig);
                    if map.len() == threshold {
                        ready_to_broadcast = Some(map.into_iter().collect());
                    } else {
                        vac.insert(map);
                    }
                }
            }

            // CHANGED: Check if collected f+1 signatures and aggregate (Lines 33-35)
            if let Some(sigs) = ready_to_broadcast {
                if self.final_broadcasted.insert(vc_key) {
                    let task = MedianCombineTask {
                        vc: median.vc.clone(),
                        s_tx: median.s_tx,
                        sigs,
                    };
                    if let Err(e) = self.enqueue_median_combine(task) {
                        self.final_broadcasted.remove(&vc_key);
                        return Err(e);
                    }
                }
            }
        } else {
            warn!(
                "❌ [Sequencing] Invalid multi-sig in SEQ-MEDIAN from node {}",
                sender
            );
        }

        Ok(())
    }

    // Lines 36-38: handle_seq_final - CHANGED to use multi-sig verification
    pub async fn handle_seq_final(&self, final_msg: SeqFinal) -> Result<(), String> {
        let vc_key = VC::from_slice(&final_msg.vc);
        if self.finalized_vcs.contains(&vc_key) {
            return Ok(());
        }

        if !self.final_verify_inflight.insert(vc_key) {
            return Ok(());
        }

        let task = FinalVerifyTask { final_msg };
        self.enqueue_final_verify(task)?;
        Ok(())
    }

    // Helper: verify ED25519 signature for seq response (unchanged)
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
            Err(_) => return Ok(false),
        };

        let message = Self::build_sequence_signature_message(&resp.vc, resp.s);
        Ok(verifying_key.verify_strict(&message, &signature).is_ok())
    }

    // CHANGED: Process median combine task - uses multi-sig aggregation
    async fn process_median_combine_task(&self, task: MedianCombineTask) -> Result<(), String> {
        let total_start = Instant::now();
        let MedianCombineTask { vc, s_tx, sigs } = task;
        let signers: Vec<usize> = sigs.iter().map(|(id, _)| *id).collect();
        let vc_key = VC::from_slice(&vc);

        // CHANGED: Aggregate BLS signatures (Line 34)
        let combine_start = Instant::now();
        let agg_sig = match self.aggregate_median_sigs_async(sigs).await {
            Ok(sig) => sig,
            Err(e) => {
                self.final_broadcasted.remove(&vc_key);
                return Err(e);
            }
        };
        let combine_time = combine_start.elapsed();

        let sigma_bytes = agg_sig.to_signature().to_bytes().to_vec();
        let tx_id = self.resolve_tx_id(&vc_key);

        // Broadcast SEQ-FINAL (Line 35)
        let final_msg = SmrolMessage::SeqFinal {
            vc: vc.clone(),
            final_sequence: s_tx,
            combined_signature: sigma_bytes,
            signers,
            sender_id: self.process_id,
            tx_id,
        };

        if let Err(e) = self.broadcast_tx.send(final_msg) {
            self.final_broadcasted.remove(&vc_key);
            return Err(format!("Failed to broadcast SEQ-FINAL: {}", e));
        }

        let total_time = total_start.elapsed();
        if total_time > Duration::from_millis(10) {
            warn!(
                "🐌 median combine task slow: total={:?}, combine={:?}",
                total_time, combine_time
            );
        }

        Ok(())
    }

    // CHANGED: Process final sign task - uses multi-sig signing
    async fn process_final_sign_task(&self, task: FinalSignTask) -> Result<(), String> {
        if *DISABLE_MULTISIG_VERIFICATION {
            return Ok(());
        }

        let FinalSignTask {
            sender,
            vc,
            median_sequence,
            message,
        } = task;

        // CHANGED: Multi-sig signing
        let sign_start = Instant::now();
        let sigma_seq = {
            let multisig_ctx = Arc::clone(&self.multisig_ctx);
            let node_id = self.process_id;
            let msg = message.clone();

            run_multisig_sign_task("sign_median", move || {
                let sig = multisig_ctx
                    .sign(node_id, &msg)
                    .ok_or_else(|| "failed to sign".to_string())?;
                Ok(sig.to_bytes().to_vec())
            })
            .await?
        };
        let sign_time = sign_start.elapsed();

        let median_msg = SmrolMessage::SeqMedian {
            vc: vc.clone(),
            median_sequence,
            proof: sigma_seq,
            sender_id: self.process_id,
        };

        if let Err(e) = self.network.send_to_node(sender, median_msg).await {
            return Err(format!("Failed to send SEQ-MEDIAN: {}", e));
        }

        if sign_time > Duration::from_millis(10) {
            warn!("🐌 final sign task slow: {:?}", sign_time);
        }

        Ok(())
    }

    // CHANGED: Process final verify task - uses multi-sig verification
    async fn process_final_verify_task(&self, task: FinalVerifyTask) -> Result<(), String> {
        let final_msg = task.final_msg;
        let vc_key = VC::from_slice(&final_msg.vc);

        // Extract signers from collected median signatures
        let signers: Vec<usize> = self
            .median_sigs
            .get(&vc_key)
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default();

        if signers.is_empty() || signers.len() < self.f + 1 {
            // Don't have signer info yet, will be verified when we get median messages
            self.final_verify_inflight.remove(&vc_key);
            return Err(format!("insufficient signers: {}", signers.len()));
        }

        // CHANGED: Verify aggregated multi-sig signature (Line 36)
        let verify_start = Instant::now();
        let verified = match self.verify_aggregated_sig_async(&final_msg, &signers).await {
            Ok(v) => v,
            Err(e) => {
                self.final_verify_inflight.remove(&vc_key);
                return Err(e);
            }
        };

        if !verified {
            self.final_verify_inflight.remove(&vc_key);
            return Ok(());
        }

        if self.finalized_vcs.contains(&vc_key) {
            self.final_verify_inflight.remove(&vc_key);
            return Ok(());
        }

        // Check if already in ledger or Mi (Lines 37-38)
        let (in_vc_ledger, in_mi) = {
            let finalization = self.finalization.read().await;
            (
                finalization.is_in_vc_ledger(&final_msg.vc),
                finalization.is_in_mi(&final_msg.vc),
            )
        };

        if in_vc_ledger || in_mi {
            self.final_verify_inflight.remove(&vc_key);
            return Ok(());
        }

        let maybe_entry = self.finalize_ready_final(final_msg).await;
        self.final_verify_inflight.remove(&vc_key);

        if let Some(entry) = maybe_entry {
            self.emit_sequenced_entry(entry).await?;
        }

        Ok(())
    }

    // Rest of the implementation (unchanged - helper methods, processing loops, etc.)
    fn calculate_median(&self, s_vec: &[u64]) -> u64 {
        let mut sorted = s_vec.to_vec();
        sorted.sort();
        sorted[sorted.len() / 2]
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

    async fn store_pending_final(&self, final_msg: SeqFinal) {
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
        } else if let Some((_, bytes)) = self.seq_payloads.remove(&final_msg.s_tx) {
            bytes
        } else if let Some(pkg_entry) = self.erasure_store.get(&vc_key) {
            let package_copy = pkg_entry.clone();
            drop(pkg_entry);
            match Self::reconstruct_full_async(package_copy).await {
                Ok(bytes) => {
                    self.erasure_store.remove(&vc_key);
                    bytes
                }
                Err(_) => {
                    self.store_pending_final(final_msg).await;
                    return None;
                }
            }
        } else {
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

        Some(TransactionEntry {
            vc_tx: final_msg.vc,
            s_tx: final_msg.s_tx,
            sigma: final_msg.sigma,
            payload,
        })
    }

    async fn reconstruct_full_async(package: ErasurePackage) -> Result<Vec<u8>, String> {
        run_seq_offload_task("erasure_reconstruct", move || package.reconstruct_full()).await
    }

    async fn emit_sequenced_entry(&self, entry: TransactionEntry) -> Result<(), String> {
        self.sequenced_entry_tx
            .send(entry)
            .map_err(|e| format!("sequenced entry send failed: {}", e))?;
        Ok(())
    }

    fn enqueue_median_combine(&self, task: MedianCombineTask) -> Result<(), String> {
        self.median_combine_tx
            .send(task)
            .map_err(|e| format!("enqueue median combine failed: {}", e))?;
        let pending = self.median_combine_backlog.fetch_add(1, Ordering::Relaxed) + 1;
        metrics::gauge!("smrol.channel_backlog", "channel" => "median_combine").set(pending as f64);
        Ok(())
    }

    fn enqueue_final_verify(&self, task: FinalVerifyTask) -> Result<(), String> {
        if let Err(err) = self.final_verify_tx.send(task) {
            let vc_key = VC::from_slice(&err.0.final_msg.vc);
            self.final_verify_inflight.remove(&vc_key);
            return Err("enqueue final verify failed".to_string());
        }
        let pending = self.final_verify_backlog.fetch_add(1, Ordering::Relaxed) + 1;
        metrics::gauge!("smrol.channel_backlog", "channel" => "final_verify").set(pending as f64);
        Ok(())
    }

    fn enqueue_final_sign(&self, task: FinalSignTask) -> Result<(), String> {
        self.final_sign_tx
            .send(task)
            .map_err(|e| format!("enqueue final sign failed: {}", e))?;
        let pending = self.final_sign_backlog.fetch_add(1, Ordering::Relaxed) + 1;
        metrics::gauge!("smrol.channel_backlog", "channel" => "final_sign").set(pending as f64);
        Ok(())
    }

    pub fn start_processing(self: &Arc<Self>) {
        warn!(
            "[Sequencing] Node {} Multi-sig verification disabled: {}",
            self.process_id, *DISABLE_MULTISIG_VERIFICATION
        );
        self.spawn_request_processor();
        self.spawn_response_processor();
        self.spawn_order_receiver();
        self.spawn_order_verifier();
        self.spawn_order_finalizer();
        self.spawn_median_processor();
        self.spawn_median_combine_processor();
        self.spawn_final_sign_processor();
        self.spawn_final_verify_processor();
        self.spawn_final_processor();
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

    // Spawn processor methods - same as original, just reference updated task processing methods
    fn spawn_request_processor(self: &Arc<Self>) {
        // Implementation same as original - calls process_seq_request_message
        // [Full implementation omitted for brevity - use original code]
    }

    fn spawn_response_processor(self: &Arc<Self>) {
        // [Use original implementation]
    }

    fn spawn_order_receiver(self: &Arc<Self>) {
        // [Use original implementation]
    }

    fn spawn_order_verifier(self: &Arc<Self>) {
        // [Use original implementation]
    }

    fn spawn_order_finalizer(self: &Arc<Self>) {
        // [Use original implementation]
    }

    fn spawn_median_processor(self: &Arc<Self>) {
        // [Use original implementation]
    }

    fn spawn_median_combine_processor(self: &Arc<Self>) {
        // [Use original implementation]
    }

    fn spawn_final_sign_processor(self: &Arc<Self>) {
        // [Use original implementation]
    }

    fn spawn_final_verify_processor(self: &Arc<Self>) {
        // [Use original implementation]
    }

    fn spawn_final_processor(self: &Arc<Self>) {
        // [Use original implementation]
    }

    // Process message methods
    async fn process_seq_request_message(
        &self,
        sender_id: usize,
        tx_hash: String,
        transaction: SmrolTransaction,
        sequence_number: u64,
    ) -> Result<(), String> {
        // [Use original implementation]
        Ok(())
    }

    async fn process_seq_response_message(
        &self,
        sender_id: usize,
        vc: Vec<u8>,
        signature_share: Vec<u8>,
        sequence_number: u64,
    ) -> Result<(), String> {
        // [Use original implementation]
        Ok(())
    }

    fn process_seq_order_message(
        &self,
        sender_id: usize,
        vc: Vec<u8>,
        responses: Vec<(usize, u64, Vec<u8>)>,
    ) -> Result<(), String> {
        // [Use original implementation]
        Ok(())
    }

    async fn process_seq_median_message(
        &self,
        sender_id: usize,
        vc: Vec<u8>,
        median_sequence: u64,
        proof: Vec<u8>,
    ) -> Result<(), String> {
        // [Use original implementation]
        Ok(())
    }

    async fn process_seq_final_message(&self, final_msg: SeqFinal) -> Result<(), String> {
        // [Use original implementation]
        Ok(())
    }

    pub async fn cleanup_expired_state(&self) {
        // [Use original implementation]
    }

    pub async fn get_pending_count(&self) -> usize {
        self.pending_txs.len()
    }

    pub async fn get_current_seq(&self) -> u64 {
        self.local_seq.load(Ordering::SeqCst).saturating_sub(1)
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
            Err(_) => true,
        };

        (waited_ok, Duration::from_millis(0))
    }
}

async fn verify_seq_order_records(
    order: Arc<SeqOrder>,
    verifying_keys: Arc<HashMap<usize, VerifyingKey>>,
    required: usize,
    cached_responses: Arc<DashMap<VC, HashMap<usize, SeqResponseRecord>>>,
) -> Result<bool, String> {
    // [Use original implementation]
    Ok(true)
}
