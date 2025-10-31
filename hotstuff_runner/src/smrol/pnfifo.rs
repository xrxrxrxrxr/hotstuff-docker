use crate::smrol::message::SmrolMessage;
use crate::smrol::network::SmrolTcpNetwork;
use blsttc::{
    PublicKeySet, SecretKeyShare, Signature as ThresholdSignature, SignatureShare, SIG_SIZE,
};
use crossbeam::channel::{unbounded, Sender};
use dashmap::DashMap;
use futures::task::AtomicWaker;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc as async_mpsc, Mutex as AsyncMutex, Notify};
use tracing::{debug, error, info, warn};

use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::task::{Context, Poll};
use std::thread;

// PROPOSAL messages sent over the dedicated channel
#[derive(Clone, Debug)]
pub struct ProposalMsg {
    pub slot: u64,
    pub value: Vec<u8>,
}

#[derive(Debug)]
pub struct VoteMsg {
    pub sender_id: usize,
    pub slot: u64,
    pub signature_share: Vec<u8>,
}

#[derive(Debug)]
pub struct FinalMsg {
    pub sender_id: usize,
    pub slot: u64,
    pub value: Vec<u8>,
    pub combined_signature: Vec<u8>,
}

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
            "pool" => "pnfifo"
        )
        .set(0.0);
        for idx in 0..worker_count {
            let thread_receiver = receiver.clone();
            thread::Builder::new()
                .name(format!("pnfifo-threshold-worker-{}", idx))
                .spawn(move || {
                    for job in thread_receiver.iter() {
                        job.run();
                    }
                })
                .expect("failed to spawn pnfifo threshold worker thread");
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
            "pool" => "pnfifo"
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
                "pool" => "pnfifo"
            )
            .set(remaining as f64);
            return Err("pnfifo threshold worker pool shut down".to_string());
        }
        Ok(ThresholdTaskFuture { state })
    }
}

static PNFIFO_THRESHOLD_POOL: Lazy<ThresholdThreadPool> = Lazy::new(|| {
    let workers = thread::available_parallelism()
        .map(|n| (n.get() * 2).max(4))
        .unwrap_or(4);
    // let workers = 16;
    ThresholdThreadPool::new(workers)
});

async fn run_pnfifo_threshold_task<R, F>(label: &'static str, task: F) -> Result<R, String>
where
    R: Send + 'static,
    F: FnOnce() -> Result<R, String> + Send + 'static,
{
    PNFIFO_THRESHOLD_POOL.submit(label, task)?.await
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
        let mut guard = self.result.lock().expect("pnfifo task state poisoned");
        *guard = Some(value);
        drop(guard);
        self.waker.wake();
    }

    fn take_result(&self) -> Option<Result<R, String>> {
        self.result
            .lock()
            .expect("pnfifo task state poisoned")
            .take()
    }

    fn register(&self, waker: &std::task::Waker) {
        self.waker.register(waker);
    }
}

struct ThresholdTaskFuture<R> {
    state: Arc<TaskState<R>>,
}

impl<R> Future for ThresholdTaskFuture<R>
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
                "task" => self.label,
                "pool" => "pnfifo"
            )
            .record(wait.as_secs_f64() * 1000.0);

            let result = task();
            let exec = start.elapsed();
            metrics::histogram!(
                "smrol.threshold_task_exec_ms",
                "task" => self.label,
                "pool" => "pnfifo"
            )
            .record(exec.as_secs_f64() * 1000.0);

            let prev = self.pending.fetch_sub(1, Ordering::Relaxed);
            let remaining = prev.saturating_sub(1);
            metrics::gauge!(
                "smrol.threshold_pending_jobs",
                "pool" => "pnfifo"
            )
            .set(remaining as f64);

            self.state.complete(result);
        }
    }
}
#[derive(Debug)]
pub struct PnfifoBc {
    node_id: usize,
    total_nodes: usize,
    threshold: usize,

    // Algorithm state
    current_slot: Arc<AtomicU64>,
    slot_values: DashMap<(usize, u64), Vec<u8>>,
    slot_outputs: DashMap<(usize, u64), (Vec<u8>, Vec<u8>)>,
    slot_proposal_flags: DashMap<(usize, u64), bool>,
    slot_final_flags: DashMap<(usize, u64), bool>,
    slot_votes: DashMap<(usize, u64), HashMap<usize, SignatureShare>>,
    slot_pending_finals: DashMap<(usize, u64), (Vec<u8>, Vec<u8>)>,
    notifier_states: DashMap<(usize, u64), Arc<Notify>>,

    proposal_tx: async_mpsc::UnboundedSender<(usize, ProposalMsg)>,
    proposal_rx: Arc<AsyncMutex<async_mpsc::UnboundedReceiver<(usize, ProposalMsg)>>>,
    vote_tx: async_mpsc::UnboundedSender<(usize, VoteMsg)>,
    vote_rx: Arc<AsyncMutex<async_mpsc::UnboundedReceiver<(usize, VoteMsg)>>>,
    final_tx: async_mpsc::UnboundedSender<(usize, FinalMsg)>,
    final_rx: Arc<AsyncMutex<async_mpsc::UnboundedReceiver<(usize, FinalMsg)>>>,

    // Cryptography
    threshold_share: SecretKeyShare,
    threshold_public: PublicKeySet,

    // Networking
    network: Arc<SmrolTcpNetwork>,
    broadcast_tx: async_mpsc::UnboundedSender<SmrolMessage>,
}

impl PnfifoBc {
    pub async fn new(
        node_id: usize,
        total_nodes: usize,
        threshold_share: SecretKeyShare,
        threshold_public: PublicKeySet,
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
            .map_err(|e| format!("Failed to start PNFIFO network: {}", e))?;

        let current_slot = Arc::new(AtomicU64::new(1));

        let (broadcast_tx, mut broadcast_rx) = async_mpsc::unbounded_channel::<SmrolMessage>();
        let network_for_broadcast = Arc::clone(&network);
        let node_id_for_broadcast = node_id;
        network.spawn(async move {
            info!(
                "[PNFIFO] node {} started dedicated broadcast worker",
                node_id_for_broadcast
            );
            while let Some(msg) = broadcast_rx.recv().await {
                let dispatch_start = Instant::now();
                let msg_kind = match &msg {
                    SmrolMessage::PnfifoProposal { slot, .. } => {
                        format!("proposal(slot={})", slot)
                    }
                    SmrolMessage::PnfifoFinal { slot, .. } => {
                        format!("final(slot={})", slot)
                    }
                    SmrolMessage::PnfifoVote { slot, .. } => {
                        format!("vote(slot={})", slot)
                    }
                    other => format!("{:?}", std::mem::discriminant(other)),
                };

                match network_for_broadcast.broadcast(msg).await {
                    Ok(_) => {
                        let elapsed = dispatch_start.elapsed();
                        if elapsed > Duration::from_millis(10) {
                            debug!(
                                "[PNFIFO] node {} broadcast {:?} exhibited high latency: {:?}",
                                node_id_for_broadcast, msg_kind, elapsed
                            );
                        }
                    }
                    Err(e) => {
                        error!("[PNFIFO] broadcast worker failed: {}", e);
                    }
                }
            }
            warn!("[PNFIFO] broadcast worker exited (channel closed)");
        });

        let slot_values = DashMap::new();
        let slot_outputs = DashMap::new();
        let slot_proposal_flags = DashMap::new();
        let slot_final_flags = DashMap::new();
        let slot_votes = DashMap::new();
        let slot_pending_finals = DashMap::new();
        let notifier_states = DashMap::new();

        let (proposal_tx, proposal_rx_raw) =
            async_mpsc::unbounded_channel::<(usize, ProposalMsg)>();
        let proposal_rx = Arc::new(AsyncMutex::new(proposal_rx_raw));
        let (vote_tx, vote_rx_raw) = async_mpsc::unbounded_channel::<(usize, VoteMsg)>();
        let vote_rx = Arc::new(AsyncMutex::new(vote_rx_raw));
        let (final_tx, final_rx_raw) = async_mpsc::unbounded_channel::<(usize, FinalMsg)>();
        let final_rx = Arc::new(AsyncMutex::new(final_rx_raw));

        Ok(Self {
            node_id,
            total_nodes,
            threshold,
            current_slot,
            slot_values,
            slot_outputs,
            slot_proposal_flags,
            slot_final_flags,
            slot_votes,
            slot_pending_finals,
            notifier_states,
            proposal_tx,
            proposal_rx,
            vote_tx,
            vote_rx,
            final_tx,
            final_rx,
            threshold_share,
            threshold_public,
            network,
            broadcast_tx,
        })
    }

    // PNFIFO internal: process leader proposals inside the dedicated channel task
    async fn process_leader_proposal_for_state(
        pnfifo: &Arc<Self>,
        leader_id: usize,
        slot: u64,
        value: Vec<u8>,
    ) -> Result<(), String> {
        let node_id = pnfifo.node_id;

        info!(
            "[PNFIFO] Node {} received PROPOSAL Leader {} slot {}",
            node_id, leader_id, slot
        );

        if !PnfifoBc::predicate_q_static(&value) {
            debug!(
                "[PNFIFO] Node {} rejected slot {} from leader {}: Q(v) not satisfied",
                node_id, slot, leader_id
            );
            return Ok(());
        }

        let key = (leader_id, slot);

        let already_proposed = pnfifo
            .slot_proposal_flags
            .get(&key)
            .map(|flag| *flag.value())
            .unwrap_or(false);

        if already_proposed && leader_id != node_id {
            debug!(
                "[PNFIFO] Node {} already processed Leader {} slot {}",
                node_id, leader_id, slot
            );
            return Ok(());
        }

        pnfifo.slot_values.insert(key, value.clone());
        pnfifo.slot_proposal_flags.insert(key, true);

        let delayed_final = pnfifo.slot_pending_finals.remove(&key).map(|entry| entry.1);

        if let Some((final_value, final_signature)) = delayed_final {
            debug!(
                "[PNFIFO] Node {} processing delayed FINAL for Leader {} slot {}",
                node_id, leader_id, slot
            );

            Self::finalize_with_signature_for_state(
                pnfifo,
                leader_id,
                slot,
                final_value,
                final_signature,
            )
            .await?;

            return Ok(());
        }

        let message_for_sign = Self::create_vote_message_static(slot, &value);
        let threshold_share = pnfifo.threshold_share.clone();
        let sign_start = Instant::now();
        let signature_share = run_pnfifo_threshold_task("pnfifo_sign", move || {
            Ok(threshold_share.sign(&message_for_sign))
        })
        .await?;
        let sign_elapsed = sign_start.elapsed();
        if sign_elapsed > Duration::from_millis(1) {
            debug!(
                "⏱️ [PNFIFO] Node {} threshold sign for Leader {} slot {} took {:?}",
                node_id, leader_id, slot, sign_elapsed
            );
        }
        let signature_share_bytes = signature_share.to_bytes();

        let vote_msg = SmrolMessage::PnfifoVote {
            leader_id,
            sender_id: node_id,
            slot,
            signature_share: signature_share_bytes.to_vec(),
        };

        pnfifo
            .network
            .send_to_node(leader_id, vote_msg)
            .await
            .map_err(|e| format!("Failed to send VOTE: {}", e))?;

        info!(
            "[PNFIFO] Node {} sent *VOTE* for Leader {} slot {}",
            node_id, leader_id, slot
        );

        debug!(
            "[PNFIFO] Node {} skip waiting for FINAL for Leader {} slot {} (pipeline mode)",
            node_id, leader_id, slot
        );

        Ok(())
    }

    pub fn network(&self) -> Arc<SmrolTcpNetwork> {
        Arc::clone(&self.network)
    }

    pub fn proposal_sender(&self) -> async_mpsc::UnboundedSender<(usize, ProposalMsg)> {
        self.proposal_tx.clone()
    }

    pub fn vote_sender(&self) -> async_mpsc::UnboundedSender<(usize, VoteMsg)> {
        self.vote_tx.clone()
    }

    pub fn final_sender(&self) -> async_mpsc::UnboundedSender<(usize, FinalMsg)> {
        self.final_tx.clone()
    }

    pub async fn broadcast(&self, slot: u64, value: Vec<u8>) -> Result<u64, String> {
        let proposal = Self::broadcast_proposal_for_leader(
            self.node_id,
            &self.current_slot,
            (self.node_id, slot),
            value,
            &self.slot_values,
            &self.slot_proposal_flags,
        )
        .await?;

        // self.broadcast_tx
        //     .send(proposal)
        //     .await
        //     .map_err(|e| format!("broadcast queue send failed: {}", e))?;

        // debug!(
        //     "[PNFIFO] Node {} enqueued slot {} for broadcast (single queue)",
        //     self.node_id, slot
        // );

        // Ok(slot)
        let enqueue_start = Instant::now();
        self.broadcast_tx
            .send(proposal)
            .map_err(|_| String::from("broadcast channel closed"))?;
        let enqueue_elapsed = enqueue_start.elapsed();
        debug!(
            "[PNFIFO] node {} enqueue broadcast slot {} took {:?}",
            self.node_id, slot, enqueue_elapsed
        );

        // tokio::task::yield_now().await;
        Ok(slot)
    }

    fn ensure_notifier(&self, leader_id: usize, slot: u64) -> Arc<Notify> {
        let key = (leader_id, slot);
        let entry = self
            .notifier_states
            .entry(key)
            .or_insert_with(|| Arc::new(Notify::new()));
        Arc::clone(entry.value())
    }

    fn get_notifier(&self, leader_id: usize, slot: u64) -> Option<Arc<Notify>> {
        self.notifier_states
            .get(&(leader_id, slot))
            .map(|entry| Arc::clone(entry.value()))
    }

    fn remove_notifier(&self, leader_id: usize, slot: u64) {
        self.notifier_states.remove(&(leader_id, slot));
    }

    async fn broadcast_proposal_for_leader(
        node_id: usize,
        current_slot: &Arc<AtomicU64>,
        key: (usize, u64),
        value: Vec<u8>,
        slot_values: &DashMap<(usize, u64), Vec<u8>>,
        slot_proposal_flags: &DashMap<(usize, u64), bool>,
    ) -> Result<SmrolMessage, String> {
        current_slot.store(
            key.1.saturating_add(1),
            std::sync::atomic::Ordering::Relaxed,
        );

        info!(
            "[PNFIFO] Node {} broadcast *PROPOSAL* for slot {}",
            node_id, key.1
        );

        slot_values.insert(key, value.clone());
        slot_proposal_flags.insert(key, true);

        let proposal = SmrolMessage::PnfifoProposal {
            sender_id: node_id,
            slot: key.1,
            value,
        };

        Ok(proposal)
    }

    async fn handle_vote_for_state(
        pnfifo: &Arc<Self>,
        leader_id: usize,
        sender_id: usize,
        slot: u64,
        signature_share: Vec<u8>,
    ) -> Result<(), String> {
        let node_id = pnfifo.node_id;

        info!(
            "[PNFIFO] Node {} received VOTE from {} for slot {} leader {}",
            node_id, sender_id, slot, leader_id
        );

        let key = (leader_id, slot);

        let Some(value) = pnfifo
            .slot_values
            .get(&key)
            .map(|entry| entry.value().clone())
        else {
            debug!(
                "[PNFIFO] Node {} received VOTE before PROPOSAL for slot {} leader {}",
                node_id, slot, leader_id
            );
            return Ok(());
        };

        if signature_share.len() != SIG_SIZE {
            warn!(
                "[PNFIFO] Node {} received malformed share length from {} for slot {} leader {}",
                node_id, sender_id, slot, leader_id
            );
            return Ok(());
        }

        let mut share_bytes = [0u8; SIG_SIZE];
        share_bytes.copy_from_slice(&signature_share);
        let share = match SignatureShare::from_bytes(share_bytes) {
            Ok(share) => share,
            Err(_) => {
                warn!(
                    "[PNFIFO] Node {} failed to parse signature share from {} for slot {} leader {}",
                    node_id, sender_id, slot, leader_id
                );
                return Ok(());
            }
        };

        let message_to_verify = PnfifoBc::create_vote_message_static(slot, &value);
        let pk_share = pnfifo.threshold_public.public_key_share(sender_id);
        let share_for_verify = share.clone();
        let message_for_verify = message_to_verify.clone();
        let verify_start = Instant::now();
        let share_valid = run_pnfifo_threshold_task("pnfifo_verify_share", move || {
            Ok(pk_share.verify(&share_for_verify, &message_for_verify))
        })
        .await?;
        let verify_elapsed = verify_start.elapsed();
        if verify_elapsed > Duration::from_millis(1) {
            debug!(
                "⏱️ [PNFIFO] Node {} threshold share verify from {} slot {} leader {} took {:?}",
                node_id, sender_id, slot, leader_id, verify_elapsed
            );
        }
        if !share_valid {
            warn!(
                "[PNFIFO] Node {} rejected invalid signature share from {} for slot {} leader {}",
                node_id, sender_id, slot, leader_id
            );
            return Ok(());
        }

        let vote_count = {
            let mut votes_entry = pnfifo.slot_votes.entry(key).or_insert_with(HashMap::new);
            let votes = votes_entry.value_mut();
            if votes.contains_key(&sender_id) {
                return Ok(());
            }
            votes.insert(sender_id, share.clone());
            votes.len()
        };

        if vote_count <= pnfifo.threshold {
            debug!(
                "[PNFIFO] Node {} accepted VOTE from {} (slot {} leader {}), votes: {}/{}",
                node_id, sender_id, slot, leader_id, vote_count, pnfifo.threshold
            );
        }

        if vote_count >= pnfifo.threshold {
            debug!(
                "[PNFIFO] Node {} reached threshold for Leader {} slot {} (votes={})",
                node_id, leader_id, slot, vote_count
            );

            let already_final = pnfifo
                .slot_final_flags
                .get(&key)
                .map(|flag| *flag.value())
                .unwrap_or(false)
                || pnfifo.slot_outputs.contains_key(&key);

            if already_final {
                return Ok(());
            }

            let shares: Vec<(usize, SignatureShare)> = pnfifo
                .slot_votes
                .get(&key)
                .map(|entry| {
                    entry
                        .iter()
                        .map(|(id, share)| (*id, share.clone()))
                        .collect()
                })
                .unwrap_or_default();

            let combined_signature = if shares.len() >= pnfifo.threshold {
                let threshold_public = pnfifo.threshold_public.clone();
                let shares_for_combine = shares.clone();
                let combine_start = Instant::now();
                let outcome = run_pnfifo_threshold_task("pnfifo_combine", move || {
                    threshold_public
                        .combine_signatures(
                            shares_for_combine.iter().map(|(id, share)| (*id, share)),
                        )
                        .map_err(|e| format!("Threshold signature combine failed: {}", e))
                })
                .await;
                let combine_elapsed = combine_start.elapsed();
                if combine_elapsed > Duration::from_millis(1) {
                    debug!(
                        "⏱️ [PNFIFO] Node {} threshold combine for Leader {} slot {} took {:?}",
                        node_id, leader_id, slot, combine_elapsed
                    );
                }
                match outcome {
                    Ok(sig) => Some(sig.to_bytes().to_vec()),
                    Err(e) => {
                        warn!(
                            "⚠️ [PNFIFO] Node {} failed to combine signature for slot {} leader {}: {}",
                            node_id, slot, leader_id, e
                        );
                        None
                    }
                }
            } else {
                None
            };

            if let Some(combined_sig) = combined_signature {
                pnfifo.slot_final_flags.insert(key, true);

                PnfifoBc::broadcast_final(
                    pnfifo.broadcast_tx.clone(),
                    node_id,
                    leader_id,
                    slot,
                    value,
                    combined_sig,
                )
                .await?;
            }
        }

        Ok(())
    }

    async fn broadcast_final(
        broadcast_tx: async_mpsc::UnboundedSender<SmrolMessage>,
        node_id: usize,
        leader_id: usize,
        slot: u64,
        value: Vec<u8>,
        combined_signature: Vec<u8>,
    ) -> Result<(), String> {
        let message = SmrolMessage::PnfifoFinal {
            leader_id,
            sender_id: node_id,
            slot,
            value,
            combined_signature,
        };

        info!("[PNFIFO] broadcast *FINAL* for slot {}", slot);
        broadcast_tx
            .send(message)
            .map_err(|e| format!("FINAL broadcast failed: {}", e))?;
        // tokio::task::yield_now().await;
        Ok(())
    }

    async fn handle_final_for_state(
        pnfifo: &Arc<Self>,
        leader_id: usize,
        sender_id: usize,
        slot: u64,
        value: Vec<u8>,
        combined_signature: Vec<u8>,
    ) -> Result<(), String> {
        let node_id = pnfifo.node_id;

        info!(
            "[PNFIFO] Node {} received FINAL from {} for Leader {} slot {}",
            node_id, sender_id, leader_id, slot
        );

        let key = (leader_id, slot);

        if pnfifo.slot_outputs.contains_key(&key) {
            debug!(
                "[PNFIFO] Node {} already processed FINAL for Leader {} slot {}",
                node_id, leader_id, slot
            );
            return Ok(());
        }

        let proposal_received = pnfifo
            .slot_proposal_flags
            .get(&key)
            .map(|flag| *flag.value())
            .unwrap_or(false);

        if !proposal_received {
            pnfifo
                .slot_pending_finals
                .insert(key, (value.clone(), combined_signature.clone()));
            debug!(
                "[PNFIFO] Node {} stored pending FINAL for Leader {} slot {} (waiting for PROPOSAL)",
                node_id, leader_id, slot
            );
            return Ok(());
        }

        Self::finalize_with_signature_for_state(pnfifo, leader_id, slot, value, combined_signature)
            .await
    }

    async fn finalize_with_signature_for_state(
        pnfifo: &Arc<Self>,
        leader_id: usize,
        slot: u64,
        value: Vec<u8>,
        combined_signature: Vec<u8>,
    ) -> Result<(), String> {
        // Validate signatures outside the lock (~100µs, avoids holding the lock)
        let message_to_verify = PnfifoBc::create_vote_message_static(slot, &value);

        if combined_signature.len() != SIG_SIZE {
            warn!(
                "[PNFIFO] Node {} slot {} combined signature length invalid",
                pnfifo.node_id, slot
            );
            return Ok(());
        }

        let mut sig_bytes = [0u8; SIG_SIZE];
        sig_bytes.copy_from_slice(&combined_signature);

        match ThresholdSignature::from_bytes(sig_bytes) {
            Ok(signature) => {
                let threshold_public = pnfifo.threshold_public.clone();
                let signature_for_verify = signature.clone();
                let message_for_verify = message_to_verify.clone();
                let verify_start = Instant::now();
                let signature_valid = run_pnfifo_threshold_task("pnfifo_verify_final", move || {
                    Ok(threshold_public
                        .public_key()
                        .verify(&signature_for_verify, &message_for_verify))
                })
                .await?;
                let verify_elapsed = verify_start.elapsed();
                if verify_elapsed > Duration::from_millis(1) {
                    debug!(
                        "⏱️ [PNFIFO] Node {} threshold verify FINAL leader {} slot {} took {:?}",
                        pnfifo.node_id, leader_id, slot, verify_elapsed
                    );
                }
                if !signature_valid {
                    warn!(
                        "[PNFIFO] Node {} slot {} signature verification failed",
                        pnfifo.node_id, slot
                    );
                    return Ok(());
                }

                let key = (leader_id, slot);

                if pnfifo.slot_outputs.contains_key(&key) {
                    debug!(
                        "[PNFIFO] Node {} already processed FINAL for Leader {} slot {}",
                        pnfifo.node_id, leader_id, slot
                    );
                    return Ok(());
                }

                pnfifo.slot_values.entry(key).or_insert(value.clone());
                pnfifo.slot_pending_finals.remove(&key);
                pnfifo
                    .slot_outputs
                    .insert(key, (value.clone(), combined_signature.clone()));
                pnfifo.slot_final_flags.insert(key, true);

                PnfifoBc::notify_slot(pnfifo, leader_id, slot);
            }
            Err(e) => {
                warn!(
                    "[PNFIFO] Node {} slot {} signature parsing error: {}",
                    pnfifo.node_id, slot, e
                );
            }
        }

        Ok(())
    }

    pub async fn get_output(&self, leader_id: usize, slot: u64) -> Option<(Vec<u8>, Vec<u8>)> {
        self.slot_outputs
            .get(&(leader_id, slot))
            .map(|entry| entry.value().clone())
    }

    // Used by the sequencing layer: wait for output availability (Algorithm 2 Line 5)
    pub async fn wait_for_output(&self, leader_id: usize, slot: u64) {
        let key = (leader_id, slot);

        if self.slot_outputs.get(&key).is_some() {
            debug!(
                "(wait_for_output) fast-path completed. leader {} slot {}",
                leader_id, slot
            );
            return;
        }

        let notifier = self.ensure_notifier(leader_id, slot);

        let mut attempts = 0;
        loop {
            let notified = notifier.notified();

            if self.slot_outputs.get(&key).is_some() {
                if attempts > 0 {
                    debug!(
                        "[PNFIFO] Node {} got output for Leader {} slot {} after {} waits",
                        self.node_id, leader_id, slot, attempts
                    );
                }
                return;
            }

            attempts += 1;
            if attempts % 100 == 0 {
                warn!(
                    "[PNFIFO] Node {} still waiting for Leader {} slot {} (attempt {})",
                    self.node_id, leader_id, slot, attempts
                );
            }

            notified.await;
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

    fn notify_slot(pnfifo: &Arc<Self>, leader_id: usize, slot: u64) {
        if let Some(notifier) = pnfifo.get_notifier(leader_id, slot) {
            notifier.notify_waiters();
            pnfifo.remove_notifier(leader_id, slot);
            debug!(
                "[PNFIFO] Notified waiters for Leader {} slot {}",
                leader_id, slot
            );
        }
    }

    pub async fn get_stats(&self) -> (usize, usize, u64) {
        let total_slots = self.slot_values.len();
        let completed_slots = self.slot_outputs.len();

        let current_slot = self.current_slot.load(std::sync::atomic::Ordering::Relaxed);

        (total_slots, completed_slots, current_slot)
    }

    pub async fn cleanup_old_slots(&self, keep_recent: u64) {
        let current = self.current_slot.load(std::sync::atomic::Ordering::Relaxed);
        let threshold = current.saturating_sub(keep_recent);

        let mut cleaned_values = 0;
        let mut cleaned_votes = 0;
        let mut cleaned_notifiers = 0;
        let mut cleaned_outputs = 0;
        let mut cleaned_pending = 0;

        self.slot_values.retain(|(_, slot_number), _| {
            if *slot_number <= threshold {
                cleaned_values += 1;
                false
            } else {
                true
            }
        });

        self.slot_outputs.retain(|(_, slot_number), _| {
            if *slot_number <= threshold {
                cleaned_outputs += 1;
                false
            } else {
                true
            }
        });

        self.slot_proposal_flags
            .retain(|(_, slot_number), _| *slot_number > threshold);
        self.slot_final_flags
            .retain(|(_, slot_number), _| *slot_number > threshold);

        self.slot_votes.retain(|(_, slot_number), _| {
            if *slot_number <= threshold {
                cleaned_votes += 1;
                false
            } else {
                true
            }
        });

        self.slot_pending_finals.retain(|(_, slot_number), _| {
            if *slot_number <= threshold {
                cleaned_pending += 1;
                false
            } else {
                true
            }
        });

        self.notifier_states.retain(|(_, slot_number), _| {
            if *slot_number <= threshold {
                cleaned_notifiers += 1;
                false
            } else {
                true
            }
        });

        if cleaned_values > 0
            || cleaned_outputs > 0
            || cleaned_votes > 0
            || cleaned_pending > 0
            || cleaned_notifiers > 0
        {
            info!(
                "[PNFIFO] Node {} cleaned values={} outputs={} votes={} pending={} notifiers={} below {}",
                self.node_id,
                cleaned_values,
                cleaned_outputs,
                cleaned_votes,
                cleaned_pending,
                cleaned_notifiers,
                threshold
            );
        }
    }

    pub async fn start(self: &Arc<Self>) -> Result<(), String> {
        self.spawn_proposal_processor();
        self.spawn_vote_processor();
        self.spawn_final_processor();

        info!("[PNFIFO] node {} initialization complete", self.node_id);
        Ok(())
    }

    fn spawn_proposal_processor(self: &Arc<Self>) {
        let proposal_rx = Arc::clone(&self.proposal_rx);
        let pnfifo = Arc::clone(self);
        tokio::spawn(async move {
            info!(
                "[PNFIFO] Node {} proposal processor started",
                pnfifo.node_id
            );

            let mut count = 0;
            let mut total_time = Duration::ZERO;
            let mut max_time = Duration::ZERO;
            let mut last_log = Instant::now();

            while let Some((leader_id, message)) = {
                let mut rx = proposal_rx.lock().await;
                rx.recv().await
            } {
                count += 1;
                let start = Instant::now();
                if let Err(e) = PnfifoBc::process_leader_proposal_for_state(
                    &pnfifo,
                    leader_id,
                    message.slot,
                    message.value,
                )
                .await
                {
                    warn!(
                        "⚠️ [PNFIFO] Node {} proposal handling failed (leader {} slot {}): {}",
                        pnfifo.node_id, leader_id, message.slot, e
                    );
                }
                let elapsed = start.elapsed();
                if elapsed > Duration::from_millis(10) {
                    debug!(
                        "🐌 [SLOW] Proposal handler slow for leader {} slot {}: {:?}",
                        leader_id, message.slot, elapsed
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
                        "📊 [Critical] Proposal stats: {} msgs, avg={:?}, max={:?}, rate={}/s",
                        count, avg, max_time, count
                    );
                    count = 0;
                    total_time = Duration::ZERO;
                    max_time = Duration::ZERO;
                    last_log = Instant::now();
                }
            }

            warn!(
                "⚠️ [PNFIFO] Node {} proposal processor exiting",
                pnfifo.node_id
            );
        });
    }

    fn spawn_vote_processor(self: &Arc<Self>) {
        let vote_rx = Arc::clone(&self.vote_rx);
        let pnfifo = Arc::clone(self);
        tokio::spawn(async move {
            info!("[PNFIFO] Node {} vote processor started", pnfifo.node_id);

            let mut count = 0;
            let mut total_time = Duration::ZERO;
            let mut max_time = Duration::ZERO;
            let mut last_log = Instant::now();

            while let Some((leader_id, message)) = {
                let mut rx = vote_rx.lock().await;
                rx.recv().await
            } {
                count += 1;
                let start = Instant::now();
                if let Err(e) = PnfifoBc::handle_vote_for_state(
                    &pnfifo,
                    leader_id,
                    message.sender_id,
                    message.slot,
                    message.signature_share,
                )
                .await
                {
                    warn!(
                        "⚠️ [PNFIFO] Node {} vote handling failed (leader {} slot {}): {}",
                        pnfifo.node_id, leader_id, message.slot, e
                    );
                }
                let elapsed = start.elapsed();
                if elapsed > Duration::from_millis(10) {
                    debug!(
                        "🐌 [SLOW] Vote handler slow for leader {} slot {}: {:?}",
                        leader_id, message.slot, elapsed
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
                        "📊 [Critical] Vote stats: {} msgs, avg={:?}, max={:?}, rate={}/s",
                        count, avg, max_time, count
                    );
                    count = 0;
                    total_time = Duration::ZERO;
                    max_time = Duration::ZERO;
                    last_log = Instant::now();
                }
            }

            warn!("⚠️ [PNFIFO] Node {} vote processor exiting", pnfifo.node_id);
        });
    }

    fn spawn_final_processor(self: &Arc<Self>) {
        let final_rx = Arc::clone(&self.final_rx);
        let pnfifo = Arc::clone(self);
        tokio::spawn(async move {
            info!("[PNFIFO] Node {} final processor started", pnfifo.node_id);

            let mut count = 0;
            let mut total_time = Duration::ZERO;
            let mut max_time = Duration::ZERO;
            let mut last_log = Instant::now();

            while let Some((leader_id, message)) = {
                let mut rx = final_rx.lock().await;
                rx.recv().await
            } {
                count += 1;
                let start = Instant::now();
                if let Err(e) = PnfifoBc::handle_final_for_state(
                    &pnfifo,
                    leader_id,
                    message.sender_id,
                    message.slot,
                    message.value,
                    message.combined_signature,
                )
                .await
                {
                    warn!(
                        "⚠️ [PNFIFO] Node {} final handling failed (leader {} slot {}): {}",
                        pnfifo.node_id, leader_id, message.slot, e
                    );
                }
                let elapsed = start.elapsed();
                if elapsed > Duration::from_millis(10) {
                    debug!(
                        "🐌 [SLOW] Final handler slow for leader {} slot {}: {:?}",
                        leader_id, message.slot, elapsed
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
                        "📊 [Critical] Final stats: {} msgs, avg={:?}, max={:?}, rate={}/s",
                        count, avg, max_time, count
                    );
                    count = 0;
                    total_time = Duration::ZERO;
                    max_time = Duration::ZERO;
                    last_log = Instant::now();
                }
            }

            warn!(
                "⚠️ [PNFIFO] Node {} final processor exiting",
                pnfifo.node_id
            );
        });
    }
}
