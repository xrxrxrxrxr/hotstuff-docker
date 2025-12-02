//! SMROL network layer built on top of the Pompe networking primitives.
//! This keeps the connection-pool/backoff design while dispatching
//! incoming frames to PNFIFO / Sequencing / Consensus queues directly.

use crate::affinity::{affinity_from_env, bind_current_thread};
use crate::flood_limiter::FloodLimiter;
use crate::smrol::message::SmrolMessage;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc, Mutex, OnceLock,
};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::{Builder, Runtime};
use tokio::sync::{mpsc as async_mpsc, mpsc::error::TrySendError, Mutex as AsyncMutex, RwLock};
use tracing::{debug, error, info, warn};

/// On-wire SMROL frame. Mirrors PompeNetworkMessage but carries SmrolMessage.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SmrolNetworkMessage {
    pub from_node_id: usize,
    pub to_node_id: Option<usize>,
    pub message: SmrolMessage,
    pub timestamp: u64,
    pub message_id: String,
}

#[derive(Debug)]
struct ConnectionStats {
    last_used: Instant,
    send_count: usize,
}

#[derive(Clone, Debug)]
struct ConnectionHandle {
    sender: async_mpsc::Sender<Arc<SmrolNetworkMessage>>,
    stats: Arc<Mutex<ConnectionStats>>,
    backlog: Arc<AtomicUsize>,
    target: usize,
}

#[derive(Debug, PartialEq, Eq)]
enum QueueSendResult {
    Enqueued,
    Saturated,
    Closed,
}

#[derive(Debug)]
pub struct SmrolTcpNetwork {
    node_id: usize,
    listen_port: u16,
    peer_nodes: HashMap<usize, SocketAddr>,

    message_tx: async_mpsc::UnboundedSender<(usize, SmrolMessage)>,
    message_rx: Arc<AsyncMutex<async_mpsc::UnboundedReceiver<(usize, SmrolMessage)>>>,

    connections: Arc<RwLock<HashMap<usize, ConnectionHandle>>>,
    sent_messages: Arc<Mutex<HashMap<String, u64>>>,

    rt: Arc<Runtime>,
    per_connection_queue: usize,
    writer_limiter: Arc<FloodLimiter>,
}

fn attack_only_mode() -> bool {
    static FLAG: OnceLock<bool> = OnceLock::new();
    *FLAG.get_or_init(|| {
        std::env::var("SMROL_ATTACK_ONLY")
            .map(|v| {
                matches!(
                    v.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false)
    })
}

fn per_connection_queue_capacity() -> usize {
    static CAP: OnceLock<usize> = OnceLock::new();
    *CAP.get_or_init(|| {
        std::env::var("SMROL_CONN_QUEUE_CAP")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .map(|cap| cap.max(64))
            .unwrap_or(1024)
    })
}

fn build_writer_limiter() -> FloodLimiter {
    if !attack_only_mode() {
        return FloodLimiter::unlimited("smrol-writer");
    }

    fn env_f64(key: &str) -> Option<f64> {
        std::env::var(key).ok()?.trim().parse::<f64>().ok()
    }

    let queue_cap = per_connection_queue_capacity().max(1) as f64;
    let default_rate = queue_cap * 4.0;
    let default_burst = queue_cap * 2.0;

    let rate = env_f64("SMROL_ATTACK_NETWORK_RATE").unwrap_or(default_rate);
    let burst = env_f64("SMROL_ATTACK_NETWORK_BURST").unwrap_or(default_burst);

    FloodLimiter::new("smrol-writer", rate, burst)
}

impl SmrolTcpNetwork {
    pub fn new(node_id: usize, peer_nodes: HashMap<usize, SocketAddr>) -> Self {
        let listen_port = 21000 + node_id as u16;

        let (message_tx, message_rx) = async_mpsc::unbounded_channel();

        let affinity_label = format!("smrol_net_rt_node{}", node_id);
        let affinity = affinity_from_env("SMROL_RT_CORES", &affinity_label).map(Arc::new);
        let requested_threads: usize = std::env::var("SMROL_RT_THREADS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(2);
        let rt_threads = match affinity.as_ref() {
            Some(cores) => {
                if cores.len() != requested_threads {
                    info!(
                        "[affinity] smrol network: overriding worker count {} -> {} to match core set",
                        requested_threads,
                        cores.len()
                    );
                }
                cores.len().max(1)
            }
            None => requested_threads.max(1),
        };

        let mut builder = Builder::new_multi_thread();
        builder
            .worker_threads(rt_threads)
            .enable_all()
            .thread_name(&format!("smrol-net-{}", node_id));

        if let Some(cores) = affinity.clone() {
            let cores = cores.clone();
            let label = affinity_label.clone();
            let assignment = Arc::new(AtomicUsize::new(0));
            builder.on_thread_start(move || {
                let idx = assignment.fetch_add(1, Ordering::Relaxed);
                if let Some(core) = cores.get(idx % cores.len()).copied() {
                    bind_current_thread(&label, core);
                }
            });
        }

        let runtime = Arc::new(builder.build().expect("Failed to build SMROL runtime"));

        info!(
            "🌐 [SMROL] Node {} initialised network runtime with {} worker(s) on port {}",
            node_id, rt_threads, listen_port
        );

        let per_connection_queue = per_connection_queue_capacity();
        let network = Self {
            node_id,
            listen_port,
            peer_nodes,
            message_tx,
            message_rx: Arc::new(AsyncMutex::new(message_rx)),
            connections: Arc::new(RwLock::new(HashMap::new())),
            sent_messages: Arc::new(Mutex::new(HashMap::new())),
            rt: runtime,
            per_connection_queue,
            writer_limiter: Arc::new(build_writer_limiter()),
        };

        network.start_connection_maintenance();
        network
    }

    pub fn spawn<F>(&self, fut: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let _ = self.rt.spawn(fut);
    }

    pub fn start_server(&self) -> Result<(), String> {
        let listener_addr = SocketAddr::from(([0, 0, 0, 0], self.listen_port));
        let net = self.clone();
        self.spawn(async move {
            match TcpListener::bind(listener_addr).await {
                Ok(listener) => {
                    info!(
                        "🎧 [SMROL] Node {} listening on {}",
                        net.node_id, listener_addr
                    );
                    loop {
                        match listener.accept().await {
                            Ok((stream, peer)) => {
                                debug!(
                                    "🔌 [SMROL] Node {} accepted connection from {}",
                                    net.node_id, peer
                                );
                                if let Err(e) = stream.set_nodelay(true) {
                                    warn!("⚠️ [SMROL] set_nodelay failed: {}", e);
                                }
                                let net_clone = net.clone();
                                tokio::spawn(async move {
                                    if let Err(e) =
                                        handle_incoming_connection(stream, net_clone).await
                                    {
                                        warn!("⚠️ [SMROL] inbound handler error: {}", e);
                                    }
                                });
                            }
                            Err(e) => {
                                error!("❌ [SMROL] accept failed: {}", e);
                                tokio::time::sleep(Duration::from_millis(100)).await;
                            }
                        }
                    }
                }
                Err(e) => {
                    error!(
                        "❌ [SMROL] Node {} failed to bind {}: {}",
                        net.node_id, listener_addr, e
                    );
                }
            }
        });
        Ok(())
    }

    pub fn warm_up_connections(&self) {
        let peers: Vec<usize> = self
            .peer_nodes
            .keys()
            .cloned()
            .filter(|nid| *nid != self.node_id)
            .collect();
        let net = self.clone();
        self.spawn(async move {
            for target in peers {
                let _ = net.send_to_node(target, SmrolMessage::Warmup).await;
            }
            info!("🔌 [SMROL] Node {} finished warmup", net.node_id);
        });
    }

    pub async fn send_to_node(
        &self,
        target_node_id: usize,
        message: SmrolMessage,
    ) -> Result<(), String> {
        if target_node_id == self.node_id {
            debug!(
                "📨 [SMROL] routing message to self: {:?}",
                std::mem::discriminant(&message)
            );
            self.enqueue_local(self.node_id, message).await?;
            return Ok(());
        }

        let addr = *self
            .peer_nodes
            .get(&target_node_id)
            .ok_or_else(|| format!("Unknown target node {}", target_node_id))?;

        let message_id = format!(
            "smrol:{}:{}:{}",
            self.node_id,
            target_node_id,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        {
            let mut sent = self.sent_messages.lock().unwrap();
            if sent.contains_key(&message_id) {
                debug!("🔄 [SMROL] skip duplicate send {}", message_id);
                return Ok(());
            }
            sent.insert(
                message_id.clone(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            );
        }

        let network_msg = Arc::new(SmrolNetworkMessage {
            from_node_id: self.node_id,
            to_node_id: Some(target_node_id),
            message,
            timestamp: current_micros(),
            message_id,
        });

        if !self.writer_limiter.allow(1.0) {
            return Ok(());
        }

        if let Some(handle) = {
            let connections = self.connections.read().await;
            connections.get(&target_node_id).cloned()
        } {
            match self.enqueue_to_handle(&handle, network_msg.clone())? {
                QueueSendResult::Enqueued => return Ok(()),
                QueueSendResult::Saturated => return Ok(()),
                QueueSendResult::Closed => {
                    self.connections.write().await.remove(&target_node_id);
                }
            }
        }

        let mut stream = TcpStream::connect(addr)
            .await
            .map_err(|e| format!("Connection failed {}: {}", addr, e))?;
        if let Err(e) = stream.set_nodelay(true) {
            warn!("⚠️ [SMROL] set_nodelay({}) failed: {}", addr, e);
        }

        let (_reader, writer) = stream.into_split();
        let (tx, rx) = async_mpsc::channel::<Arc<SmrolNetworkMessage>>(self.per_connection_queue);
        let stats = Arc::new(Mutex::new(ConnectionStats {
            last_used: Instant::now(),
            send_count: 0,
        }));
        let backlog = Arc::new(AtomicUsize::new(0));
        metrics::gauge!(
            "smrol.network_writer_backlog",
            "target" => target_node_id.to_string()
        )
        .set(0.0);
        let handle = ConnectionHandle {
            sender: tx.clone(),
            stats: Arc::clone(&stats),
            backlog: Arc::clone(&backlog),
            target: target_node_id,
        };

        self.connections
            .write()
            .await
            .insert(target_node_id, handle.clone());

        self.spawn_writer_task(
            target_node_id,
            writer,
            rx,
            Arc::clone(&self.connections),
            stats,
            backlog,
        );

        match self.enqueue_to_handle(&handle, network_msg)? {
            QueueSendResult::Enqueued | QueueSendResult::Saturated => Ok(()),
            QueueSendResult::Closed => {
                Err(format!("failed to enqueue message to {}", target_node_id))
            }
        }
    }

    // pub async fn broadcast(&self, message: SmrolMessage) -> Result<(), String> {
    //     let _ = self.send_to_node(self.node_id, message.clone()).await;

    //     let mut last_err: Option<String> = None;
    //     for (&nid, _) in &self.peer_nodes {
    //         if nid == self.node_id {
    //             continue;
    //         }
    //         if let Err(e) = self.send_to_node(nid, message.clone()).await {
    //             warn!("⚠️ [SMROL] broadcast to {} failed: {}", nid, e);
    //             last_err = Some(e);
    //         }
    //     }

    //     if let Some(err) = last_err {
    //         Err(err)
    //     } else {
    //         Ok(())
    //     }
    // }

    pub async fn broadcast(&self, message: SmrolMessage) -> Result<(), String> {
        self.broadcast_inner(message, true).await
    }

    pub async fn broadcast_skip_self(&self, message: SmrolMessage) -> Result<(), String> {
        self.broadcast_inner(message, false).await
    }

    async fn broadcast_inner(
        &self,
        message: SmrolMessage,
        include_self: bool,
    ) -> Result<(), String> {
        if include_self {
            let _ = self.send_to_node(self.node_id, message.clone()).await;
        }

        for (&nid, _) in &self.peer_nodes {
            if nid == self.node_id {
                continue;
            }

            let net = self.clone();
            let msg = message.clone();
            tokio::spawn(async move {
                let _ = net.send_to_node(nid, msg).await;
            });
        }
        Ok(())
    }

    fn enqueue_to_handle(
        &self,
        handle: &ConnectionHandle,
        msg: Arc<SmrolNetworkMessage>,
    ) -> Result<QueueSendResult, String> {
        match handle.sender.try_send(msg) {
            Ok(()) => {
                let pending = handle.backlog.fetch_add(1, Ordering::Relaxed) + 1;
                metrics::gauge!(
                    "smrol.network_writer_backlog",
                    "target" => handle.target.to_string()
                )
                .set(pending as f64);
                Ok(QueueSendResult::Enqueued)
            }
            Err(TrySendError::Full(_msg)) => {
                if attack_only_mode() {
                    self.writer_limiter.note_drop();
                }
                Ok(QueueSendResult::Saturated)
            }
            Err(TrySendError::Closed(_msg)) => Ok(QueueSendResult::Closed),
        }
    }

    fn spawn_writer_task(
        &self,
        target_node_id: usize,
        mut writer: OwnedWriteHalf,
        mut rx: async_mpsc::Receiver<Arc<SmrolNetworkMessage>>,
        connections: Arc<RwLock<HashMap<usize, ConnectionHandle>>>,
        stats: Arc<Mutex<ConnectionStats>>,
        backlog: Arc<AtomicUsize>,
    ) {
        let node_id = self.node_id;
        self.spawn(async move {
            let target_label = target_node_id.to_string();
            while let Some(msg) = rx.recv().await {
                let previous = backlog.fetch_sub(1, Ordering::Relaxed);
                let remaining = previous.saturating_sub(1);
                metrics::gauge!(
                    "smrol.network_writer_backlog",
                    "target" => target_label.clone()
                )
                .set(remaining as f64);
                let start = Instant::now();
                if let Err(e) = SmrolTcpNetwork::send_message_on_writer(&mut writer, &msg).await {
                    warn!(
                        "⚠️ [SMROL] writer task {} -> {} failed: {}",
                        node_id, target_node_id, e
                    );
                    break;
                }
                let elapsed = start.elapsed();

                // Monitor write time
                if elapsed > Duration::from_millis(5) {
                    warn!(
                        "⚠️ [Check] Writer {} -> {} slow write: {:?}",
                        node_id, target_node_id, elapsed
                    );
                }
                // if let Ok(mut guard) = stats.lock() {
                //     guard.last_used = Instant::now();
                //     guard.send_count += 1;
                // }
            }

            {
                let mut map = connections.write().await;
                map.remove(&target_node_id);
            }

            metrics::gauge!(
                "smrol.network_writer_backlog",
                "target" => target_label
            )
            .set(0.0);

            debug!(
                "🔌 [SMROL] writer task closed for {} -> {}",
                node_id, target_node_id
            );
        });
    }

    fn start_connection_maintenance(&self) {
        let connections = Arc::clone(&self.connections);
        let sent_messages = Arc::clone(&self.sent_messages);
        let node_id = self.node_id;
        self.spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(60));
            loop {
                ticker.tick().await;

                {
                    let mut guard = connections.write().await;
                    let mut to_remove = Vec::new();
                    for (&target, handle) in guard.iter() {
                        if let Ok(stats) = handle.stats.lock() {
                            if stats.last_used.elapsed() > Duration::from_secs(600) {
                                to_remove.push(target);
                            }
                        }
                    }
                    for target in to_remove {
                        if guard.remove(&target).is_some() {
                            info!(
                                "🧹 [SMROL] Node {} cleaned idle connection to {}",
                                node_id, target
                            );
                        }
                    }
                }

                {
                    let mut sent = sent_messages.lock().unwrap();
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                    sent.retain(|_, ts| now.saturating_sub(*ts) < 300);
                    if sent.len() > 2000 {
                        let mut entries: Vec<_> =
                            sent.iter().map(|(k, &v)| (k.clone(), v)).collect();
                        entries.sort_by_key(|(_, ts)| *ts);
                        for (key, _) in entries.into_iter().take(sent.len() / 2) {
                            sent.remove(&key);
                        }
                    }
                }
            }
        });
    }

    pub async fn recv(&self) -> Option<(usize, SmrolMessage)> {
        let mut guard = self.message_rx.lock().await;
        guard.recv().await
    }

    async fn enqueue_local(&self, sender: usize, message: SmrolMessage) -> Result<(), String> {
        self.message_tx
            .send((sender, message))
            .map_err(|e| format!("Failed to enqueue message: {}", e))
    }

    async fn send_message_on_writer(
        writer: &mut OwnedWriteHalf,
        network_msg: &SmrolNetworkMessage,
    ) -> Result<(), String> {
        let start = Instant::now();
        let serialized =
            bincode::serialize(network_msg).map_err(|e| format!("Serialization failed: {}", e))?;
        let len = serialized.len() as u32;
        // let t0=Instant::now();
        if len > 1_000_000 {
            // 1MB
            warn!(
                "⚠️ Large message: {} bytes to node {:?}",
                len, network_msg.to_node_id
            );
        }
        writer
            .write_all(&len.to_be_bytes())
            .await
            .map_err(|e| format!("Failed to write length: {}", e))?;
        // let elapsed_len = t0.elapsed();
        // if elapsed_len > Duration::from_millis(50) {
        //     warn!(
        //         "⚠️ [Check] TCP write length took {:?} ({} bytes)",
        //         elapsed_len, len
        //     );
        // }
        // let t1=Instant::now();
        writer
            .write_all(&serialized)
            .await
            .map_err(|e| format!("Failed to write message: {}", e))?;
        // let elapsed_msg = t1.elapsed();
        // if elapsed_msg > Duration::from_millis(50) {
        //     warn!(
        //         "⚠️ [Check] TCP write message took {:?} ({} bytes)",
        //         elapsed_msg, len
        //     );
        // }
        // let elapsed = start.elapsed();
        // if elapsed > Duration::from_millis(50) {
        //     warn!(
        //         "⚠️ [Check] TCP write took {:?} ({} bytes)",
        //         elapsed, len
        //     );
        // }

        Ok(())
    }
}

impl Clone for SmrolTcpNetwork {
    fn clone(&self) -> Self {
        Self {
            node_id: self.node_id,
            listen_port: self.listen_port,
            peer_nodes: self.peer_nodes.clone(),
            message_tx: self.message_tx.clone(),
            message_rx: Arc::clone(&self.message_rx),
            connections: Arc::clone(&self.connections),
            sent_messages: Arc::clone(&self.sent_messages),
            rt: Arc::clone(&self.rt),
            per_connection_queue: self.per_connection_queue,
            writer_limiter: Arc::clone(&self.writer_limiter),
        }
    }
}

async fn handle_incoming_connection(
    stream: TcpStream,
    network: SmrolTcpNetwork,
) -> Result<(), String> {
    let mut stream = stream;
    if let Err(e) = stream.set_nodelay(true) {
        warn!("⚠️ [SMROL] set_nodelay failed on incoming: {}", e);
    }

    let mut processed = HashSet::new();
    loop {
        let mut length_buf = [0u8; 4];
        if let Err(e) = stream.read_exact(&mut length_buf).await {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                debug!("🔌 [SMROL] inbound connection closed");
                return Ok(());
            }
            return Err(format!("Failed to read length: {}", e));
        }
        let len = u32::from_be_bytes(length_buf) as usize;
        if len == 0 {
            debug!("💓 [SMROL] heartbeat received");
            continue;
        }
        if len > 10 * 1024 * 1024 {
            return Err(format!("Message too large: {} bytes", len));
        }

        let mut buf = vec![0u8; len];
        stream
            .read_exact(&mut buf)
            .await
            .map_err(|e| format!("Failed to read message: {}", e))?;

        let frame: SmrolNetworkMessage =
            bincode::deserialize(&buf).map_err(|e| format!("Deserialization failed: {}", e))?;

        if processed.contains(&frame.message_id) {
            debug!("🔄 [SMROL] duplicate frame {} ignored", frame.message_id);
            continue;
        }
        processed.insert(frame.message_id.clone());
        if processed.len() > 1024 {
            processed.clear();
        }

        if let Err(e) = network
            .enqueue_local(frame.from_node_id, frame.message)
            .await
        {
            warn!("⚠️ [SMROL] inbound enqueue failed: {}", e);
            return Err(e);
        }
    }
}

fn current_micros() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}
