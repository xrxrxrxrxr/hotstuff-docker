// hotstuff_runner/src/pompe_network.rs
//! Pompe network implementation with fixes for incomplete timestamp collection

use crate::affinity::{affinity_from_env, bind_current_thread};
use crate::pompe::PompeMessage;
use crate::resolve_target;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::{Builder, Runtime};
use tokio::sync::mpsc as async_mpsc;
use tokio::sync::Mutex as AsyncMutex;
use tracing::{debug, error, info, warn};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PompeNetworkMessage {
    pub from_node_id: usize,
    pub to_node_id: Option<usize>,
    pub message: PompeMessage,
    pub timestamp: u64,
    pub message_id: String, // Message ID used for deduplication
}

#[derive(Debug)]
struct ConnectionStats {
    last_used: Instant,
    send_count: usize,
}

#[derive(Clone, Debug)]
struct ConnectionHandle {
    sender: async_mpsc::UnboundedSender<Arc<PompeNetworkMessage>>,
    stats: Arc<Mutex<ConnectionStats>>,
}

// #[derive(Clone)]
pub struct PompeNetwork {
    node_id: usize,
    pompe_port: u16,
    pub peer_node_ids: Vec<usize>,
    message_tx: async_mpsc::UnboundedSender<(usize, PompeMessage)>,
    message_rx: Arc<AsyncMutex<async_mpsc::UnboundedReceiver<(usize, PompeMessage)>>>,

    // Connection pool with retry handling
    // connection_pool: Arc<Mutex<HashMap<usize, Option<TcpStream>>>>,
    // Per-connection state guarded by its own AsyncMutex to avoid holding write locks across await
    connections: Arc<tokio::sync::RwLock<HashMap<usize, ConnectionHandle>>>,
    sent_messages: Arc<Mutex<HashMap<String, u64>>>, // Deduplicate messages
    // Dedicated runtime so Pompe networking does not block other tasks
    rt: Arc<Runtime>,
}

impl PompeNetwork {
    pub fn new(node_id: usize, peer_node_ids: Vec<usize>) -> Self {
        // Configure the Pompe port via environment variables:
        // 1) POMPE_PORT=<port> (takes priority)
        // 2) Or POMPE_PORT_BASE=<base port, default 20000>, computed as base + node_id
        let pompe_port: u16 = std::env::var("POMPE_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| {
                let base: u16 = std::env::var("POMPE_PORT_BASE")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(20000);
                base //+ node_id as u16
            });
        let (tx, rx) = async_mpsc::unbounded_channel();
        // Build a dedicated Tokio runtime (thread count via POMPE_RT_THREADS, default 2)
        let affinity_label = format!("pompe_net_rt_node{}", node_id);
        let affinity = affinity_from_env("POMPE_RT_CORES", &affinity_label).map(Arc::new);
        let requested_threads: usize = std::env::var("POMPE_RT_THREADS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(2);
        let rt_threads = match affinity.as_ref() {
            Some(cores) => {
                if cores.len() != requested_threads {
                    info!(
                        "[affinity] pompe network: overriding worker count {} -> {} to match core set",
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
            .thread_name(&format!("pompe-net-{}", node_id));

        if let Some(core_set) = affinity.clone() {
            let cores = core_set.clone();
            let assignment = Arc::new(AtomicUsize::new(0));
            let label = affinity_label.clone();
            builder.on_thread_start(move || {
                let idx = assignment.fetch_add(1, Ordering::Relaxed);
                if let Some(core) = cores.get(idx % cores.len()).copied() {
                    bind_current_thread(&label, core);
                }
            });
        }

        let rt = Arc::new(builder.build().expect("Failed to build Pompe runtime"));

        info!(
            "Creating Pompe network for node {}, port {}",
            node_id, pompe_port
        );
        info!("Peer node list: {:?}", peer_node_ids);

        // Ensure the current node is present in the peer list
        if !peer_node_ids.contains(&node_id) {
            warn!(
                "Current node {} not found in peer list: {:?}",
                node_id, peer_node_ids
            );
        }

        let network = Self {
            node_id,
            pompe_port,
            peer_node_ids,
            message_tx: tx,
            message_rx: Arc::new(AsyncMutex::new(rx)),
            connections: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            // connection_pool: Arc::new(Mutex::new(HashMap::new())),
            sent_messages: Arc::new(Mutex::new(HashMap::new())),
            rt,
        };
        // Start connection maintenance in the background
        network.start_connection_maintenance();
        network
    }

    // Background task that maintains connections and trims dedup state
    fn start_connection_maintenance(&self) {
        let connections = Arc::clone(&self.connections);
        let node_id = self.node_id;
        // Also keep a handle to sent_messages for periodic cleanup
        let sent_messages = Arc::clone(&self.sent_messages);

        let rt = self.rt.clone();
        rt.spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60)); // Clean once every 60 seconds

            loop {
                interval.tick().await;

                {
                    let mut guard = connections.write().await;
                    let mut to_remove = Vec::new();
                    for (&target_node_id, handle) in guard.iter() {
                        if let Ok(stats) = handle.stats.lock() {
                            if stats.last_used.elapsed() > Duration::from_secs(600) {
                                to_remove.push(target_node_id);
                            }
                        }
                    }

                    for node_id_to_remove in to_remove {
                        if guard.remove(&node_id_to_remove).is_some() {
                            info!(
                                "[connection maintenance] node {} removed idle connection to node {}",
                                node_id, node_id_to_remove
                            );
                        }
                    }
                }

                // Periodically cleanup dedup records to prevent unbounded growth
                {
                    let mut sent = sent_messages.lock().unwrap();
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                    sent.retain(|_, ts| now.saturating_sub(*ts) < 300);
                    if sent.len() > 2000 {
                        // trim older half if too many
                        let mut entries: Vec<_> =
                            sent.iter().map(|(k, &v)| (k.clone(), v)).collect();
                        entries.sort_by_key(|(_, v)| *v);
                        for (k, _) in entries.into_iter().take(sent.len() / 2) {
                            sent.remove(&k);
                        }
                    }
                }
            }
        });
    }

    pub fn start_server(&self) -> Result<(), String> {
        let addr = format!("0.0.0.0:{}", self.pompe_port);
        let message_tx = self.message_tx.clone();
        let node_id = self.node_id;
        let rt = self.rt.clone();
        rt.spawn(async move {
            match TcpListener::bind(&addr).await {
                Ok(listener) => {
                    info!("Node {} Pompe server listening on {}", node_id, addr);
                    loop {
                        match listener.accept().await {
                            Ok((mut socket, peer)) => {
                                debug!("Node {} Pompe connection from {}", node_id, peer);
                                if let Err(e) = socket.set_nodelay(true) {
                                    warn!("Failed to set TCP_NODELAY: {}", e);
                                }
                                let tx = message_tx.clone();
                                tokio::spawn(async move {
                                    let _ = handle_pompe_connection(&mut socket, tx).await;
                                });
                            }
                            Err(e) => {
                                warn!("Node {} Pompe accept error: {}", node_id, e);
                                tokio::time::sleep(Duration::from_millis(100)).await;
                            }
                        }
                    }
                }
                Err(e) => error!(
                    "Node {} failed to bind Pompe address {}: {}",
                    node_id, addr, e
                ),
            }
        });
        Ok(())
    }

    // Proactively warm connections to all peers to reduce first-send latency
    pub fn warm_up_connections(&self) {
        let peers: Vec<usize> = self
            .peer_node_ids
            .iter()
            .cloned()
            .filter(|nid| *nid != self.node_id)
            .collect();
        let net = self.clone();
        let rt = self.rt.clone();
        rt.spawn(async move {
            for nid in peers {
                let _ = net
                    .send_to_node(
                        nid,
                        PompeMessage::Ordering2Response {
                            tx_hash: "warmup".to_string(),
                            timestamp: 0,
                            node_id: net.node_id,
                        },
                    )
                    .await;
                // Ignore failures; the pool will attempt to reconnect later
            }
            info!("Node {} connection warm-up task complete", net.node_id);
        });
    }

    pub fn spawn<F>(&self, fut: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let _ = self.rt.spawn(fut);
    }

    // Improved single-node send with retries and connection pooling
    pub async fn send_to_node(
        &self,
        target_node_id: usize,
        message: PompeMessage,
    ) -> Result<(), String> {
        // Special handling: send to self
        if target_node_id == self.node_id {
            debug!(
                "Sending Pompe message to self: {:?}",
                std::mem::discriminant(&message)
            );
            if let Err(e) = self.message_tx.send((self.node_id, message)) {
                error!(
                    "Node {} failed to send Pompe message to self: {}",
                    self.node_id, e
                );
                return Err(format!("Send-to-self failed: {}", e));
            }
            return Ok(());
        }

        // Generate a message ID for deduplication
        let message_id = format!(
            "{}:{}:{}",
            self.node_id,
            target_node_id,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        // Check whether we have already sent this message
        {
            let mut sent = self.sent_messages.lock().unwrap();
            if sent.contains_key(&message_id) {
                debug!("Skipping duplicate message: {}", message_id);
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

        let network_msg = Arc::new(PompeNetworkMessage {
            from_node_id: self.node_id,
            to_node_id: Some(target_node_id),
            message,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64,
            message_id,
        });

        if let Some(handle) = {
            let guard = self.connections.read().await;
            guard.get(&target_node_id).cloned()
        } {
            if self
                .enqueue_to_handle(handle.clone(), Arc::clone(&network_msg))
                .await?
            {
                if let Ok(mut stats) = handle.stats.lock() {
                    stats.last_used = Instant::now();
                    stats.send_count += 1;
                }
                return Ok(());
            } else {
                self.connections.write().await.remove(&target_node_id);
                warn!(
                    "Node {} -> Node {} connection unavailable; rebuilding",
                    self.node_id, target_node_id
                );
            }
        }

        let target_addr = resolve_target(target_node_id, 20000);
        info!(
            "Node {} resolved Pompe address for node {}: {}",
            self.node_id, target_node_id, target_addr
        );

        let stream = TcpStream::connect(&target_addr).await.map_err(|e| {
            error!(
                "Node {} failed to connect to node {}: {}",
                self.node_id, target_node_id, e
            );
            format!("Connection failed: {}", e)
        })?;

        if let Err(e) = stream.set_nodelay(true) {
            warn!("Failed to set TCP_NODELAY: {}", e);
        }

        let (_reader_half, writer_half) = stream.into_split();
        let (tx, rx) = async_mpsc::unbounded_channel::<Arc<PompeNetworkMessage>>();
        let stats = Arc::new(Mutex::new(ConnectionStats {
            last_used: Instant::now(),
            send_count: 0,
        }));
        let handle = ConnectionHandle {
            sender: tx.clone(),
            stats: Arc::clone(&stats),
        };

        self.connections
            .write()
            .await
            .insert(target_node_id, handle.clone());

        self.spawn_writer_task(
            target_node_id,
            writer_half,
            rx,
            Arc::clone(&self.connections),
            stats,
        );

        if self.enqueue_to_handle(handle.clone(), network_msg).await? {
            if let Ok(mut stats) = handle.stats.lock() {
                stats.last_used = Instant::now();
                stats.send_count += 1;
            }
            Ok(())
        } else {
            self.connections.write().await.remove(&target_node_id);
            Err(format!("Failed to send to node {}", target_node_id))
        }
    }

    async fn enqueue_to_handle(
        &self,
        handle: ConnectionHandle,
        msg: Arc<PompeNetworkMessage>,
    ) -> Result<bool, String> {
        match handle.sender.send(msg) {
            Ok(()) => Ok(true),
            Err(_e) => Ok(false),
        }
    }

    fn spawn_writer_task(
        &self,
        target_node_id: usize,
        mut writer: OwnedWriteHalf,
        mut rx: async_mpsc::UnboundedReceiver<Arc<PompeNetworkMessage>>,
        connections: Arc<tokio::sync::RwLock<HashMap<usize, ConnectionHandle>>>,
        stats: Arc<Mutex<ConnectionStats>>,
    ) {
        let node_id = self.node_id;
        self.spawn(async move {
            while let Some(msg) = rx.recv().await {
                let write_start = Instant::now();
                if let Err(e) =
                    PompeNetwork::send_message_on_writer(node_id, &mut writer, &msg).await
                {
                    warn!(
                        "[Pompe] {} -> {} writer task send failed: {}",
                        node_id, target_node_id, e
                    );
                    break;
                }

                if let Ok(mut stats_guard) = stats.lock() {
                    stats_guard.last_used = Instant::now();
                    stats_guard.send_count += 1;
                }

                let elapsed = write_start.elapsed();
                if elapsed > Duration::from_millis(5) {
                    debug!(
                        "[Pompe] {} -> {} send slow: {:?}",
                        node_id, target_node_id, elapsed
                    );
                }
            }

            let mut guard = connections.write().await;
            if guard.remove(&target_node_id).is_some() {
                info!(
                    "[Pompe] node {} closed writer to {}",
                    node_id, target_node_id
                );
            }
        });
    }

    // Helper to send a message on a given TCP writer
    async fn send_message_on_writer(
        node_id: usize,
        writer: &mut OwnedWriteHalf,
        network_msg: &PompeNetworkMessage,
    ) -> Result<(), String> {
        // Use bincode for compact serialization to reduce cost and jitter
        let ser_start = std::time::Instant::now();
        let serialized =
            bincode::serialize(network_msg).map_err(|e| format!("Serialization failed: {}", e))?;
        let message_length = serialized.len() as u32;
        let ser_cost = ser_start.elapsed();

        writer
            .write_all(&message_length.to_be_bytes())
            .await
            .map_err(|e| format!("Failed to write length: {}", e))?;

        writer
            .write_all(&serialized)
            .await
            .map_err(|e| format!("Failed to write message: {}", e))?;

        // Flush is usually a no-op for TCP; avoid unnecessary syscalls
        if ser_cost.as_micros() > 50 {
            debug!(
                "[Pompe-serialize] node {} serialization: {:?} ({} bytes)",
                node_id, ser_cost, message_length
            );
        }
        Ok(())
    }

    // Parallel broadcast: short-circuit self then fan out concurrently
    pub async fn broadcast(&self, message: PompeMessage) -> Result<(), String> {
        use tokio::task::JoinHandle;
        let start_time = std::time::Instant::now();
        info!(
            "Node {} Pompe broadcast {:?} to {} nodes",
            self.node_id,
            std::mem::discriminant(&message),
            self.peer_node_ids.len()
        );

        let mut success_count = 0usize;
        let mut failure_details: Vec<String> = Vec::new();

        // 1) Send to self first (short-circuit, no TCP)
        if self.peer_node_ids.contains(&self.node_id) {
            match self.send_to_node(self.node_id, message.clone()).await {
                Ok(_) => success_count += 1,
                Err(e) => failure_details.push(format!("self: {}", e)),
            }
        }

        // 2) Send to other nodes in parallel
        let mut handles: Vec<JoinHandle<(usize, Result<(), String>)>> = Vec::new();
        for &target_node_id in &self.peer_node_ids {
            if target_node_id == self.node_id {
                continue;
            }
            let net = self.clone();
            let msg = message.clone();
            // Use the dedicated Pompe runtime to avoid contending with the global runtime
            let handle = tokio::spawn(async move {
                let res = net.send_to_node(target_node_id, msg).await;
                (target_node_id, res)
            });
            handles.push(handle);
        }

        for h in handles {
            match h.await {
                Ok((nid, Ok(()))) => success_count += 1,
                Ok((nid, Err(e))) => failure_details.push(format!("Node {}: {}", nid, e)),
                Err(e) => failure_details.push(format!("JoinError: {}", e)),
            }
        }

        let total_duration = start_time.elapsed();
        info!(
            "[broadcast complete] node {}: {}/{} succeeded, elapsed {:?}",
            self.node_id,
            success_count,
            self.peer_node_ids.len(),
            total_duration
        );

        if !failure_details.is_empty() {
            warn!(
                "Node {} Pompe broadcast partially failed: {:?}",
                self.node_id, failure_details
            );
        }

        if success_count > 0 {
            Ok(())
        } else {
            Err("All broadcast targets failed".to_string())
        }
    }

    pub async fn recv(&self) -> Option<(usize, PompeMessage)> {
        let mut rx = self.message_rx.lock().await;
        rx.recv().await
    }

    // Clean up expired message metadata
    pub fn cleanup_old_messages(&self) {
        let mut sent = self.sent_messages.lock().unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Drop message records older than 5 minutes
        sent.retain(|_, &mut timestamp| now - timestamp < 300);

        if sent.len() > 1000 {
            // Trim the oldest half if the cache grows too large
            let mut entries: Vec<_> = sent.iter().map(|(k, &v)| (k.clone(), v)).collect();
            entries.sort_by_key(|(_, timestamp)| *timestamp);

            let keys_to_remove: Vec<_> = entries
                .iter()
                .take(entries.len() / 2)
                .map(|(message_id, _)| message_id.clone())
                .collect();

            for message_id in keys_to_remove {
                sent.remove(&message_id);
            }
        }
    }
    // Inspect the connection pool state
    pub async fn get_connection_stats(&self) -> (usize, usize) {
        let connections = self.connections.read().await;
        let active_connections = connections.len();
        let mut total_messages: usize = 0;
        for handle in connections.values() {
            if let Ok(stats) = handle.stats.lock() {
                total_messages += stats.send_count;
            }
        }

        if active_connections > 0 {
            info!(
                "[connection pool] node {} active connections: {}, total sends: {}",
                self.node_id, active_connections, total_messages
            );
        }

        (active_connections, total_messages)
    }
}

async fn handle_pompe_connection(
    socket: &mut TcpStream,
    message_tx: async_mpsc::UnboundedSender<(usize, PompeMessage)>,
) -> Result<(), String> {
    let mut processed_messages = std::collections::HashSet::new();

    loop {
        let mut length_buf = [0u8; 4];
        match socket.read_exact(&mut length_buf).await {
            Ok(_) => {}
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                debug!("Pompe connection closed cleanly");
                break;
            }
            Err(e) => {
                return Err(format!("Read error: {}", e));
            }
        }

        let message_length = u32::from_be_bytes(length_buf) as usize;

        if message_length > 1024 * 1024 {
            error!("Pompe message too large: {} bytes", message_length);
            break;
        }

        if message_length == 0 {
            debug!("Received empty Pompe message");
            continue;
        }

        let mut message_buf = vec![0u8; message_length];
        socket
            .read_exact(&mut message_buf)
            .await
            .map_err(|e| format!("Failed to read message: {}", e))?;

        // Use bincode to stay aligned with the sender
        match bincode::deserialize::<PompeNetworkMessage>(&message_buf) {
            Ok(net_msg) => {
                // Deduplicate messages
                if processed_messages.contains(&net_msg.message_id) {
                    debug!("Skipped duplicate Pompe message: {}", net_msg.message_id);
                    continue;
                }
                processed_messages.insert(net_msg.message_id.clone());

                // Limit dedup cache size
                if processed_messages.len() > 1000 {
                    processed_messages.clear();
                }

                debug!(
                    "Received Pompe message: from node {}, kind: {:?}, ID: {}",
                    net_msg.from_node_id,
                    std::mem::discriminant(&net_msg.message),
                    &net_msg.message_id[0..8]
                );

                if let Err(e) = message_tx.send((net_msg.from_node_id, net_msg.message)) {
                    error!("Failed to enqueue Pompe message: {}", e);
                    break;
                }
            }
            Err(e) => {
                error!("Failed to deserialize Pompe message: {}", e);
            }
        }
    }

    Ok(())
}

impl Clone for PompeNetwork {
    fn clone(&self) -> Self {
        Self {
            node_id: self.node_id,
            pompe_port: self.pompe_port,
            peer_node_ids: self.peer_node_ids.clone(),
            message_tx: self.message_tx.clone(),
            message_rx: Arc::clone(&self.message_rx),
            connections: Arc::clone(&self.connections),
            sent_messages: Arc::clone(&self.sent_messages),
            rt: Arc::clone(&self.rt),
        }
    }
}
