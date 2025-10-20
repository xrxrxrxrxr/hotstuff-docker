//! SMROL network layer built on top of the Pompe networking primitives.
//! This keeps the connection-pool/backoff design while dispatching
//! incoming frames to PNFIFO / Sequencing / Consensus queues directly.

use crate::smrol::message::SmrolMessage;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::{Builder, Runtime};
use tokio::sync::mpsc as async_mpsc;
use tokio::sync::{Mutex as AsyncMutex, RwLock};
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
struct ConnectionState {
    writer: OwnedWriteHalf,
    last_used: Instant,
    send_count: usize,
}

#[derive(Debug)]
pub struct SmrolTcpNetwork {
    node_id: usize,
    listen_port: u16,
    peer_nodes: HashMap<usize, SocketAddr>,

    message_tx: async_mpsc::Sender<(usize, SmrolMessage)>,
    message_rx: Arc<AsyncMutex<async_mpsc::Receiver<(usize, SmrolMessage)>>>,

    connections: Arc<RwLock<HashMap<usize, Arc<AsyncMutex<ConnectionState>>>>>,
    sent_messages: Arc<Mutex<HashMap<String, u64>>>,

    rt: Arc<Runtime>,
}

impl SmrolTcpNetwork {
    pub fn new(node_id: usize, peer_nodes: HashMap<usize, SocketAddr>) -> Self {
        let listen_port = 21000 + node_id as u16;

        let (message_tx, message_rx) = async_mpsc::channel(Self::message_queue_capacity());

        let rt_threads: usize = std::env::var("SMROL_RT_THREADS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(2);
        let runtime = Arc::new(
            Builder::new_multi_thread()
                .worker_threads(rt_threads)
                .enable_all()
                .thread_name(&format!("smrol-net-{}", node_id))
                .build()
                .expect("Failed to build SMROL runtime"),
        );

        info!(
            "🌐 [SMROL] Node {} initialised network runtime with {} worker(s) on port {}",
            node_id, rt_threads, listen_port
        );

        let network = Self {
            node_id,
            listen_port,
            peer_nodes,
            message_tx,
            message_rx: Arc::new(AsyncMutex::new(message_rx)),
            connections: Arc::new(RwLock::new(HashMap::new())),
            sent_messages: Arc::new(Mutex::new(HashMap::new())),
            rt: runtime,
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
            .ok_or_else(|| format!("未知目标节点 {}", target_node_id))?;

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

        let network_msg = SmrolNetworkMessage {
            from_node_id: self.node_id,
            to_node_id: Some(target_node_id),
            message,
            timestamp: current_micros(),
            message_id,
        };

        if let Some(conn) = {
            let connections = self.connections.read().await;
            connections.get(&target_node_id).cloned()
        } {
            let mut guard = conn.lock().await;
            if let Err(e) = self
                .send_message_on_writer(&mut guard.writer, &network_msg)
                .await
            {
                warn!(
                    "⚠️ [SMROL] Node {} write to {} failed: {} (retrying)",
                    self.node_id, target_node_id, e
                );
                self.connections.write().await.remove(&target_node_id);
            } else {
                guard.last_used = Instant::now();
                guard.send_count += 1;
                return Ok(());
            }
        }

        let mut stream = TcpStream::connect(addr)
            .await
            .map_err(|e| format!("连接失败 {}: {}", addr, e))?;
        if let Err(e) = stream.set_nodelay(true) {
            warn!("⚠️ [SMROL] set_nodelay({}) failed: {}", addr, e);
        }
        let (_reader, writer) = stream.into_split();
        let mut state = ConnectionState {
            writer,
            last_used: Instant::now(),
            send_count: 0,
        };
        self.send_message_on_writer(&mut state.writer, &network_msg)
            .await?;
        self.connections
            .write()
            .await
            .insert(target_node_id, Arc::new(AsyncMutex::new(state)));

        Ok(())
    }

    pub async fn broadcast(&self, message: SmrolMessage) -> Result<(), String> {
        let _ = self.send_to_node(self.node_id, message.clone()).await;

        let mut handles = Vec::new();
        for (&nid, _) in &self.peer_nodes {
            if nid == self.node_id {
                continue;
            }
            let net = self.clone();
            let msg = message.clone();
            handles.push(tokio::spawn(async move {
                (nid, net.send_to_node(nid, msg).await)
            }));
        }

        let mut last_err: Option<String> = None;
        for handle in handles {
            match handle.await {
                Ok((nid, Ok(()))) => debug!("📤 [SMROL] broadcast delivered to {}", nid),
                Ok((nid, Err(e))) => {
                    warn!("⚠️ [SMROL] broadcast to {} failed: {}", nid, e);
                    last_err = Some(e);
                }
                Err(e) => {
                    warn!("⚠️ [SMROL] broadcast join error: {}", e);
                    last_err = Some(format!("join error: {}", e));
                }
            }
        }

        if let Some(err) = last_err {
            Err(err)
        } else {
            Ok(())
        }
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
                    for (&target, conn) in guard.iter() {
                        if conn.lock().await.last_used.elapsed() > Duration::from_secs(600) {
                            to_remove.push(target);
                        }
                    }
                    for target in to_remove {
                        guard.remove(&target);
                        info!(
                            "🧹 [SMROL] Node {} cleaned idle connection to {}",
                            node_id, target
                        );
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
            .await
            .map_err(|e| format!("消息入队失败: {}", e))
    }

    fn message_queue_capacity() -> usize {
        std::env::var("SMROL_NETWORK_QUEUE_CAP")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(4096)
    }

    async fn send_message_on_writer(
        &self,
        writer: &mut OwnedWriteHalf,
        network_msg: &SmrolNetworkMessage,
    ) -> Result<(), String> {
        let serialized =
            bincode::serialize(network_msg).map_err(|e| format!("序列化失败: {}", e))?;
        let len = serialized.len() as u32;
        writer
            .write_all(&len.to_be_bytes())
            .await
            .map_err(|e| format!("写入长度失败: {}", e))?;
        writer
            .write_all(&serialized)
            .await
            .map_err(|e| format!("写入消息失败: {}", e))
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
            return Err(format!("读取长度失败: {}", e));
        }
        let len = u32::from_be_bytes(length_buf) as usize;
        if len == 0 {
            debug!("💓 [SMROL] heartbeat received");
            continue;
        }
        if len > 10 * 1024 * 1024 {
            return Err(format!("消息过大: {} bytes", len));
        }

        let mut buf = vec![0u8; len];
        stream
            .read_exact(&mut buf)
            .await
            .map_err(|e| format!("读取消息失败: {}", e))?;

        let frame: SmrolNetworkMessage =
            bincode::deserialize(&buf).map_err(|e| format!("反序列化失败: {}", e))?;

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
