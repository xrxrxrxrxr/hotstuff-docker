// hotstuff_runner/src/tokio_network.rs
//! Tokio-based async TCP network implementation (length-prefixed framing)
//! Compatible with `hotstuff_rs::networking::network::Network` trait.

// use crate::tcp_network::TcpNetworkConfig as TokioNetworkConfig; // reuse existing config struct
use crate::affinity::{affinity_from_env, bind_current_thread};
use borsh::{BorshDeserialize, BorshSerialize};
use crossbeam::channel::{unbounded, Receiver, Sender};
use ed25519_dalek::VerifyingKey;
use futures::future::join_all;
use hotstuff_rs::block_sync::messages::BlockSyncMessage;
use hotstuff_rs::networking::messages::{Message, ProgressMessage};
use hotstuff_rs::networking::network::Network;
use hotstuff_rs::types::update_sets::ValidatorSetUpdates;
use hotstuff_rs::types::validator_set::ValidatorSet;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{tcp::OwnedWriteHalf, TcpListener, TcpSocket, TcpStream};
use tokio::runtime::{Builder, Runtime};
use tokio::sync::Mutex as AsyncMutex;
use tracing::{debug, error, info, warn};

// Simple frame header to resync reliably
const NET_MAGIC: u32 = 0x48534E57; // 'HSNW'
const MAX_MSG_SIZE: usize = 10 * 1024 * 1024; // 10MB

#[derive(Clone)]
pub struct TokioNetworkConfig {
    pub my_addr: SocketAddr,
    pub peer_addrs: HashMap<VerifyingKey, SocketAddr>,
    pub my_key: VerifyingKey,
}

// Message type tags (mirror of tcp_network.rs)
#[derive(Serialize, Deserialize, Clone, Debug)]
enum MessageType {
    Proposal,
    Vote,
    NewView,
    Timeout,
    TimeoutCertificate,
    HotStuff,
    Pacemaker,
    BlockSyncAdvertise,
    BlockSyncRequest,
    BlockSyncResponse,
}

#[derive(Serialize, Deserialize, Clone)]
struct NetworkMessage {
    from: Vec<u8>,
    message_type: MessageType,
    message_bytes: Vec<u8>,
}

fn message_to_bytes(
    message: &Message,
) -> Result<(MessageType, Vec<u8>), Box<dyn std::error::Error>> {
    let message_type = match message {
        Message::ProgressMessage(progress_msg) => match progress_msg {
            ProgressMessage::HotStuffMessage(_) => MessageType::HotStuff,
            ProgressMessage::PacemakerMessage(_) => MessageType::Pacemaker,
            ProgressMessage::BlockSyncAdvertiseMessage(_) => MessageType::BlockSyncAdvertise,
        },
        Message::BlockSyncMessage(sync_msg) => match sync_msg {
            BlockSyncMessage::BlockSyncRequest(_) => MessageType::BlockSyncRequest,
            BlockSyncMessage::BlockSyncResponse(_) => MessageType::BlockSyncResponse,
        },
    };
    let bytes = message.try_to_vec().map_err(|e| {
        error!("Failed to serialize message: {}", e);
        format!("Message serialization failed: {}", e)
    })?;
    Ok((message_type, bytes))
}

fn bytes_to_message(
    _message_type: MessageType,
    bytes: &[u8],
) -> Result<Message, Box<dyn std::error::Error>> {
    let message = Message::try_from_slice(bytes).map_err(|e| {
        error!("Failed to deserialize message: {}", e);
        format!("Message deserialization failed: {}", e)
    })?;
    Ok(message)
}

pub struct TokioNetwork {
    config: TokioNetworkConfig,
    message_rx: Receiver<(VerifyingKey, Message)>,
    message_tx: Sender<(VerifyingKey, Message)>,
    connections: Arc<RwLock<HashMap<VerifyingKey, Arc<AsyncMutex<ConnectionState>>>>>,
    rt: Arc<Runtime>,
}

#[derive(Debug)]
struct ConnectionState {
    writer: tokio::net::tcp::OwnedWriteHalf,
    last_used: std::time::Instant,
    send_count: usize,
}

impl TokioNetwork {
    pub fn new(config: TokioNetworkConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let affinity_label = "tokio_network_rt".to_string();
        let affinity = affinity_from_env("TOKIO_NETWORK_RT_CORES", &affinity_label).map(Arc::new);
        let default_threads = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(2);
        let requested_threads: usize = std::env::var("TOKIO_NETWORK_RT_THREADS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(default_threads);
        let rt_threads = match affinity.as_ref() {
            Some(cores) => {
                if cores.len() != requested_threads {
                    info!(
                        "[affinity] tokio network: overriding worker count {} -> {} to match core set",
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
            .thread_name("hs-tokio-net");

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

        let rt = Arc::new(builder.build()?);
        let (tx, rx) = unbounded();

        let this = Self {
            config: config.clone(),
            message_rx: rx,
            message_tx: tx,
            connections: Arc::new(RwLock::new(HashMap::new())),
            rt: rt.clone(),
        };

        // spawn server
        let server_cfg = config.clone();
        let server_tx = this.message_tx.clone();
        rt.spawn(async move {
            if let Err(e) = run_server(server_cfg, server_tx).await {
                error!("Tokio TCP server error: {}", e);
            }
        });

        // give listener a brief moment
        std::thread::sleep(Duration::from_millis(200));

        this.start_connection_maintenance();
        Ok(this)
    }

    fn dispatch_local(&self, sender: VerifyingKey, message: Message) {
        if let Err(e) = self.message_tx.send((sender, message)) {
            error!("Local message dispatch failed: {}", e);
        }
    }

    fn start_connection_maintenance(&self) {
        let connections = Arc::clone(&self.connections);
        let my_bytes = self.config.my_key.to_bytes();
        self.rt.spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(60));
            loop {
                ticker.tick().await;
                let snapshot = {
                    let guard = connections.read();
                    guard
                        .iter()
                        .map(|(peer, state)| (*peer, Arc::clone(state)))
                        .collect::<Vec<_>>()
                };

                let mut stale = Vec::new();
                for (peer, state) in snapshot {
                    if state.lock().await.last_used.elapsed() > Duration::from_secs(600) {
                        stale.push(peer);
                    }
                }

                if !stale.is_empty() {
                    let mut guard = connections.write();
                    for peer in stale {
                        guard.remove(&peer);
                        debug!(
                            "🧹 [TokioNetwork] {:?} cleared idle connection to {:?}",
                            &my_bytes[0..4],
                            &peer.to_bytes()[0..4]
                        );
                    }
                }
            }
        });
    }

    fn frame_for_message(&self, message: &Message) -> Result<Vec<u8>, String> {
        let (msg_type, msg_bytes) = message_to_bytes(message)
            .map_err(|e| format!("Message serialization failed: {}", e))?;
        let net_msg = NetworkMessage {
            from: self.config.my_key.to_bytes().to_vec(),
            message_type: msg_type,
            message_bytes: msg_bytes,
        };
        let serialized = bincode::serialize(&net_msg)
            .map_err(|e| format!("bincode serialization failed: {}", e))?;
        let len = serialized.len() as u32;
        let mut frame = Vec::with_capacity(8 + serialized.len());
        frame.extend_from_slice(&NET_MAGIC.to_be_bytes());
        frame.extend_from_slice(&len.to_be_bytes());
        frame.extend_from_slice(&serialized);
        Ok(frame)
    }

    async fn write_frame(writer: &mut OwnedWriteHalf, frame: &[u8]) -> Result<(), String> {
        writer
            .write_all(frame)
            .await
            .map_err(|e| format!("Failed to write TCP frame: {}", e))
    }

    async fn connect_and_send(&self, peer: VerifyingKey, frame: Vec<u8>) -> Result<(), String> {
        let addr = *self
            .config
            .peer_addrs
            .get(&peer)
            .ok_or_else(|| format!("Unknown node: {:?}", &peer.to_bytes()[0..4]))?;

        let stream = TcpStream::connect(addr)
            .await
            .map_err(|e| format!("Failed to connect to {}: {}", addr, e))?;
        stream
            .set_nodelay(true)
            .map_err(|e| format!("Failed to set TCP_NODELAY: {}", e))?;
        let (_reader, writer) = stream.into_split();

        let state = Arc::new(AsyncMutex::new(ConnectionState {
            writer,
            last_used: std::time::Instant::now(),
            send_count: 0,
        }));

        {
            let mut guard = state.lock().await;
            Self::write_frame(&mut guard.writer, &frame).await?;
            guard.last_used = std::time::Instant::now();
            guard.send_count += 1;
        }

        self.connections.write().insert(peer, state);
        Ok(())
    }

    async fn send_frame_to_peer_async(
        &self,
        peer: VerifyingKey,
        frame: Vec<u8>,
    ) -> Result<(), String> {
        if peer == self.config.my_key {
            return Ok(());
        }

        if let Some(conn) = self.connections.read().get(&peer).cloned() {
            let mut guard = conn.lock().await;
            if let Err(e) = Self::write_frame(&mut guard.writer, &frame).await {
                drop(guard);
                self.connections.write().remove(&peer);
                return self.connect_and_send(peer, frame).await;
            }
            guard.last_used = std::time::Instant::now();
            guard.send_count += 1;
            return Ok(());
        }

        self.connect_and_send(peer, frame).await
    }
}

async fn run_server(
    config: TokioNetworkConfig,
    message_tx: Sender<(VerifyingKey, Message)>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Bind with reuse options for Docker deployments
    let listener = bind_listener(config.my_addr)?;
    info!("Tokio TCP server listening on {}", config.my_addr);

    loop {
        let (stream, peer) = listener.accept().await?;
        let tx = message_tx.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, tx).await {
                error!("Error handling client connection from {}: {}", peer, e);
            }
        });
    }
}

fn bind_listener(addr: SocketAddr) -> Result<TcpListener, Box<dyn std::error::Error>> {
    // Use TcpSocket to set reuse flags on Unix
    let socket = match addr {
        SocketAddr::V4(_) => TcpSocket::new_v4()?,
        SocketAddr::V6(_) => TcpSocket::new_v6()?,
    };
    #[cfg(unix)]
    {
        socket.set_reuseaddr(true)?;
        socket.set_reuseport(true)?;
    }
    socket.bind(addr)?;
    Ok(socket.listen(1024)?)
}

async fn handle_connection(
    mut stream: TcpStream,
    message_tx: Sender<(VerifyingKey, Message)>,
) -> Result<(), Box<dyn std::error::Error>> {
    stream.set_nodelay(true)?;
    let peer_addr = stream.peer_addr()?;
    debug!("New connection from: {}", peer_addr);

    loop {
        let mut magic_or_len = [0u8; 4];
        if let Err(e) = stream.read_exact(&mut magic_or_len).await {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                debug!("Connection closed cleanly: {}", peer_addr);
                break;
            }
            error!("Failed to read frame header from {}: {}", peer_addr, e);
            break;
        }

        let magic = u32::from_be_bytes(magic_or_len);
        let length: usize = if magic == NET_MAGIC {
            let mut length_buf = [0u8; 4];
            if let Err(e) = stream.read_exact(&mut length_buf).await {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    debug!("Connection closed while reading length: {}", peer_addr);
                    break;
                }
                error!("Failed to read length from {}: {}", peer_addr, e);
                break;
            }
            u32::from_be_bytes(length_buf) as usize
        } else {
            u32::from_be_bytes(magic_or_len) as usize
        };

        if length == 0 {
            continue; // ping frame
        }
        if length > MAX_MSG_SIZE {
            warn!(
                "Received abnormal length {} from {}; dropping connection",
                length, peer_addr
            );
            break;
        }

        let mut buf = vec![0u8; length];
        if let Err(e) = stream.read_exact(&mut buf).await {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                debug!("Connection closed while reading message: {}", peer_addr);
                break;
            }
            error!("Failed to read message body from {}: {}", peer_addr, e);
            break;
        }

        match bincode::deserialize::<NetworkMessage>(&buf) {
            Ok(net_msg) => {
                let sender_key: VerifyingKey = match net_msg.from.try_into() {
                    Ok(bytes_array) => match VerifyingKey::from_bytes(&bytes_array) {
                        Ok(key) => key,
                        Err(_) => {
                            error!("Failed to construct VerifyingKey from bytes");
                            continue;
                        }
                    },
                    Err(_) => {
                        error!("Byte array length incorrect");
                        continue;
                    }
                };
                match bytes_to_message(net_msg.message_type, &net_msg.message_bytes) {
                    Ok(hs_msg) => {
                        if let Err(e) = message_tx.send((sender_key, hs_msg)) {
                            error!("Failed to send message to queue: {}", e);
                            break;
                        }
                    }
                    Err(e) => error!("Failed to deserialize HotStuff message: {}", e),
                }
            }
            Err(e) => error!(
                "Failed to deserialize network message from {}: {}",
                peer_addr, e
            ),
        }
    }

    Ok(())
}

impl Clone for TokioNetwork {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            message_rx: self.message_rx.clone(),
            message_tx: self.message_tx.clone(),
            connections: self.connections.clone(),
            rt: self.rt.clone(),
        }
    }
}

impl Network for TokioNetwork {
    fn init_validator_set(&mut self, validator_set: ValidatorSet) {
        info!(
            "Tokio TCP node {:?} initialized validator set with {} validators",
            self.config.my_key.to_bytes()[0..4].to_vec(),
            validator_set.len()
        );
    }

    fn update_validator_set(&mut self, _updates: ValidatorSetUpdates) {
        info!(
            "Tokio TCP node {:?} updated validator set",
            self.config.my_key.to_bytes()[0..4].to_vec()
        );
        // Could dynamically add/remove writer tasks if needed.
    }

    fn broadcast(&mut self, message: Message) {
        // serialize once per peer (keeps format identical to tcp_network)
        self.dispatch_local(self.config.my_key, message.clone());

        let frame = match self.frame_for_message(&message) {
            Ok(f) => f,
            Err(e) => {
                error!("Broadcast serialization failed: {}", e);
                return;
            }
        };

        let futures = self
            .config
            .peer_addrs
            .keys()
            .filter(|peer| **peer != self.config.my_key)
            .map(|peer| {
                let frame = frame.clone();
                let net = self.clone();
                let peer_key = *peer;
                async move {
                    if let Err(e) = net.send_frame_to_peer_async(peer_key, frame).await {
                        Err((peer_key, e))
                    } else {
                        Ok(())
                    }
                }
            })
            .collect::<Vec<_>>();

        if !futures.is_empty() {
            let results = self.rt.block_on(async { join_all(futures).await });
            for res in results {
                if let Err((peer, e)) = res {
                    error!(
                        "Broadcast send failed to {:?}: {}",
                        peer.to_bytes()[0..4].to_vec(),
                        e
                    );
                }
            }
        }
    }

    fn send(&mut self, peer: VerifyingKey, message: Message) {
        if peer == self.config.my_key {
            self.dispatch_local(self.config.my_key, message);
            return;
        }

        let frame = match self.frame_for_message(&message) {
            Ok(f) => f,
            Err(e) => {
                error!("Send serialization failed: {}", e);
                return;
            }
        };

        let net = self.clone();
        let result = self
            .rt
            .block_on(async { net.send_frame_to_peer_async(peer, frame).await });
        if let Err(e) = result {
            error!("Send failed to {:?}: {}", peer.to_bytes()[0..4].to_vec(), e);
        }
    }

    fn recv(&mut self) -> Option<(VerifyingKey, Message)> {
        match self.message_rx.try_recv() {
            Ok(msg) => Some(msg),
            Err(crossbeam::channel::TryRecvError::Empty) => None,
            Err(crossbeam::channel::TryRecvError::Disconnected) => {
                error!("Receive channel has been closed");
                None
            }
        }
    }
}
