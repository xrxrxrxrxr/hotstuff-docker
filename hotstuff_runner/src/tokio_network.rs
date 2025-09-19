// hotstuff_runner/src/tokio_network.rs
//! Tokio-based async TCP network implementation (length-prefixed framing)
//! Compatible with `hotstuff_rs::networking::network::Network` trait.

// use crate::tcp_network::TcpNetworkConfig as TokioNetworkConfig; // reuse existing config struct
use crossbeam::channel::{unbounded, Receiver, Sender};
use ed25519_dalek::VerifyingKey;
use borsh::{BorshSerialize, BorshDeserialize};
use hotstuff_rs::block_sync::messages::BlockSyncMessage;
use hotstuff_rs::networking::messages::{Message, ProgressMessage};
use hotstuff_rs::networking::network::Network;
use hotstuff_rs::types::update_sets::ValidatorSetUpdates;
use hotstuff_rs::types::validator_set::ValidatorSet;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::runtime::{Builder, Runtime};
use tokio::sync::mpsc;
use tokio::time::{interval, sleep};
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

fn message_to_bytes(message: &Message) -> Result<(MessageType, Vec<u8>), Box<dyn std::error::Error>> {
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
        error!("序列化消息失败: {}", e);
        format!("Message serialization failed: {}", e)
    })?;
    Ok((message_type, bytes))
}

fn bytes_to_message(_message_type: MessageType, bytes: &[u8]) -> Result<Message, Box<dyn std::error::Error>> {
    let message = Message::try_from_slice(bytes).map_err(|e| {
        error!("反序列化消息失败: {}", e);
        format!("Message deserialization failed: {}", e)
    })?;
    Ok(message)
}

#[derive(Clone)]
struct PeerWriter {
    tx: mpsc::Sender<Vec<u8>>, // bounded async channel for frames
}

pub struct TokioNetwork {
    config: TokioNetworkConfig,
    message_rx: Receiver<(VerifyingKey, Message)>,
    message_tx: Sender<(VerifyingKey, Message)>,
    writer_queues: Arc<RwLock<HashMap<VerifyingKey, PeerWriter>>>,
    rt: Arc<Runtime>,
}

impl TokioNetwork {
    pub fn new(config: TokioNetworkConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let rt = Arc::new(
            Builder::new_multi_thread()
                .enable_all()
                .thread_name("hs-tokio-net")
                .build()?,
        );
        let (tx, rx) = unbounded();

        let mut this = Self {
            config: config.clone(),
            message_rx: rx,
            message_tx: tx,
            writer_queues: Arc::new(RwLock::new(HashMap::new())),
            rt: rt.clone(),
        };

        // spawn server
        let server_cfg = config.clone();
        let server_tx = this.message_tx.clone();
        rt.spawn(async move {
            if let Err(e) = run_server(server_cfg, server_tx).await {
                error!("Tokio TCP服务器错误: {}", e);
            }
        });

        // give listener a brief moment
        std::thread::sleep(Duration::from_millis(200));

        this.spawn_peer_writers();
        Ok(this)
    }

    fn spawn_peer_writers(&mut self) {
        // bounded queue capacity via env
        let cap: usize = std::env::var("HS_PEER_QUEUE_CAP")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(16384);

        for (peer_key, addr) in &self.config.peer_addrs {
            if *peer_key == self.config.my_key {
                continue;
            }
            let (tx, rx) = mpsc::channel::<Vec<u8>>(cap);
            self.writer_queues
                .write()
                .insert(*peer_key, PeerWriter { tx: tx.clone() });

            let peer = *peer_key;
            let peer_addr = *addr;
            self.rt.spawn(async move {
                peer_writer_task(peer, peer_addr, rx).await;
            });
        }
    }

    fn send_frame_to_peer(&self, peer_key: &VerifyingKey, frame: Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
        if *peer_key == self.config.my_key {
            // loopback: decode and deliver
            if let Ok(net_msg) = bincode::deserialize::<NetworkMessage>(&frame[8..]) {
                if let Ok(message) = bytes_to_message(net_msg.message_type, &net_msg.message_bytes) {
                    let _ = self.message_tx.send((self.config.my_key, message));
                }
            }
            return Ok(());
        }

        if let Some(writer) = self.writer_queues.read().get(peer_key).cloned() {
            // try fast path then block if necessary to preserve consensus messages
            if let Err(e) = writer.tx.try_send(frame) {
                match e {
                    mpsc::error::TrySendError::Full(_f) => {
                        // 不要阻塞 runtime 线程，直接丢弃该帧（可调大 HS_PEER_QUEUE_CAP 以减少丢弃）。
                        warn!("peer {:?} 发送队列满，丢弃一帧以避免阻塞", peer_key.to_bytes()[0..4].to_vec());
                    }
                    mpsc::error::TrySendError::Closed(_) => {
                        error!("peer {:?} 发送队列已关闭", peer_key.to_bytes()[0..4].to_vec());
                    }
                }
            }
            Ok(())
        } else {
            warn!("peer {:?} 无 writer 队列", peer_key.to_bytes()[0..4].to_vec());
            Err("无 writer 队列".into())
        }
    }
}

async fn run_server(
    config: TokioNetworkConfig,
    message_tx: Sender<(VerifyingKey, Message)>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Bind with reuse options for Docker deployments
    let listener = bind_listener(config.my_addr)?;
    info!("Tokio TCP服务器监听: {}", config.my_addr);

    loop {
        let (stream, peer) = listener.accept().await?;
        let tx = message_tx.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, tx).await {
                error!("处理客户端连接错误 from {}: {}", peer, e);
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
    debug!("新连接来自: {}", peer_addr);

    loop {
        let mut magic_or_len = [0u8; 4];
        if let Err(e) = stream.read_exact(&mut magic_or_len).await {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                debug!("连接正常关闭: {}", peer_addr);
                break;
            }
            error!("读取帧头失败 from {}: {}", peer_addr, e);
            break;
        }

        let magic = u32::from_be_bytes(magic_or_len);
        let length: usize = if magic == NET_MAGIC {
            let mut length_buf = [0u8; 4];
            if let Err(e) = stream.read_exact(&mut length_buf).await {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    debug!("连接在读取长度时关闭: {}", peer_addr);
                    break;
                }
                error!("读取长度失败 from {}: {}", peer_addr, e);
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
            warn!("收到异常长度 {} from {}，丢弃连接", length, peer_addr);
            break;
        }

        let mut buf = vec![0u8; length];
        if let Err(e) = stream.read_exact(&mut buf).await {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                debug!("连接在读取消息时关闭: {}", peer_addr);
                break;
            }
            error!("读取消息内容失败 from {}: {}", peer_addr, e);
            break;
        }

        match bincode::deserialize::<NetworkMessage>(&buf) {
            Ok(net_msg) => {
                let sender_key: VerifyingKey = match net_msg.from.try_into() {
                    Ok(bytes_array) => match VerifyingKey::from_bytes(&bytes_array) {
                        Ok(key) => key,
                        Err(_) => {
                            error!("无法从字节构造 VerifyingKey");
                            continue;
                        }
                    },
                    Err(_) => {
                        error!("字节数组长度不正确");
                        continue;
                    }
                };
                match bytes_to_message(net_msg.message_type, &net_msg.message_bytes) {
                    Ok(hs_msg) => {
                        if let Err(e) = message_tx.send((sender_key, hs_msg)) {
                            error!("发送消息到队列失败: {}", e);
                            break;
                        }
                    }
                    Err(e) => error!("反序列化 HotStuff 消息失败: {}", e),
                }
            }
            Err(e) => error!("反序列化网络消息失败 from {}: {}", peer_addr, e),
        }
    }

    Ok(())
}

async fn peer_writer_task(peer: VerifyingKey, addr: SocketAddr, mut rx: mpsc::Receiver<Vec<u8>>) {
    let mut ping = interval(Duration::from_secs(10));
    let mut backoff = Duration::from_millis(100);

    loop {
        // connect with retry/backoff
        let mut stream = match TcpStream::connect(addr).await {
            Ok(s) => {
                let _ = s.set_nodelay(true);
                backoff = Duration::from_millis(100);
                debug!("peer {:?} 已连接 {}", peer.to_bytes()[0..4].to_vec(), addr);
                s
            }
            Err(e) => {
                warn!("peer {:?} 连接失败 {}: {}，等待 {:?}", peer.to_bytes()[0..4].to_vec(), addr, e, backoff);
                sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(3));
                continue;
            }
        };

        loop {
            tokio::select! {
                biased;
                _ = ping.tick() => {
                    // send ping frame (magic + length 0)
                    if let Err(e) = stream.write_all(&NET_MAGIC.to_be_bytes()).await.or_else(|_| Err(std::io::Error::new(std::io::ErrorKind::Other, "err"))) {
                        warn!("peer {:?} ping 写失败: {}", peer.to_bytes()[0..4].to_vec(), e);
                        break; // reconnect
                    }
                    if let Err(e) = stream.write_all(&0u32.to_be_bytes()).await {
                        warn!("peer {:?} ping 写失败: {}", peer.to_bytes()[0..4].to_vec(), e);
                        break;
                    }
                }
                maybe = rx.recv() => {
                    match maybe {
                        Some(frame) => {
                            if let Err(e) = stream.write_all(&frame).await {
                                warn!("peer {:?} 写失败: {}，重连", peer.to_bytes()[0..4].to_vec(), e);
                                break; // reconnect and retry later
                            }
                        }
                        None => {
                            debug!("peer {:?} 发送端关闭", peer.to_bytes()[0..4].to_vec());
                            return;
                        }
                    }
                }
            }
        }
    }
}

impl Clone for TokioNetwork {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            message_rx: self.message_rx.clone(),
            message_tx: self.message_tx.clone(),
            writer_queues: self.writer_queues.clone(),
            rt: self.rt.clone(),
        }
    }
}

impl Network for TokioNetwork {
    fn init_validator_set(&mut self, validator_set: ValidatorSet) {
        info!(
            "Tokio TCP节点 {:?} 初始化验证者集合: {} 个验证者",
            self.config.my_key.to_bytes()[0..4].to_vec(),
            validator_set.len()
        );
    }

    fn update_validator_set(&mut self, _updates: ValidatorSetUpdates) {
        info!(
            "Tokio TCP节点 {:?} 更新验证者集合",
            self.config.my_key.to_bytes()[0..4].to_vec()
        );
        // Could dynamically add/remove writer tasks if needed.
    }

    fn broadcast(&mut self, message: Message) {
        // serialize once per peer (keeps format identical to tcp_network)
        let (msg_type, msg_bytes) = match message_to_bytes(&message) {
            Ok(v) => v,
            Err(e) => {
                error!("广播序列化失败: {}", e);
                return;
            }
        };
        let net_msg = NetworkMessage {
            from: self.config.my_key.to_bytes().to_vec(),
            message_type: msg_type,
            message_bytes: msg_bytes,
        };
        let serialized = match bincode::serialize(&net_msg) {
            Ok(b) => b,
            Err(e) => {
                error!("广播 bincode 失败: {}", e);
                return;
            }
        };
        let len = serialized.len() as u32;
        let mut frame = Vec::with_capacity(8 + serialized.len());
        frame.extend_from_slice(&NET_MAGIC.to_be_bytes());
        frame.extend_from_slice(&len.to_be_bytes());
        frame.extend_from_slice(&serialized);

        for peer_key in self.config.peer_addrs.keys() {
            if let Err(e) = self.send_frame_to_peer(peer_key, frame.clone()) {
                error!("广播发送失败给 {:?}: {}", peer_key.to_bytes()[0..4].to_vec(), e);
            }
        }
    }

    fn send(&mut self, peer: VerifyingKey, message: Message) {
        let (msg_type, msg_bytes) = match message_to_bytes(&message) {
            Ok(v) => v,
            Err(e) => {
                error!("发送序列化失败: {}", e);
                return;
            }
        };
        let net_msg = NetworkMessage {
            from: self.config.my_key.to_bytes().to_vec(),
            message_type: msg_type,
            message_bytes: msg_bytes,
        };
        let serialized = match bincode::serialize(&net_msg) {
            Ok(b) => b,
            Err(e) => {
                error!("发送 bincode 失败: {}", e);
                return;
            }
        };
        let len = serialized.len() as u32;
        let mut frame = Vec::with_capacity(8 + serialized.len());
        frame.extend_from_slice(&NET_MAGIC.to_be_bytes());
        frame.extend_from_slice(&len.to_be_bytes());
        frame.extend_from_slice(&serialized);

        if let Err(e) = self.send_frame_to_peer(&peer, frame) {
            error!("发送失败给 {:?}: {}", peer.to_bytes()[0..4].to_vec(), e);
        }
    }

    fn recv(&mut self) -> Option<(VerifyingKey, Message)> {
        match self.message_rx.try_recv() {
            Ok(msg) => Some(msg),
            Err(crossbeam::channel::TryRecvError::Empty) => None,
            Err(crossbeam::channel::TryRecvError::Disconnected) => {
                error!("接收通道已断开");
                None
            }
        }
    }
}
