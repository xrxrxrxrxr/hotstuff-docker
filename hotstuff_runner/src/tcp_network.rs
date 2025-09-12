// hotstuff_runner/src/tcp_network.rs
//! 基于TCP的真实网络实现，用于Docker多进程部署 - 无锁版本

use hotstuff_rs::{
    networking::{
        network::Network,
        messages::{Message, ProgressMessage},
    },
    types::{
        validator_set::ValidatorSet,
        update_sets::ValidatorSetUpdates,
        crypto_primitives::VerifyingKey,
    },
};
use std::sync::{Arc, mpsc};
use std::collections::HashMap;
use std::net::{TcpListener, TcpStream, SocketAddr};
use std::io::{Read, Write};
use std::thread;
use std::time::Duration;
use tracing::{debug, info, error, warn};
use serde::{Serialize, Deserialize};
use borsh::{BorshSerialize, BorshDeserialize};
use hotstuff_rs::block_sync::messages::BlockSyncMessage;
use crossbeam::channel::{unbounded, Receiver, Sender};
use parking_lot::RwLock; // 更高效的读写锁

// 简单的帧同步头，避免长度错位造成“超大消息”误判
const NET_MAGIC: u32 = 0x48534E57; // 'HSNW'
const MAX_MSG_SIZE: usize = 10 * 1024 * 1024; // 10MB

// 每个 peer 的异步发送队列
#[derive(Clone)]
struct PeerWriter {
    tx: crossbeam::channel::Sender<Vec<u8>>, // 有界队列
}

// 定义消息类型枚举
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

// 网络消息包装器
#[derive(Serialize, Deserialize, Clone)]
struct NetworkMessage {
    from: Vec<u8>,
    message_type: MessageType,
    message_bytes: Vec<u8>,
}

// TCP网络配置
#[derive(Clone)]
pub struct TcpNetworkConfig {
    pub my_addr: SocketAddr,
    pub peer_addrs: HashMap<VerifyingKey, SocketAddr>,
    pub my_key: VerifyingKey,
}

// 连接管理器 - 使用无锁数据结构
struct ConnectionManager {
    connections: Arc<RwLock<HashMap<VerifyingKey, TcpStream>>>,
    config: TcpNetworkConfig,
}

impl ConnectionManager {
    fn new(config: TcpNetworkConfig) -> Self {
        Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    fn get_or_create_connection(&self, peer_key: &VerifyingKey) -> Option<TcpStream> {
        // 先尝试读锁获取现有连接
        {
            let connections = self.connections.read();
            if let Some(existing_connection) = connections.get(peer_key).and_then(|s| s.try_clone().ok()) {
                return Some(existing_connection);
            }
        }

        // 如果没有连接或连接失效，创建新连接
        if let Some(peer_addr) = self.config.peer_addrs.get(peer_key) {
            match TcpStream::connect(peer_addr) {
                Ok(stream) => {
                    // 降低小包延迟并设置合理超时
                    let _ = stream.set_nodelay(true);
                    let _ = stream.set_read_timeout(Some(Duration::from_secs(10)));
                    let _ = stream.set_write_timeout(Some(Duration::from_secs(10)));
                    if let Ok(cloned) = stream.try_clone() {
                        // 使用写锁更新连接
                        let mut connections = self.connections.write();
                        connections.insert(*peer_key, stream);
                        return Some(cloned);
                    }
                }
                Err(e) => {
                    error!("连接失败 {}: {}", peer_addr, e);
                }
            }
        }
        None
    }

    // fn get_or_create_connection(&self, peer_key: &VerifyingKey) -> Option<TcpStream> {
    //     // 1. 先检查现有连接的健康状态
    //     {
    //         let connections = self.connections.read();
    //         if let Some(existing_connection) = connections.get(peer_key) {
    //             if let Ok(cloned) = existing_connection.try_clone() {
    //                 // 关键：检查连接是否还活着
    //                 if self.is_connection_alive(&cloned) {
    //                     return Some(cloned);
    //                 }
    //             }
    //         }
    //     }

    //     // 2. 清理失效连接
    //     {
    //         let mut connections = self.connections.write();
    //         connections.remove(peer_key);
    //     }

    //     // 3. 创建新连接，带重试
    //     if let Some(peer_addr) = self.config.peer_addrs.get(peer_key) {
    //         for attempt in 1..=3 {
    //             match TcpStream::connect(peer_addr) {
    //                 Ok(stream) => {
    //                     // 4. 设置TCP选项
    //                     let _ = stream.set_nodelay(true);
    //                     let _ = stream.set_read_timeout(Some(Duration::from_secs(10)));
    //                     let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));
                        
    //                     if let Ok(cloned) = stream.try_clone() {
    //                         let mut connections = self.connections.write();
    //                         connections.insert(*peer_key, stream);
    //                         return Some(cloned);
    //                     }
    //                 }
    //                 Err(e) => {
    //                     if attempt < 3 {
    //                         std::thread::sleep(Duration::from_millis(100));
    //                     } else {
    //                         error!("连接失败 {} (所有 {} 次尝试): {}", peer_addr, attempt, e);
    //                     }
    //                 }
    //             }
    //         }
    //     }
    //     None
    // }
    
    pub fn cleanup_stale_connections(&self) {
        let mut connections = self.connections.write();
        let initial_count = connections.len();
        
        connections.retain(|_, stream| {
            stream.peer_addr().is_ok() // 保留有效连接
        });
        
        let cleaned_count = initial_count - connections.len();
        if cleaned_count > 0 {
            info!("清理了 {} 个失效连接", cleaned_count);
        }
    }

    // 添加连接健康检查
    fn is_connection_alive(&self, stream: &TcpStream) -> bool {
        match stream.take_error() {
            Ok(Some(_)) => false, // 有错误
            Ok(None) => true,     // 无错误
            Err(_) => false,      // 检查失败
        }
    }
}

// TCP网络实现 - 使用无锁通道
pub struct TcpNetwork {
    config: TcpNetworkConfig,
    message_rx: Receiver<(VerifyingKey, Message)>,
    message_tx: Sender<(VerifyingKey, Message)>,
    connection_manager: Arc<ConnectionManager>,
    _server_handle: thread::JoinHandle<()>,
    writer_queues: Arc<RwLock<HashMap<VerifyingKey, PeerWriter>>>,
}

impl TcpNetwork {
    pub fn new(config: TcpNetworkConfig) -> Result<Self, Box<dyn std::error::Error>> {
        // 使用 crossbeam 的无锁通道
        let (tx, rx) = unbounded();
        
        let connection_manager = Arc::new(ConnectionManager::new(config.clone()));
        
        // 添加定期清理任务
        let cleanup_manager = connection_manager.clone();
        thread::spawn(move || {
            loop {
                thread::sleep(Duration::from_secs(30));
                cleanup_manager.cleanup_stale_connections();
            }
        });

        // 启动TCP服务器
        let server_config = config.clone();
        let server_tx = tx.clone();
        let server_handle = thread::spawn(move || {
            if let Err(e) = run_tcp_server(server_config, server_tx) {
                error!("TCP服务器错误: {}", e);
            }
        });

        // 等待服务器启动
        thread::sleep(std::time::Duration::from_millis(500));

        // 初始化连接
        let mut network = Self {
            config: config.clone(),
            message_rx: rx,
            message_tx: tx,
            connection_manager,
            _server_handle: server_handle,
            writer_queues: Arc::new(RwLock::new(HashMap::new())),
        };

        network.connect_to_peers()?;
        network.spawn_peer_writers();
        
        Ok(network)
    }

    fn spawn_peer_writers(&mut self) {
        // 发送队列容量
        let cap: usize = std::env::var("HS_PEER_QUEUE_CAP").ok().and_then(|s| s.parse().ok()).unwrap_or(1024);
        for (peer_key, _addr) in &self.config.peer_addrs {
            if *peer_key == self.config.my_key { continue; }
            let (tx, rx) = crossbeam::channel::bounded::<Vec<u8>>(cap);
            self.writer_queues.write().insert(*peer_key, PeerWriter { tx });

            let peer = *peer_key;
            let cm = self.connection_manager.clone();
            thread::spawn(move || {
                // 简单 ping 周期
                let ping_interval = std::time::Duration::from_secs(10);
                let mut last_ping = std::time::Instant::now();
                loop {
                    // 定期发送空帧作为 ping
                    if last_ping.elapsed() >= ping_interval {
                        if let Some(mut s) = cm.get_or_create_connection(&peer) {
                            let _ = s.write_all(&NET_MAGIC.to_be_bytes());
                            let _ = s.write_all(&0u32.to_be_bytes()); // 长度0
                            let _ = s.flush();
                        }
                        last_ping = std::time::Instant::now();
                    }

                    match rx.recv_timeout(std::time::Duration::from_millis(50)) {
                        Ok(buf) => {
                            // 写入，失败则重连再写一次
                            let mut try_write = |stream: &mut TcpStream, data: &[u8]| -> std::io::Result<()> {
                                stream.write_all(data)?; stream.flush()?; Ok(())
                            };
                            if let Some(mut s) = cm.get_or_create_connection(&peer) {
                                if let Err(e) = try_write(&mut s, &buf) {
                                    warn!("peer {:?} 写失败: {}，重连重试", &peer.to_bytes()[0..4].to_vec(), e);
                                    drop(s);
                                    if let Some(mut s2) = cm.get_or_create_connection(&peer) {
                                        if let Err(e2) = try_write(&mut s2, &buf) {
                                            error!("peer {:?} 写重试失败: {}", &peer.to_bytes()[0..4].to_vec(), e2);
                                        }
                                    }
                                }
                            }
                        }
                        Err(crossbeam::channel::RecvTimeoutError::Timeout) => { /* loop ping */ }
                        Err(crossbeam::channel::RecvTimeoutError::Disconnected) => break,
                    }
                }
            });
        }
    }

    fn connect_to_peers(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        for (peer_key, peer_addr) in &self.config.peer_addrs {
            if *peer_key == self.config.my_key {
                continue;
            }

            info!("尝试连接对等节点: {:?} -> {}", 
                  peer_key.to_bytes()[0..4].to_vec(), peer_addr);
            
            // 使用连接管理器建立初始连接
            if self.connection_manager.get_or_create_connection(peer_key).is_some() {
                info!("成功连接到对等节点: {}", peer_addr);
            } else {
                warn!("初始连接失败: {}", peer_addr);
            }
        }
        
        Ok(())
    }

    fn message_to_bytes(message: &Message) -> Result<(MessageType, Vec<u8>), Box<dyn std::error::Error>> {
        let message_type = match message {
            Message::ProgressMessage(progress_msg) => {
                match progress_msg {
                    ProgressMessage::HotStuffMessage(_) => MessageType::HotStuff,
                    ProgressMessage::PacemakerMessage(_) => MessageType::Pacemaker,
                    ProgressMessage::BlockSyncAdvertiseMessage(_) => MessageType::BlockSyncAdvertise,
                }
            }
            Message::BlockSyncMessage(sync_msg) => {
                match sync_msg {
                    BlockSyncMessage::BlockSyncRequest(_) => MessageType::BlockSyncRequest,
                    BlockSyncMessage::BlockSyncResponse(_) => MessageType::BlockSyncResponse,
                }
            }
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

    pub fn get_connection_stats(&self) -> (usize, usize) {
        let connections = self.connection_manager.connections.read();
        let active_count = connections.len();
        let total_attempts = connections.values().len(); // 简化统计
        
        if active_count > 20 {
            warn!("连接池过大: {} 个活跃连接", active_count);
        }
        
        (active_count, total_attempts)
    }


    fn send_to_peer(&self, peer_key: &VerifyingKey, message: &Message) -> Result<(), Box<dyn std::error::Error>> {
        // 发送给自己的消息直接放入通道
        if *peer_key == self.config.my_key {
            self.message_tx.send((self.config.my_key, message.clone()))?;
            return Ok(());
        }

        // 构造帧并入 per-peer writer 队列
        let (message_type, message_bytes) = Self::message_to_bytes(message)?;
        let net_msg = NetworkMessage {
            from: self.config.my_key.to_bytes().to_vec(),
            message_type,
            message_bytes,
        };
        let serialized = bincode::serialize(&net_msg)?;
        let length = serialized.len() as u32;
        let mut frame = Vec::with_capacity(8 + serialized.len());
        frame.extend_from_slice(&NET_MAGIC.to_be_bytes());
        frame.extend_from_slice(&length.to_be_bytes());
        frame.extend_from_slice(&serialized);

        if let Some(writer) = self.writer_queues.read().get(peer_key).cloned() {
            use std::time::Duration;
            match writer.tx.send_timeout(frame, Duration::from_millis(100)) {
                Ok(_) => Ok(()),
                Err(crossbeam::channel::SendTimeoutError::Timeout(frame)) => {
                    warn!("peer {:?} 发送队列堵塞>100ms，继续等待并重试一次", peer_key.to_bytes()[0..4].to_vec());
                    // 再尝试一次阻塞发送，避免丢关键共识消息
                    if writer.tx.send(frame).is_err() {
                        error!("peer {:?} 发送队列断开，消息丢弃", peer_key.to_bytes()[0..4].to_vec());
                    }
                    Ok(())
                }
                Err(crossbeam::channel::SendTimeoutError::Disconnected(_)) => {
                    error!("peer {:?} 发送队列断开", peer_key.to_bytes()[0..4].to_vec());
                    Err("writer disconnected".into())
                }
            }
        } else {
            warn!("peer {:?} 无 writer 队列", peer_key.to_bytes()[0..4].to_vec());
            Err("无 writer 队列".into())
        }
    }
}

impl Clone for TcpNetwork {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            message_rx: self.message_rx.clone(),
            message_tx: self.message_tx.clone(),
            connection_manager: self.connection_manager.clone(),
            _server_handle: thread::spawn(|| {}),
            writer_queues: self.writer_queues.clone(),
        }
    }
}

impl TcpNetwork {
    /// 批量广播消息到所有节点
    pub fn broadcast_batch(&mut self, messages: Vec<Message>) -> Result<(), Box<dyn std::error::Error>> {
        if messages.is_empty() {
            return Ok(());
        }

        let total_nodes = self.config.peer_addrs.len();
        let message_count = messages.len();
        
        debug!("开始批量广播 {} 个消息到 {} 个节点", message_count, total_nodes);

        // 批量序列化所有消息
        let mut serialized_messages = Vec::with_capacity(messages.len());
        for message in &messages {
            let (message_type, message_bytes) = Self::message_to_bytes(message)?;
            
            let net_msg = NetworkMessage {
                from: self.config.my_key.to_bytes().to_vec(),
                message_type,
                message_bytes,
            };
            
            let serialized = bincode::serialize(&net_msg)?;
            serialized_messages.push(serialized);
        }

        let mut success_count = 0;
        let mut total_bytes_sent = 0;

        // 批量发送到每个节点
        for peer_key in self.config.peer_addrs.keys() {
            match self.send_batch_to_peer(peer_key, &serialized_messages) {
                Ok(bytes_sent) => {
                    success_count += 1;
                    total_bytes_sent += bytes_sent;
                }
                Err(e) => {
                    error!("批量发送失败到 {:?}: {}", peer_key.to_bytes()[0..4].to_vec(), e);
                }
            }
        }

        debug!("批量广播完成: {}/{} 节点成功, {} 字节总计", 
               success_count, total_nodes, total_bytes_sent);
        
        Ok(())
    }

    /// 批量发送消息到单个节点
    fn send_batch_to_peer(&self, peer_key: &VerifyingKey, serialized_messages: &[Vec<u8>]) 
        -> Result<usize, Box<dyn std::error::Error>> {
        
        // 发送给自己的消息直接放入通道
        if *peer_key == self.config.my_key {
            // 对于发送给自己的消息，需要反序列化后放入接收队列
            for serialized in serialized_messages {
                if let Ok(net_msg) = bincode::deserialize::<NetworkMessage>(serialized) {
                    if let Ok(message) = Self::bytes_to_message(net_msg.message_type, &net_msg.message_bytes) {
                        let _ = self.message_tx.send((self.config.my_key, message));
                    }
                }
            }
            return Ok(serialized_messages.iter().map(|m| m.len()).sum());
        }

        // 由 per-peer writer 逐条发送
        let mut total_bytes = 0usize;
        if let Some(writer) = self.writer_queues.read().get(peer_key).cloned() {
            for serialized in serialized_messages {
                let length = serialized.len() as u32;
                let mut frame = Vec::with_capacity(8 + serialized.len());
                frame.extend_from_slice(&NET_MAGIC.to_be_bytes());
                frame.extend_from_slice(&length.to_be_bytes());
                frame.extend_from_slice(serialized);
                total_bytes += frame.len();
                use std::time::Duration;
                if let Err(e) = writer.tx.send_timeout(frame, Duration::from_millis(100)) {
                    match e {
                        crossbeam::channel::SendTimeoutError::Timeout(frame) => {
                            warn!("peer {:?} 批量队列堵塞>100ms，阻塞重试", peer_key.to_bytes()[0..4].to_vec());
                            let _ = writer.tx.send(frame);
                        }
                        crossbeam::channel::SendTimeoutError::Disconnected(_) => {
                            error!("peer {:?} 批量队列断开", peer_key.to_bytes()[0..4].to_vec());
                        }
                    }
                }
            }
            Ok(total_bytes)
        } else {
            warn!("peer {:?} 无 writer 队列", peer_key.to_bytes()[0..4].to_vec());
            Err("无 writer 队列".into())
        }
    }

    /// 发送多个消息到指定节点列表
    pub fn send_batch_to_peers(&mut self, targets: Vec<(VerifyingKey, Message)>) 
        -> Result<(), Box<dyn std::error::Error>> {
        
        if targets.is_empty() {
            return Ok(());
        }

        // 按目标节点分组消息
        let mut peer_messages: HashMap<VerifyingKey, Vec<Message>> = HashMap::new();
        
        for (peer_key, message) in targets {
            peer_messages.entry(peer_key)
                .or_insert_with(Vec::new)
                .push(message);
        }

        let mut total_success = 0;
        let total_peers = peer_messages.len();

        // 对每个节点批量发送其消息
        for (peer_key, messages) in peer_messages {
            // 批量序列化该节点的所有消息
            let mut serialized_messages = Vec::with_capacity(messages.len());
            let mut serialization_failed = false;

            for message in &messages {
                match Self::message_to_bytes(&message) {
                    Ok((message_type, message_bytes)) => {
                        let net_msg = NetworkMessage {
                            from: self.config.my_key.to_bytes().to_vec(),
                            message_type,
                            message_bytes,
                        };
                        
                        match bincode::serialize(&net_msg) {
                            Ok(serialized) => serialized_messages.push(serialized),
                            Err(e) => {
                                error!("序列化失败: {}", e);
                                serialization_failed = true;
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        error!("消息转换失败: {}", e);
                        serialization_failed = true;
                        break;
                    }
                }
            }

            if !serialization_failed {
                match self.send_batch_to_peer(&peer_key, &serialized_messages) {
                    Ok(_) => total_success += 1,
                    Err(e) => error!("批量发送到 {:?} 失败: {}", 
                                   peer_key.to_bytes()[0..4].to_vec(), e),
                }
            }
        }

        debug!("批量发送完成: {}/{} 节点成功", total_success, total_peers);
        Ok(())
    }
}

impl Network for TcpNetwork {
    fn init_validator_set(&mut self, validator_set: ValidatorSet) {
        info!("TCP节点 {:?} 初始化验证者集合: {} 个验证者", 
              self.config.my_key.to_bytes()[0..4].to_vec(),
              validator_set.len());
    }

    fn update_validator_set(&mut self, _updates: ValidatorSetUpdates) {
        info!("TCP节点 {:?} 更新验证者集合", 
              self.config.my_key.to_bytes()[0..4].to_vec());
    }

    fn broadcast(&mut self, message: Message) {
        // 使用批量广播优化单消息广播
        if let Err(e) = self.broadcast_batch(vec![message]) {
            error!("广播失败: {}", e);
        }
    }

    // fn broadcast(&mut self, message: Message) {
    //     // 回退到单消息发送
    //     for peer_key in self.config.peer_addrs.keys() {
    //         if let Err(e) = self.send_to_peer(peer_key, &message) {
    //             error!("发送失败: {}", e);
    //         }
    //     }
    // }

    fn send(&mut self, peer: VerifyingKey, message: Message) {
        if let Err(e) = self.send_to_peer(&peer, &message) {
            error!("发送失败给 {:?}: {}", peer.to_bytes()[0..4].to_vec(), e);
        }
    }

    // fn recv(&mut self) -> Option<(VerifyingKey, Message)> {
    //     // 使用非阻塞接收
    //     self.message_rx.try_recv().ok()
    // }
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

// remove_connection 已移至 ConnectionManager

// TCP服务器运行函数 - 使用无锁通道
fn run_tcp_server(
    config: TcpNetworkConfig,
    message_tx: Sender<(VerifyingKey, Message)>,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(config.my_addr)?;
    info!("TCP服务器监听: {}", config.my_addr);
    
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                // 禁用Nagle，降低延迟抖动
                let _ = stream.set_nodelay(true);
                let _ = stream.set_read_timeout(Some(Duration::from_secs(30)));
                let tx = message_tx.clone();
                thread::spawn(move || {
                    if let Err(e) = handle_client(stream, tx) {
                        error!("处理客户端连接错误: {}", e);
                    }
                });
            }
            Err(e) => {
                error!("接受连接错误: {}", e);
            }
        }
    }
    
    Ok(())
}

// 处理客户端连接 - 使用无锁通道
fn handle_client(
    mut stream: TcpStream,
    message_tx: Sender<(VerifyingKey, Message)>,
) -> Result<(), Box<dyn std::error::Error>> {
    let peer_addr = stream.peer_addr()?;
    debug!("新连接来自: {}", peer_addr);
    
    loop {
    // 先读取 magic/或旧版长度（兼容旧协议：若非 NET_MAGIC，则将其解释为长度）
    let mut magic_or_len = [0u8; 4];
    match stream.read_exact(&mut magic_or_len) {
        Ok(_) => {},
        Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            debug!("连接正常关闭: {}", peer_addr);
            break;
        }
        Err(e) => {
            error!("读取帧头失败 from {}: {}", peer_addr, e);
            break;
        }
    }
    let magic = u32::from_be_bytes(magic_or_len);
    let length: usize = if magic == NET_MAGIC {
        // 读取消息长度
        let mut length_buf = [0u8; 4];
        match stream.read_exact(&mut length_buf) {
            Ok(_) => {}
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                debug!("连接在读取长度时关闭: {}", peer_addr);
                break;
            }
            Err(e) => {
                error!("读取长度失败 from {}: {}", peer_addr, e);
                break;
            }
        }
        u32::from_be_bytes(length_buf) as usize
    } else {
        // 兼容旧协议：magic_or_len 实际就是长度
        u32::from_be_bytes(magic_or_len) as usize
    };
        
        
        if length == 0 {
            // ping 帧，继续下一条
            continue;
        }
        if length > MAX_MSG_SIZE {
            // 可能帧错位：尝试扫描 magic 重新同步
            warn!("收到异常长度 {} from {}，尝试帧重同步", length, peer_addr);
            let mut window: [u8; 4] = [0; 4];
            let mut found = false;
            // 最多扫描 64KB
            for _ in 0..(64 * 1024) {
                let mut b = [0u8;1];
                if let Err(_) = stream.read_exact(&mut b) { break; }
                window[0] = window[1];
                window[1] = window[2];
                window[2] = window[3];
                window[3] = b[0];
                if u32::from_be_bytes(window) == NET_MAGIC {
                    // 读取长度
                    let mut len_buf = [0u8;4];
                    if stream.read_exact(&mut len_buf).is_ok() {
                        let new_len = u32::from_be_bytes(len_buf) as usize;
                        if new_len <= MAX_MSG_SIZE {
                            // 读取消息体
                            let mut message_buf = vec![0u8; new_len];
                            if stream.read_exact(&mut message_buf).is_ok() {
                                // 正常处理
                                match bincode::deserialize::<NetworkMessage>(&message_buf) {
                                    Ok(net_msg) => {
                                        // 从字节重新构造 VerifyingKey
                                        let sender_key: VerifyingKey = match net_msg.from.try_into() {
                                            Ok(bytes_array) => match VerifyingKey::from_bytes(&bytes_array) { Ok(key) => key, Err(_) => continue },
                                            Err(_) => continue,
                                        };
                                        if let Ok(hotstuff_message) = TcpNetwork::bytes_to_message(net_msg.message_type, &net_msg.message_bytes) {
                                            let _ = message_tx.send((sender_key, hotstuff_message));
                                        }
                                    }
                                    Err(_) => {}
                                }
                                found = true;
                            }
                        }
                    }
                    break;
                }
            }
            if !found { error!("帧重同步失败 from {}", peer_addr); break; }
            continue;
        }
        
        if length == 0 {
            debug!("收到空消息 from {}", peer_addr);
            continue;
        }
        
        // 读取消息内容
        let mut message_buf = vec![0u8; length];
        match stream.read_exact(&mut message_buf) {
            Ok(_) => {},
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                debug!("连接在读取消息时关闭: {}", peer_addr);
                break;
            }
            Err(e) => {
                error!("读取消息内容失败 from {}: {}", peer_addr, e);
                break;
            }
        }
        
        // 反序列化网络消息
        match bincode::deserialize::<NetworkMessage>(&message_buf) {
            Ok(net_msg) => {
                // 从字节重新构造 VerifyingKey
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
                
                // 反序列化 HotStuff 消息
                match TcpNetwork::bytes_to_message(net_msg.message_type, &net_msg.message_bytes) {
                    Ok(hotstuff_message) => {
                        // 使用无锁通道发送消息
                        if let Err(e) = message_tx.send((sender_key, hotstuff_message)) {
                            error!("发送消息到队列失败: {}", e);
                            break;
                        }
                    }
                    Err(e) => {
                        error!("反序列化 HotStuff 消息失败: {}", e);
                    }
                }
            }
            Err(e) => {
                error!("反序列化网络消息失败 from {}: {}", peer_addr, e);
            }
        }
    }
    
    Ok(())
}
