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
}

impl TcpNetwork {
    pub fn new(config: TcpNetworkConfig) -> Result<Self, Box<dyn std::error::Error>> {
        // 使用 crossbeam 的无锁通道
        let (tx, rx) = unbounded();
        
        let connection_manager = Arc::new(ConnectionManager::new(config.clone()));
        
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
        };

        network.connect_to_peers()?;
        
        Ok(network)
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

    fn send_to_peer(&self, peer_key: &VerifyingKey, message: &Message) -> Result<(), Box<dyn std::error::Error>> {
        // 发送给自己的消息直接放入通道
        if *peer_key == self.config.my_key {
            self.message_tx.send((self.config.my_key, message.clone()))?;
            return Ok(());
        }

        // 获取连接并发送
        if let Some(mut stream) = self.connection_manager.get_or_create_connection(peer_key) {
            let (message_type, message_bytes) = Self::message_to_bytes(message)?;
            
            let net_msg = NetworkMessage {
                from: self.config.my_key.to_bytes().to_vec(),
                message_type,
                message_bytes,
            };
            
            let serialized = bincode::serialize(&net_msg)?;
            let length = serialized.len() as u32;
            
            // 发送消息
            stream.write_all(&length.to_be_bytes())?;
            stream.write_all(&serialized)?;
            stream.flush()?;
            
            Ok(())
        } else {
            Err("无法建立连接".into())
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

        // 获取连接
        if let Some(mut stream) = self.connection_manager.get_or_create_connection(peer_key) {
            let mut total_bytes = 0;

            // 计算总数据大小，为批量写入做准备
            let batch_size: usize = serialized_messages.iter()
                .map(|msg| 4 + msg.len()) // 4字节长度前缀 + 消息内容
                .sum();

            // 创建批量数据缓冲区
            let mut batch_buffer = Vec::with_capacity(batch_size);

            // 将所有消息打包到一个缓冲区
            for serialized in serialized_messages {
                let length = serialized.len() as u32;
                batch_buffer.extend_from_slice(&length.to_be_bytes());
                batch_buffer.extend_from_slice(serialized);
            }

            // 一次性发送所有数据
            stream.write_all(&batch_buffer)?;
            stream.flush()?;

            total_bytes = batch_buffer.len();
            
            debug!("批量发送到 {:?}: {} 消息, {} 字节", 
                   peer_key.to_bytes()[0..4].to_vec(), 
                   serialized_messages.len(), 
                   total_bytes);

            Ok(total_bytes)
        } else {
            Err("无法建立连接".into())
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

// TCP服务器运行函数 - 使用无锁通道
fn run_tcp_server(
    config: TcpNetworkConfig,
    message_tx: Sender<(VerifyingKey, Message)>,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(config.my_addr)?;
    info!("TCP服务器监听: {}", config.my_addr);
    
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
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
        // 读取消息长度
        let mut length_buf = [0u8; 4];
        match stream.read_exact(&mut length_buf) {
            Ok(_) => {},
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                debug!("连接正常关闭: {}", peer_addr);
                break;
            }
            Err(e) => {
                error!("读取长度失败 from {}: {}", peer_addr, e);
                break;
            }
        }
        
        let length = u32::from_be_bytes(length_buf) as usize;
        // info!("******* 收到消息长度: {} bytes from {}", length, peer_addr);
        
        
        if length > 10 * 1024 * 1024 { // 10MB limit
            error!("消息太大: {} bytes from {}", length, peer_addr);
            break;
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