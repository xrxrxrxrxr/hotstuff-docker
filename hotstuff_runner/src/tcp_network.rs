// hotstuff_runner/src/tcp_network.rs
//! 基于TCP的真实网络实现，用于Docker多进程部署

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
use std::sync::{Arc, Mutex, mpsc};
use std::collections::HashMap;
use std::net::{TcpListener, TcpStream, SocketAddr};
use std::io::{Read, Write};
use std::thread;
// use log::{debug, info, error, warn};
use tracing::{debug, info, error, warn};
use serde::{Serialize, Deserialize};
// 确保导入正确版本的 Borsh traits
use borsh::{BorshSerialize, BorshDeserialize};
use hotstuff_rs::block_sync::messages::BlockSyncMessage;

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
    // 添加其他消息类型
}

// 网络消息包装器 - 直接传输字节
#[derive(Serialize, Deserialize, Clone)]
struct NetworkMessage {
    from: Vec<u8>,  // VerifyingKey bytes
    message_type: MessageType, // 消息类型标识
    message_bytes: Vec<u8>, // Message 的原始字节（使用其他方式序列化）
}

// TCP网络配置
#[derive(Clone)]
pub struct TcpNetworkConfig {
    pub my_addr: SocketAddr,
    pub peer_addrs: HashMap<VerifyingKey, SocketAddr>,
    pub my_key: VerifyingKey,
}

// TCP网络实现
pub struct TcpNetwork {
    config: TcpNetworkConfig,
    message_rx: Arc<Mutex<mpsc::Receiver<(VerifyingKey, Message)>>>,
    peer_connections: Arc<Mutex<HashMap<VerifyingKey, TcpStream>>>,
    _server_handle: thread::JoinHandle<()>,
}

impl TcpNetwork {
    pub fn new(config: TcpNetworkConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let (tx, rx) = mpsc::channel();
        let message_rx = Arc::new(Mutex::new(rx));
        let peer_connections = Arc::new(Mutex::new(HashMap::new()));
        
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

        // 连接到对等节点
        let mut network = Self {
            config: config.clone(),
            message_rx,
            peer_connections,
            _server_handle: server_handle,
        };

        network.connect_to_peers()?;
        
        Ok(network)
    }

    fn connect_to_peers(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let mut connections = self.peer_connections.lock().unwrap();
        
        for (peer_key, peer_addr) in &self.config.peer_addrs {
            if *peer_key == self.config.my_key {
                continue;
            }

            info!("🔗 尝试连接对等节点: {:?} -> {}", 
                  peer_key.to_bytes()[0..4].to_vec(), peer_addr);
            
            // 添加重试机制
            let mut connected = false;
            for attempt in 1..=3 {
                match TcpStream::connect(peer_addr) {
                    Ok(stream) => {
                        info!("✅ 成功连接到对等节点: {} (尝试 {})", peer_addr, attempt);
                        connections.insert(*peer_key, stream);
                        connected = true;
                        break;
                    }
                    Err(e) => {
                        if attempt < 3 {
                            warn!("⚠️ 连接尝试 {}/3 失败 {}: {}", attempt, peer_addr, e);
                            thread::sleep(std::time::Duration::from_millis(1000));
                        } else {
                            warn!("⚠️ 所有连接尝试失败 {}: {} (节点可能还未启动)", peer_addr, e);
                        }
                    }
                }
            }
        }
        
        info!("🌐 建立了 {} 个对等连接", connections.len());
        Ok(())
    }

    // 将 Message 转换为字节的辅助函数
    fn message_to_bytes(message: &Message) -> Result<(MessageType, Vec<u8>), Box<dyn std::error::Error>> {
        // 根据消息类型判断 MessageType
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
        
        // 使用 Borsh 序列化 - 使用静态方法调用
        let bytes = message.try_to_vec().map_err(|e| {
            error!("❌ 序列化消息失败: {}", e);
            format!("Message serialization failed: {}", e)
        })?;
        
        debug!("✅ 序列化消息成功: {} bytes, 类型: {:?}", bytes.len(), message_type);
        Ok((message_type, bytes))
    }

    // 从字节重建 Message 的辅助函数
    fn bytes_to_message(_message_type: MessageType, bytes: &[u8]) -> Result<Message, Box<dyn std::error::Error>> {
        // 使用 BorshDeserialize trait 方法
        let message = Message::try_from_slice(bytes).map_err(|e| {
            error!("❌ 反序列化消息失败: {}", e);
            format!("Message deserialization failed: {}", e)
        })?;
        
        debug!("✅ 反序列化消息成功: {} bytes", bytes.len());
        Ok(message)
    }

    fn send_to_peer(&self, peer_key: &VerifyingKey, message: &Message) -> Result<(), Box<dyn std::error::Error>> {
        let mut connections = self.peer_connections.lock().unwrap();
        
        if let Some(stream) = connections.get_mut(peer_key) {
            let (message_type, message_bytes) = Self::message_to_bytes(message)?;
            
            let net_msg = NetworkMessage {
                from: self.config.my_key.to_bytes().to_vec(),
                message_type,
                message_bytes,
            };
            
            let serialized = bincode::serialize(&net_msg)?;
            let length = serialized.len() as u32;
            
            // 发送长度前缀
            stream.write_all(&length.to_be_bytes())?;
            // 发送消息内容
            stream.write_all(&serialized)?;
            stream.flush()?;
            
            debug!("📤 发送消息到 {:?}", peer_key.to_bytes()[0..4].to_vec());
            Ok(())
        } else {
            // 尝试重新连接
            if let Some(peer_addr) = self.config.peer_addrs.get(peer_key) {
                match TcpStream::connect(peer_addr) {
                    Ok(stream) => {
                        info!("🔄 重新连接到对等节点: {}", peer_addr);
                        connections.insert(*peer_key, stream);
                        // 递归调用发送
                        drop(connections); // 释放锁
                        return self.send_to_peer(peer_key, message);
                    }
                    Err(e) => {
                        error!("❌ 重新连接失败 {}: {}", peer_addr, e);
                        return Err(Box::new(e));
                    }
                }
            } else {
                error!("❌ 找不到对等节点地址: {:?}", peer_key.to_bytes()[0..4].to_vec());
                return Err("未知对等节点".into());
            }
        }
    }
}

impl Clone for TcpNetwork {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            message_rx: self.message_rx.clone(),  // ✅ 共享同一个接收通道
            peer_connections: self.peer_connections.clone(),
            _server_handle: thread::spawn(|| {}), // 注意：这不是真正的克隆，但满足类型要求
        }
    }
}

impl Network for TcpNetwork {
    fn init_validator_set(&mut self, validator_set: ValidatorSet) {
        info!("🏗️ TCP节点 {:?} 初始化验证者集合: {} 个验证者", 
              self.config.my_key.to_bytes()[0..4].to_vec(),
              validator_set.len());
    }

    fn update_validator_set(&mut self, _updates: ValidatorSetUpdates) {
        info!("🔄 TCP节点 {:?} 更新验证者集合", 
              self.config.my_key.to_bytes()[0..4].to_vec());
    }

    fn broadcast(&mut self, message: Message) {
        let peer_count = self.config.peer_addrs.len() - 1; // 排除自己
        debug!("📡 TCP节点 {:?} 广播给 {} 个对等节点", 
               self.config.my_key.to_bytes()[0..4].to_vec(), 
               peer_count);
        
        let mut success_count = 0;
        for peer_key in self.config.peer_addrs.keys() {
            if *peer_key != self.config.my_key {
                if let Err(e) = self.send_to_peer(peer_key, &message) {
                    error!("广播发送失败到 {:?}: {}", peer_key.to_bytes()[0..4].to_vec(), e);
                } else {
                    success_count += 1;
                }
            }
        }
        
        debug!("✅ 成功广播给 {}/{} 个对等节点", success_count, peer_count);
    }

    fn send(&mut self, peer: VerifyingKey, message: Message) {
        debug!("📨 TCP节点发送消息给 {:?}", peer.to_bytes()[0..4].to_vec());
        
        if let Err(e) = self.send_to_peer(&peer, &message) {
            error!("❌ 发送失败给 {:?}: {}", peer.to_bytes()[0..4].to_vec(), e);
        }
    }

    fn recv(&mut self) -> Option<(VerifyingKey, Message)> {
        let receiver = self.message_rx.lock().unwrap();
        match receiver.try_recv() {
            Ok(msg) => {
                debug!("📬 TCP节点接收消息来自 {:?}", msg.0.to_bytes()[0..4].to_vec());
                Some(msg)
            }
            Err(mpsc::TryRecvError::Empty) => None,
            Err(mpsc::TryRecvError::Disconnected) => {
                error!("❌ TCP节点接收通道断开");
                None
            }
        }
    }
}

// TCP服务器运行函数
fn run_tcp_server(
    config: TcpNetworkConfig,
    message_tx: mpsc::Sender<(VerifyingKey, Message)>,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(config.my_addr)?;
    info!("🎧 TCP服务器监听: {}", config.my_addr);
    
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

// 处理客户端连接
fn handle_client(
    mut stream: TcpStream,
    message_tx: mpsc::Sender<(VerifyingKey, Message)>,
) -> Result<(), Box<dyn std::error::Error>> {
    let peer_addr = stream.peer_addr()?;
    debug!("📞 新连接来自: {}", peer_addr);
    
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
        
        // 防止过大的消息
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
                debug!("📨 收到网络消息 from {}, 消息类型: {:?}", peer_addr, net_msg.message_type);
                
                // 从字节重新构造 VerifyingKey
                let sender_key: VerifyingKey = match net_msg.from.try_into() {
                    Ok(bytes_array) => match VerifyingKey::from_bytes(&bytes_array) {
                        Ok(key) => key,
                        Err(_) => {
                            error!("❌ 无法从字节构造 VerifyingKey");
                            continue;
                        }
                    },
                    Err(_) => {
                        error!("❌ 字节数组长度不正确");
                        continue;
                    }
                };
                
                // 反序列化 HotStuff 消息
                match TcpNetwork::bytes_to_message(net_msg.message_type, &net_msg.message_bytes) {
                    Ok(hotstuff_message) => {
                        debug!("✅ 成功反序列化 HotStuff 消息");
                        
                        // 发送到消息队列
                        if let Err(e) = message_tx.send((sender_key, hotstuff_message)) {
                            error!("❌ 发送消息到队列失败: {}", e);
                            break; // 如果队列断开，退出循环
                        }
                    }
                    Err(e) => {
                        error!("❌ 反序列化 HotStuff 消息失败: {}", e);
                        // 继续处理下一个消息，不退出
                    }
                }
            }
            Err(e) => {
                error!("❌ 反序列化网络消息失败 from {}: {}", peer_addr, e);
                // 继续处理下一个消息，不退出
            }
        }
    }
    
    debug!("客户端连接处理结束: {}", peer_addr);
    Ok(())
}

// // 新增：网络包装器
// #[derive(Clone)]
// pub struct SharedTcpNetwork {
//     inner: Arc<Mutex<TcpNetwork>>,
// }

// impl SharedTcpNetwork {
//     pub fn new(config: TcpNetworkConfig) -> Result<Self, Box<dyn std::error::Error>> {
//         let tcp_network = TcpNetwork::new(config)?;
//         Ok(Self {
//             inner: Arc::new(Mutex::new(tcp_network)),
//         })
//     }
// }

// impl Network for SharedTcpNetwork {
//     fn init_validator_set(&mut self, validator_set: ValidatorSet) {
//         self.inner.lock().unwrap().init_validator_set(validator_set);
//     }

//     fn update_validator_set(&mut self, updates: ValidatorSetUpdates) {
//         self.inner.lock().unwrap().update_validator_set(updates);
//     }

//     fn broadcast(&mut self, message: Message) {
//         self.inner.lock().unwrap().broadcast(message);
//     }

//     fn send(&mut self, peer: VerifyingKey, message: Message) {
//         self.inner.lock().unwrap().send(peer, message);
//     }

//     fn recv(&mut self) -> Option<(VerifyingKey, Message)> {
//         self.inner.lock().unwrap().recv()
//     }
// }

// impl Clone for SharedTcpNetwork {
//     fn clone(&self) -> Self {
//         Self {
//             inner: Arc::clone(&self.inner),
//         }
//     }
// }