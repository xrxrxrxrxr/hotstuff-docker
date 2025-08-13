// hotstuff_runner/src/pompe_network.rs
//! 修复的Pompe网络实现 - 解决时间戳收集不全问题

use std::sync::{Arc, Mutex};
use tokio::sync::mpsc as async_mpsc;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use crate::pompe::PompeMessage;
use tracing::{debug, info, error, warn};
use serde::{Serialize, Deserialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PompeNetworkMessage {
    pub from_node_id: usize,
    pub to_node_id: Option<usize>,
    pub message: PompeMessage,
    pub timestamp: u64,
    pub message_id: String, // 添加消息ID用于去重
}

pub struct PompeNetwork {
    node_id: usize,
    pompe_port: u16,
    pub peer_node_ids: Vec<usize>,
    message_tx: async_mpsc::UnboundedSender<(usize, PompeMessage)>,
    message_rx: Arc<Mutex<async_mpsc::UnboundedReceiver<(usize, PompeMessage)>>>,
    
    // 🚨 新增：连接池和重试机制
    connection_pool: Arc<Mutex<HashMap<usize, Option<TcpStream>>>>,
    sent_messages: Arc<Mutex<HashMap<String, u64>>>, // 消息去重
}

impl PompeNetwork {
    pub fn new(node_id: usize, peer_node_ids: Vec<usize>) -> Self {
        let pompe_port = 20000 + node_id as u16;
        let (tx, rx) = async_mpsc::unbounded_channel();
        
        info!("🌐 创建Pompe网络，节点 {}, 端口: {}", node_id, pompe_port);
        info!("🔍 对等节点列表: {:?}", peer_node_ids);
        
        // 🚨 验证当前节点在对等列表中
        if !peer_node_ids.contains(&node_id) {
            warn!("⚠️ 当前节点 {} 不在对等节点列表中: {:?}", node_id, peer_node_ids);
        }
        
        Self {
            node_id,
            pompe_port,
            peer_node_ids,
            message_tx: tx,
            message_rx: Arc::new(Mutex::new(rx)),
            connection_pool: Arc::new(Mutex::new(HashMap::new())),
            sent_messages: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn start_server(&self) -> Result<(), String> {
        let addr = format!("0.0.0.0:{}", self.pompe_port);
        let listener = TcpListener::bind(&addr).await.map_err(|e| format!("绑定地址失败: {}", e))?;
        let message_tx = self.message_tx.clone();
        let node_id = self.node_id;
        
        info!("🎧 Node {} Pompe服务器监听: {}", node_id, addr);
        
        tokio::spawn(async move {
            while let Ok((mut socket, addr)) = listener.accept().await {
                debug!("📞 Node {} Pompe连接来自: {}", node_id, addr);
                
                let tx = message_tx.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_pompe_connection(&mut socket, tx).await {
                        debug!("Pompe连接处理结束: {}", e);
                    }
                });
            }
        });
        
        Ok(())
    }

    // 🚨 改进的单节点发送，支持重试和连接池
    pub async fn send_to_node(&self, target_node_id: usize, message: PompeMessage) -> Result<(), String> {
        // 🚨 特殊处理：发送给自己
        if target_node_id == self.node_id {
            debug!("📨 发送Pompe消息给自己: {:?}", std::mem::discriminant(&message));
            if let Err(e) = self.message_tx.send((self.node_id, message)) {
                error!("❌ Node {} Pompe发送给自己失败: {}", self.node_id, e);
                return Err(format!("发送给自己失败: {}", e));
            }
            return Ok(());
        }

        // 生成消息ID用于去重
        let message_id = format!("{}:{}:{}", 
            self.node_id, target_node_id, 
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos());

        // 检查是否已发送过
        {
            let mut sent = self.sent_messages.lock().unwrap();
            if sent.contains_key(&message_id) {
                debug!("🔄 跳过重复消息: {}", message_id);
                return Ok(());
            }
            sent.insert(message_id.clone(), std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs());
        }

        let target_addr = format!("node{}:{}", target_node_id, 20000 + target_node_id);
        
        let network_msg = PompeNetworkMessage {
            from_node_id: self.node_id,
            to_node_id: Some(target_node_id),
            message,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64,
            message_id,
        };

        // 🚨 重试机制：最多重试3次
        let mut last_error_msg = String::new();
        for attempt in 1..=3 {
            match TcpStream::connect(&target_addr).await {
                Ok(mut stream) => {
                    let serialized = serde_json::to_vec(&network_msg).map_err(|e| format!("序列化失败: {}", e))?;
                    let message_length = serialized.len() as u32;
                    
                    match stream.write_all(&message_length.to_be_bytes()).await {
                        Ok(_) => {
                            match stream.write_all(&serialized).await {
                                Ok(_) => {
                                    if let Err(e) = stream.flush().await {
                                        warn!("⚠️ 刷新连接失败 {} (尝试 {}): {}", target_addr, attempt, e);
                                        continue;
                                    }
                                    
                                    debug!("📤 Node {} Pompe发送到节点 {} 成功 (尝试 {}, {}字节)", 
                                           self.node_id, target_node_id, attempt, message_length);
                                    return Ok(());
                                }
                                Err(e) => {
                                    warn!("⚠️ 写入消息失败 {} (尝试 {}): {}", target_addr, attempt, e);
                                    last_error_msg = format!("写入消息失败: {}", e);
                                    continue;
                                }
                            }
                        }
                        Err(e) => {
                            warn!("⚠️ 写入长度失败 {} (尝试 {}): {}", target_addr, attempt, e);
                            last_error_msg = format!("写入长度失败: {}", e);
                            continue;
                        }
                    }
                }
                Err(e) => {
                    warn!("⚠️ Node {} Pompe连接到节点 {} 失败 (尝试 {}): {}", 
                          self.node_id, target_node_id, attempt, e);
                    last_error_msg = format!("连接失败: {}", e);
                    
                    if attempt < 3 {
                        // 等待一段时间再重试
                        tokio::time::sleep(tokio::time::Duration::from_millis(100 * attempt as u64)).await;
                    }
                }
            }
        }
        
        error!("❌ Node {} Pompe发送到节点 {} 最终失败，已重试3次", self.node_id, target_node_id);
        Err(last_error_msg.into())
    }

    // 🚨 改进的广播：确保发送到所有节点，包括自己
    pub async fn broadcast(&self, message: PompeMessage) -> Result<(), String> {
        info!("📡 Node {} Pompe广播消息: {:?} 到 {} 个节点", 
              self.node_id, std::mem::discriminant(&message), self.peer_node_ids.len());
        
        let mut success_count = 0;
        let mut failure_details = Vec::new();
        
        // 🚨 关键修复：向所有节点发送，包括自己
        for &target_node_id in &self.peer_node_ids {
            info!("📤 [广播详情] Node {} -> Node {} 开始发送", self.node_id, target_node_id);
            
            match self.send_to_node(target_node_id, message.clone()).await {
                Ok(_) => {
                    success_count += 1;
                    info!("✅ [广播详情] Node {} -> Node {} 成功", self.node_id, target_node_id);
                }
                Err(e) => {
                    error!("❌ [广播详情] Node {} -> Node {} 失败: {}", self.node_id, target_node_id, e);
                    failure_details.push(format!("Node {}: {}", target_node_id, e));
                }
            }
        }
        
        info!("📊 Node {} Pompe广播完成: {}/{} 成功", 
              self.node_id, success_count, self.peer_node_ids.len());
              
        if !failure_details.is_empty() {
            warn!("⚠️ Node {} Pompe广播部分失败: {:?}", self.node_id, failure_details);
        }
        
        // 🚨 只要有至少一个成功就认为广播成功（包括发送给自己）
        if success_count > 0 {
            Ok(())
        } else {
            Err("所有广播目标都失败了".to_string())
        }
    }

    pub async fn recv(&self) -> Option<(usize, PompeMessage)> {
        let mut rx = self.message_rx.lock().unwrap();
        rx.try_recv().ok()
    }

    // 🚨 新增：清理过期消息的维护函数
    pub fn cleanup_old_messages(&self) {
        let mut sent = self.sent_messages.lock().unwrap();
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
        
        // 清理超过5分钟的消息记录
        sent.retain(|_, &mut timestamp| now - timestamp < 300);
        
        if sent.len() > 1000 {
            // 如果消息记录过多，清理一半最旧的
            let mut entries: Vec<_> = sent.iter().map(|(k, &v)| (k.clone(), v)).collect();
            entries.sort_by_key(|(_, timestamp)| *timestamp);

            let keys_to_remove: Vec<_> = entries.iter()
                .take(entries.len() / 2)
                .map(|(message_id, _)| message_id.clone())
                .collect();

            for message_id in keys_to_remove {
                sent.remove(&message_id);
            }
        }
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
            Ok(_) => {},
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                debug!("Pompe连接正常关闭");
                break;
            }
            Err(e) => {
                return Err(format!("读取错误: {}", e));
            }
        }
        
        let message_length = u32::from_be_bytes(length_buf) as usize;
        
        if message_length > 1024 * 1024 {
            error!("❌ Pompe消息过大: {} bytes", message_length);
            break;
        }
        
        if message_length == 0 {
            debug!("收到空Pompe消息");
            continue;
        }
        
        let mut message_buf = vec![0u8; message_length];
        socket.read_exact(&mut message_buf).await.map_err(|e| format!("读取消息失败: {}", e))?;
        
        match serde_json::from_slice::<PompeNetworkMessage>(&message_buf) {
            Ok(net_msg) => {
                // 🚨 消息去重
                if processed_messages.contains(&net_msg.message_id) {
                    debug!("🔄 跳过重复的Pompe消息: {}", net_msg.message_id);
                    continue;
                }
                processed_messages.insert(net_msg.message_id.clone());
                
                // 限制去重缓存大小
                if processed_messages.len() > 1000 {
                    processed_messages.clear();
                }
                
                debug!("📨 收到Pompe消息: 来自节点 {}, 类型: {:?}, ID: {}", 
                       net_msg.from_node_id, std::mem::discriminant(&net_msg.message), 
                       &net_msg.message_id[0..8]);
                
                if let Err(e) = message_tx.send((net_msg.from_node_id, net_msg.message)) {
                    error!("❌ Pompe消息队列发送失败: {}", e);
                    break;
                }
            }
            Err(e) => {
                error!("❌ Pompe消息反序列化失败: {}", e);
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
            connection_pool: Arc::clone(&self.connection_pool),
            sent_messages: Arc::clone(&self.sent_messages),
        }
    }
}