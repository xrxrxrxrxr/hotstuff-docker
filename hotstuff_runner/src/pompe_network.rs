// hotstuff_runner/src/pompe_network.rs
//! 修复的Pompe网络实现 - 解决时间戳收集不全问题

use crate::pompe::PompeMessage;
use crate::resolve_target;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
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
    pub message_id: String, // 添加消息ID用于去重
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

    // 🚨 新增：连接池和重试机制
    // connection_pool: Arc<Mutex<HashMap<usize, Option<TcpStream>>>>,
    // 🚨 优化：连接池管理
    // 避免在 await 期间持有写锁：每个连接状态单独放入 AsyncMutex 中
    connections: Arc<tokio::sync::RwLock<HashMap<usize, ConnectionHandle>>>,
    sent_messages: Arc<Mutex<HashMap<String, u64>>>, // 消息去重
    // 独立运行时，用于隔离 Pompe 网络与其他任务
    rt: Arc<Runtime>,
}

impl PompeNetwork {
    pub fn new(node_id: usize, peer_node_ids: Vec<usize>) -> Self {
        // 支持通过环境变量配置 Pompe 端口：
        // 1) POMPE_PORT=端口号（优先）
        // 2) 或 POMPE_PORT_BASE=基准端口（默认20000），按 base + node_id 计算
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
        // 创建独立的 Tokio 运行时（线程数可由环境变量 POMPE_RT_THREADS 配置，默认 2）
        let rt_threads: usize = std::env::var("POMPE_RT_THREADS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(2);
        let rt = Arc::new(
            Builder::new_multi_thread()
                .worker_threads(rt_threads)
                .enable_all()
                .thread_name(&format!("pompe-net-{}", node_id))
                .build()
                .expect("Failed to build Pompe runtime"),
        );

        info!("🌐 创建Pompe网络，节点 {}, 端口: {}", node_id, pompe_port);
        info!("🔍 对等节点列表: {:?}", peer_node_ids);

        // 🚨 验证当前节点在对等列表中
        if !peer_node_ids.contains(&node_id) {
            warn!(
                "⚠️ 当前节点 {} 不在对等节点列表中: {:?}",
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
        // 🚨 启动连接维护任务
        network.start_connection_maintenance();
        network
    }

    // 🚨 新增：连接维护任务
    fn start_connection_maintenance(&self) {
        let connections = Arc::clone(&self.connections);
        let node_id = self.node_id;
        // Also keep a handle to sent_messages for periodic cleanup
        let sent_messages = Arc::clone(&self.sent_messages);

        let rt = self.rt.clone();
        rt.spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60)); // 每60秒清理一次

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
                                "🧹 [连接维护] Node {} 清理到节点 {} 的空闲连接",
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
                    info!("🎧 Node {} Pompe服务器监听: {}", node_id, addr);
                    loop {
                        match listener.accept().await {
                            Ok((mut socket, peer)) => {
                                debug!("📞 Node {} Pompe连接来自: {}", node_id, peer);
                                if let Err(e) = socket.set_nodelay(true) {
                                    warn!("⚠️ 设置TCP_NODELAY失败: {}", e);
                                }
                                let tx = message_tx.clone();
                                tokio::spawn(async move {
                                    let _ = handle_pompe_connection(&mut socket, tx).await;
                                });
                            }
                            Err(e) => {
                                warn!("Node {} Pompe accept 错误: {}", node_id, e);
                                tokio::time::sleep(Duration::from_millis(100)).await;
                            }
                        }
                    }
                }
                Err(e) => error!("Node {} 绑定Pompe地址失败 {}: {}", node_id, addr, e),
            }
        });
        Ok(())
    }

    // 主动预热到所有对等节点的连接，减少首次发送延迟
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
                // 即使失败也忽略，连接池会在后续尝试建立
            }
            info!("🔌 Node {} 连接预热任务完成", net.node_id);
        });
    }

    pub fn spawn<F>(&self, fut: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let _ = self.rt.spawn(fut);
    }

    // 🚨 改进的单节点发送，支持重试和连接池
    pub async fn send_to_node(
        &self,
        target_node_id: usize,
        message: PompeMessage,
    ) -> Result<(), String> {
        // 🚨 特殊处理：发送给自己
        if target_node_id == self.node_id {
            debug!(
                "📨 发送Pompe消息给自己: {:?}",
                std::mem::discriminant(&message)
            );
            if let Err(e) = self.message_tx.send((self.node_id, message)) {
                error!("❌ Node {} Pompe发送给自己失败: {}", self.node_id, e);
                return Err(format!("发送给自己失败: {}", e));
            }
            return Ok(());
        }

        // 生成消息ID用于去重
        let message_id = format!(
            "{}:{}:{}",
            self.node_id,
            target_node_id,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        // 检查是否已发送过
        {
            let mut sent = self.sent_messages.lock().unwrap();
            if sent.contains_key(&message_id) {
                debug!("🔄 跳过重复消息: {}", message_id);
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
                    "⚠️ Node {} -> Node {} 现有连接不可用，准备重建",
                    self.node_id, target_node_id
                );
            }
        }

        let target_addr = resolve_target(target_node_id, 20000);
        info!(
            "🔗 Node {} Pompe resolve node addr {}: {}",
            self.node_id, target_node_id, target_addr
        );

        let stream = TcpStream::connect(&target_addr).await.map_err(|e| {
            error!(
                "❌ Node {} 连接到节点 {} 失败: {}",
                self.node_id, target_node_id, e
            );
            format!("连接失败: {}", e)
        })?;

        if let Err(e) = stream.set_nodelay(true) {
            warn!("⚠️ 设置TCP_NODELAY失败: {}", e);
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
            Err(format!("发送到节点 {} 失败", target_node_id))
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
                        "⚠️ [Pompe] {} -> {} writer task send failed: {}",
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
                        "⏱️ [Pompe] {} -> {} send slow: {:?}",
                        node_id, target_node_id, elapsed
                    );
                }
            }

            let mut guard = connections.write().await;
            if guard.remove(&target_node_id).is_some() {
                info!(
                    "🧹 [Pompe] Node {} closed writer to {}",
                    node_id, target_node_id
                );
            }
        });
    }

    // 🚨 新增：在指定流上发送消息的辅助方法
    async fn send_message_on_writer(
        node_id: usize,
        writer: &mut OwnedWriteHalf,
        network_msg: &PompeNetworkMessage,
    ) -> Result<(), String> {
        // 使用bincode更紧凑，减少序列化成本和网络抖动
        let ser_start = std::time::Instant::now();
        let serialized =
            bincode::serialize(network_msg).map_err(|e| format!("序列化失败: {}", e))?;
        let message_length = serialized.len() as u32;
        let ser_cost = ser_start.elapsed();

        writer
            .write_all(&message_length.to_be_bytes())
            .await
            .map_err(|e| format!("写入长度失败: {}", e))?;

        writer
            .write_all(&serialized)
            .await
            .map_err(|e| format!("写入消息失败: {}", e))?;

        // 对于TCP流，flush通常是空操作；避免多余系统调用
        if ser_cost.as_micros() > 50 {
            debug!(
                "⏱️ [Pompe-序列化] Node {} 序列化耗时: {:?} ({} bytes)",
                node_id, ser_cost, message_length
            );
        }
        Ok(())
    }

    // 🚨 并行广播：并行发送，并优先短路发送给自己
    pub async fn broadcast(&self, message: PompeMessage) -> Result<(), String> {
        use tokio::task::JoinHandle;
        let start_time = std::time::Instant::now();
        info!(
            "📡 Node {} Pompe并行广播: {:?} 到 {} 个节点",
            self.node_id,
            std::mem::discriminant(&message),
            self.peer_node_ids.len()
        );

        let mut success_count = 0usize;
        let mut failure_details: Vec<String> = Vec::new();

        // 1) 先发送给自己（短路，不经TCP）
        if self.peer_node_ids.contains(&self.node_id) {
            match self.send_to_node(self.node_id, message.clone()).await {
                Ok(_) => success_count += 1,
                Err(e) => failure_details.push(format!("self: {}", e)),
            }
        }

        // 2) 并行发送给其他节点
        let mut handles: Vec<JoinHandle<(usize, Result<(), String>)>> = Vec::new();
        for &target_node_id in &self.peer_node_ids {
            if target_node_id == self.node_id {
                continue;
            }
            let net = self.clone();
            let msg = message.clone();
            // 在 Pompe 独立运行时上并行发送，避免与其他任务争抢全局 RT
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
            "📊 [并行广播完成] Node {} 完成: {}/{} 成功, 总耗时: {:?}",
            self.node_id,
            success_count,
            self.peer_node_ids.len(),
            total_duration
        );

        if !failure_details.is_empty() {
            warn!(
                "⚠️ Node {} Pompe广播部分失败: {:?}",
                self.node_id, failure_details
            );
        }

        if success_count > 0 {
            Ok(())
        } else {
            Err("所有广播目标都失败了".to_string())
        }
    }

    pub async fn recv(&self) -> Option<(usize, PompeMessage)> {
        let mut rx = self.message_rx.lock().await;
        rx.recv().await
    }

    // 🚨 新增：清理过期消息的维护函数
    pub fn cleanup_old_messages(&self) {
        let mut sent = self.sent_messages.lock().unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // 清理超过5分钟的消息记录
        sent.retain(|_, &mut timestamp| now - timestamp < 300);

        if sent.len() > 1000 {
            // 如果消息记录过多，清理一半最旧的
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
    // 🚨 新增：获取连接池状态
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
                "🔗 [连接池状态] Node {} 活跃连接: {}, 总发送数: {}",
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
        socket
            .read_exact(&mut message_buf)
            .await
            .map_err(|e| format!("读取消息失败: {}", e))?;

        // 使用bincode与发送端保持一致
        match bincode::deserialize::<PompeNetworkMessage>(&message_buf) {
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

                debug!(
                    "📨 收到Pompe消息: 来自节点 {}, 类型: {:?}, ID: {}",
                    net_msg.from_node_id,
                    std::mem::discriminant(&net_msg.message),
                    &net_msg.message_id[0..8]
                );

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
            connections: Arc::clone(&self.connections),
            sent_messages: Arc::clone(&self.sent_messages),
            rt: Arc::clone(&self.rt),
        }
    }
}
