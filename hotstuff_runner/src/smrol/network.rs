// src/smrol/network.rs - 优化后的SMROL TCP网络层实现

use crate::smrol::message::{SmrolMessage, SmrolTransaction};
use futures::Future;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::{Builder, Handle, Runtime};
use tokio::sync::{mpsc, RwLock};
use tokio::time::{interval, sleep};
use tracing::{debug, error, info, warn};

// SMROL网络消息封装
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SmrolNetworkMessage {
    pub from_node_id: usize,
    pub to_node_id: Option<usize>, // None表示广播
    pub message: SmrolMessage,
    pub timestamp: u64,
    pub message_id: String, // 消息去重ID
}

// TCP连接状态管理
#[derive(Debug)]
struct TcpConnectionState {
    writer: tokio::net::tcp::OwnedWriteHalf,
    last_used: std::time::Instant,
    send_count: usize,
}

// SMROL TCP网络管理器
#[derive(Debug)]
pub struct SmrolTcpNetwork {
    node_id: usize,
    listen_port: u16,
    peer_nodes: HashMap<usize, SocketAddr>, // 其他节点的地址

    // 连接池管理 (与Pompe一致的嵌套异步锁)
    connections:
        Arc<tokio::sync::RwLock<HashMap<usize, Arc<tokio::sync::Mutex<TcpConnectionState>>>>>,

    // 消息通道
    // 消息通道 - 分类处理
    pnfifo_tx: mpsc::UnboundedSender<(usize, SmrolMessage)>,
    pnfifo_rx: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<(usize, SmrolMessage)>>>,

    sequencing_tx: mpsc::UnboundedSender<(usize, SmrolMessage)>,
    sequencing_rx: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<(usize, SmrolMessage)>>>,

    consensus_tx: mpsc::UnboundedSender<(usize, SmrolMessage)>,
    consensus_rx: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<(usize, SmrolMessage)>>>,

    outbound_tx: mpsc::UnboundedSender<SmrolNetworkMessage>,
    outbound_rx: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<SmrolNetworkMessage>>>,

    peer_writers: Arc<RwLock<HashMap<usize, mpsc::Sender<Vec<u8>>>>>,

    // 消息去重机制
    sent_messages: Arc<std::sync::Mutex<HashMap<String, u64>>>,

    // 统计信息
    sent_messages_count: Arc<std::sync::atomic::AtomicU64>,
    received_messages_count: Arc<std::sync::atomic::AtomicU64>,

    // 专用运行时隔离
    rt: Option<Arc<Runtime>>,
}

impl SmrolTcpNetwork {
    fn peer_queue_capacity() -> usize {
        std::env::var("SMROL_PEER_QUEUE_CAP")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(4096)
    }

    fn runtime_handle(&self) -> Option<Handle> {
        self.rt.as_ref().map(|runtime| runtime.handle().clone())
    }

    fn spawn_on_runtime<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        if let Some(handle) = self.runtime_handle() {
            handle.spawn(future);
        } else {
            tokio::spawn(future);
        }
    }

    pub fn new(node_id: usize, peer_nodes: HashMap<usize, SocketAddr>) -> Self {
        // SMROL使用专用端口范围: 21000 + node_id
        let listen_port = 21000 + node_id as u16;

        let (pnfifo_tx, pnfifo_rx) = mpsc::unbounded_channel();
        let (sequencing_tx, sequencing_rx) = mpsc::unbounded_channel();
        let (consensus_tx, consensus_rx) = mpsc::unbounded_channel();
        let (outbound_tx, outbound_rx) = mpsc::unbounded_channel();

        // 创建专用运行时
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
            "🌐 [SMROL-TCP] Node {} 初始化TCP网络，监听端口: {}",
            node_id, listen_port
        );
        info!("🔗 [SMROL-TCP] Node {} 对等节点: {:?}", node_id, peer_nodes);

        let network = Self {
            node_id,
            listen_port,
            peer_nodes,
            connections: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            pnfifo_tx,
            pnfifo_rx: Arc::new(tokio::sync::Mutex::new(pnfifo_rx)),
            sequencing_tx,
            sequencing_rx: Arc::new(tokio::sync::Mutex::new(sequencing_rx)),
            consensus_tx,
            consensus_rx: Arc::new(tokio::sync::Mutex::new(consensus_rx)),
            outbound_tx,
            outbound_rx: Arc::new(tokio::sync::Mutex::new(outbound_rx)),
            peer_writers: Arc::new(RwLock::new(HashMap::new())),
            sent_messages: Arc::new(std::sync::Mutex::new(HashMap::new())),
            sent_messages_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            received_messages_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            rt: Some(runtime.clone()),
        };

        // 启动连接维护任务
        network.start_connection_maintenance();
        network.spawn_peer_writers();
        network
    }

    // 连接维护任务 (与Pompe一致)
    fn start_connection_maintenance(&self) {
        let connections = Arc::clone(&self.connections);
        let sent_messages = Arc::clone(&self.sent_messages);
        let node_id = self.node_id;

        self.spawn_on_runtime(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));

            loop {
                interval.tick().await;

                let mut connections_guard = connections.write().await;
                let mut to_remove = Vec::new();

                for (&target_node_id, conn_state) in connections_guard.iter() {
                    if conn_state.lock().await.last_used.elapsed() > Duration::from_secs(600) {
                        to_remove.push(target_node_id);
                    }
                }

                if !to_remove.is_empty() {
                    for node_id_to_remove in to_remove {
                        connections_guard.remove(&node_id_to_remove);
                        info!(
                            "🧹 [SMROL-TCP] Node {} 清理到节点{}的空闲连接",
                            node_id, node_id_to_remove
                        );
                    }
                }
                drop(connections_guard);

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
                        entries.sort_by_key(|(_, v)| *v);
                        for (k, _) in entries.into_iter().take(sent.len() / 2) {
                            sent.remove(&k);
                        }
                    }
                }
            }
        });
    }

    fn spawn_peer_writers(&self) {
        for (&peer_id, &addr) in &self.peer_nodes {
            if peer_id == self.node_id {
                continue;
            }

            let (tx, rx) = mpsc::channel::<Vec<u8>>(Self::peer_queue_capacity());
            let writers_map = Arc::clone(&self.peer_writers);
            let tx_for_map = tx.clone();
            self.spawn_on_runtime(async move {
                writers_map.write().await.insert(peer_id, tx_for_map);
            });

            self.spawn_on_runtime(peer_writer_task(peer_id, addr, rx, tx));
        }
    }

    async fn queue_frame(&self, target_id: usize, frame: Vec<u8>) -> Result<(), String> {
        if target_id == self.node_id {
            return Err("attempted to queue frame to self".into());
        }

        let sender = self.ensure_peer_writer(target_id).await?;
        use tokio::sync::mpsc::error::TrySendError;

        match sender.try_send(frame.clone()) {
            Ok(_) => Ok(()),
            Err(TrySendError::Full(_)) => sender
                .send(frame)
                .await
                .map_err(|e| format!("发送队列已关闭: {}", e)),
            Err(TrySendError::Closed(_)) => Err("发送队列已关闭".into()),
        }
    }

    async fn ensure_peer_writer(&self, target_id: usize) -> Result<mpsc::Sender<Vec<u8>>, String> {
        {
            let readers = self.peer_writers.read().await;
            if let Some(sender) = readers.get(&target_id) {
                return Ok(sender.clone());
            }
        }

        let addr = *self
            .peer_nodes
            .get(&target_id)
            .ok_or_else(|| format!("未知目标节点 {}", target_id))?;

        let (tx, rx) = mpsc::channel::<Vec<u8>>(Self::peer_queue_capacity());
        {
            let mut writers = self.peer_writers.write().await;
            if let Some(existing) = writers.get(&target_id) {
                return Ok(existing.clone());
            }
            writers.insert(target_id, tx.clone());
        }

        self.spawn_on_runtime(peer_writer_task(target_id, addr, rx, tx.clone()));

        Ok(tx)
    }

    fn frame_message(message: &SmrolNetworkMessage) -> Result<Vec<u8>, String> {
        let serialized = bincode::serialize(message).map_err(|e| format!("序列化失败: {}", e))?;
        let mut frame = Vec::with_capacity(4 + serialized.len());
        frame.extend_from_slice(&(serialized.len() as u32).to_be_bytes());
        frame.extend_from_slice(&serialized);
        Ok(frame)
    }

    pub fn get_pnfifo_receiver(
        &self,
    ) -> Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<(usize, SmrolMessage)>>> {
        Arc::clone(&self.pnfifo_rx)
    }

    pub fn get_sequencing_receiver(
        &self,
    ) -> Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<(usize, SmrolMessage)>>> {
        Arc::clone(&self.sequencing_rx)
    }

    pub fn get_consensus_receiver(
        &self,
    ) -> Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<(usize, SmrolMessage)>>> {
        Arc::clone(&self.consensus_rx)
    }

    // 启动TCP服务器监听 (使用专用运行时)
    pub async fn start_server(&self) -> Result<(), String> {
        let addr = format!("0.0.0.0:{}", self.listen_port);
        let listener = TcpListener::bind(&addr)
            .await
            .map_err(|e| format!("绑定地址{}失败: {}", addr, e))?;

        let pnfifo_tx = self.pnfifo_tx.clone();
        let sequencing_tx = self.sequencing_tx.clone();
        let consensus_tx = self.consensus_tx.clone();
        let node_id = self.node_id;
        let received_messages_count = Arc::clone(&self.received_messages_count);
        self.spawn_on_runtime(async move {
            info!("🎧 [SMROL-TCP] Node {} 服务器启动，监听: {}", node_id, addr);

            loop {
                match listener.accept().await {
                    Ok((stream, peer_addr)) => {
                        info!("🔌 [SMROL-TCP] Node {} 接受连接: {}", node_id, peer_addr);

                        let pnfifo_tx_clone = pnfifo_tx.clone();
                        let sequencing_tx_clone = sequencing_tx.clone();
                        let consensus_tx_clone = consensus_tx.clone();
                        let received_messages_count_clone = Arc::clone(&received_messages_count);

                        tokio::spawn(async move {
                            if let Err(e) = Self::handle_incoming_with_dispatch(
                                stream,
                                pnfifo_tx_clone,
                                sequencing_tx_clone,
                                consensus_tx_clone,
                                node_id,
                                received_messages_count_clone,
                            )
                            .await
                            {
                                error!(
                                    "❌ [SMROL-TCP] Node {} 处理连接{}失败: {}",
                                    node_id, peer_addr, e
                                );
                            }
                        });
                    }
                    Err(e) => {
                        error!("❌ [SMROL-TCP] Node {} 接受连接失败: {}", node_id, e);
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        });

        self.start_outbound_processor().await;

        Ok(())
    }

    // 处理入站连接 (添加消息去重)
    async fn handle_incoming_with_dispatch(
        mut stream: TcpStream,
        pnfifo_tx: mpsc::UnboundedSender<(usize, SmrolMessage)>,
        sequencing_tx: mpsc::UnboundedSender<(usize, SmrolMessage)>,
        consensus_tx: mpsc::UnboundedSender<(usize, SmrolMessage)>,
        node_id: usize,
        received_messages_count: Arc<std::sync::atomic::AtomicU64>,
    ) -> Result<(), String> {
        let peer_addr = stream
            .peer_addr()
            .map_err(|e| format!("获取对端地址失败: {}", e))?;

        debug!(
            "📡 [SMROL-TCP] Node {} 开始处理来自{}的消息",
            node_id, peer_addr
        );

        // 消息去重缓存
        let mut processed_messages = std::collections::HashSet::new();

        loop {
            // 读取消息长度
            let mut length_buf = [0u8; 4];
            match stream.read_exact(&mut length_buf).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    debug!("🔌 [SMROL-TCP] Node {} 连接{}正常关闭", node_id, peer_addr);
                    break;
                }
                Err(e) => {
                    error!("❌ [SMROL-TCP] Node {} 读取长度失败: {}", node_id, e);
                    break;
                }
            }

            let message_length = u32::from_be_bytes(length_buf) as usize;

            // 安全检查
            if message_length > 10 * 1024 * 1024 {
                // 10MB限制
                error!(
                    "❌ [SMROL-TCP] Node {} 消息过大: {} bytes",
                    node_id, message_length
                );
                break;
            }

            if message_length == 0 {
                debug!("💓 [SMROL-TCP] Node {} 收到心跳包", node_id);
                continue;
            }

            // 读取消息内容
            let mut message_buf = vec![0u8; message_length];
            if let Err(e) = stream.read_exact(&mut message_buf).await {
                error!("❌ [SMROL-TCP] Node {} 读取消息内容失败: {}", node_id, e);
                break;
            }

            // 反序列化消息
            match bincode::deserialize::<SmrolNetworkMessage>(&message_buf) {
                Ok(smrol_msg) => {
                    // 消息去重检查
                    if processed_messages.contains(&smrol_msg.message_id) {
                        debug!(
                            "🔄 [SMROL-TCP] Node {} 跳过重复消息: {}",
                            node_id,
                            &smrol_msg.message_id[0..8]
                        );
                        continue;
                    }
                    processed_messages.insert(smrol_msg.message_id.clone());

                    // 限制去重缓存大小
                    if processed_messages.len() > 1000 {
                        processed_messages.clear();
                    }

                    let count = received_messages_count
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                        + 1;
                    let sender_id = smrol_msg.from_node_id;

                    // 根据消息类型分发到不同通道
                    let sender_id = smrol_msg.from_node_id;
                    match &smrol_msg.message {
                        SmrolMessage::PnfifoProposal { .. }
                        | SmrolMessage::PnfifoVote { .. }
                        | SmrolMessage::PnfifoFinal { .. } => {
                            if let Err(e) = pnfifo_tx.send((sender_id, smrol_msg.message)) {
                                error!("❌ [SMROL-TCP] PNFIFO消息分发失败: {}", e);
                            } else {
                                debug!("📨 [SMROL-TCP] Node {} 分发PNFIFO消息到处理器", node_id);
                            }
                        }
                        SmrolMessage::SeqRequest { .. }
                        | SmrolMessage::SeqResponse { .. }
                        | SmrolMessage::SeqOrder { .. }
                        | SmrolMessage::SeqFinal { .. } => {
                            if let Err(e) = sequencing_tx.send((sender_id, smrol_msg.message)) {
                                error!("❌ [SMROL-TCP] Sequencing消息分发失败: {}", e);
                            } else {
                                debug!(
                                    "📨 [SMROL-TCP] Node {} 分发Sequencing消息到处理器",
                                    node_id
                                );
                            }
                        }
                        SmrolMessage::ConsensusProposal { .. }
                        | SmrolMessage::ConsensusVote { .. } => {
                            if let Err(e) = consensus_tx.send((sender_id, smrol_msg.message)) {
                                error!("❌ [SMROL-TCP] Consensus消息分发失败: {}", e);
                            } else {
                                debug!("📨 [SMROL-TCP] Node {} 分发Consensus消息到处理器", node_id);
                            }
                        }
                        SmrolMessage::Warmup => {
                            debug!("💓 [SMROL-TCP] Node {} 收到预热消息", node_id);
                        }
                    }

                    debug!(
                        "📨 [SMROL-TCP] Node {} 接收第{}条消息，来自节点{}",
                        node_id, count, smrol_msg.from_node_id
                    );
                }
                Err(e) => {
                    error!("❌ [SMROL-TCP] Node {} 反序列化消息失败: {}", node_id, e);
                }
            }
        }

        Ok(())
    }

    // 启动出站消息处理 (并行广播优化)
    pub async fn start_outbound_processor(&self) {
        let outbound_rx = Arc::clone(&self.outbound_rx);
        let node_id = self.node_id;
        let self_clone = self.clone();

        self.spawn_on_runtime(async move {
            info!("📤 [SMROL-TCP] Node {} 启动出站消息处理器", node_id);

            let mut rx = outbound_rx.lock().await;

            while let Some(smrol_msg) = rx.recv().await {
                if let Some(target_id) = smrol_msg.to_node_id {
                    if target_id == node_id {
                        if let Err(e) = self_clone.deliver_to_local_processors(&smrol_msg).await {
                            error!("❌ [SMROL-TCP] Node {} 本地派发消息失败: {}", node_id, e);
                        }
                        continue;
                    }

                    match SmrolTcpNetwork::frame_message(&smrol_msg) {
                        Ok(frame) => {
                            if let Err(e) = self_clone.queue_frame(target_id, frame).await {
                                error!(
                                    "❌ [SMROL-TCP] Node {} 发送消息到{}失败: {}",
                                    node_id, target_id, e
                                );
                            }
                        }
                        Err(e) => {
                            error!("❌ [SMROL-TCP] Node {} 序列化消息失败: {}", node_id, e);
                        }
                    }
                } else {
                    match SmrolTcpNetwork::frame_message(&smrol_msg) {
                        Ok(frame) => {
                            for (&peer_id, _) in &self_clone.peer_nodes {
                                if peer_id == node_id {
                                    if let Err(e) =
                                        self_clone.deliver_to_local_processors(&smrol_msg).await
                                    {
                                        error!(
                                            "❌ [SMROL-TCP] Node {} 本地派发消息失败: {}",
                                            node_id, e
                                        );
                                    }
                                    continue;
                                }

                                if let Err(e) = self_clone.queue_frame(peer_id, frame.clone()).await
                                {
                                    warn!(
                                        "⚠️ [SMROL-TCP] Node {} 广播到{}失败: {}",
                                        node_id, peer_id, e
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            error!("❌ [SMROL-TCP] Node {} 序列化消息失败: {}", node_id, e);
                        }
                    }
                }
            }
        });
    }

    // 发送消息（公共接口） - 添加自消息绕过 + 消息去重
    pub async fn send_message(&self, mut message: SmrolNetworkMessage) -> Result<(), String> {
        // 自消息绕过优化
        if let Some(target_id) = message.to_node_id {
            if target_id == self.node_id {
                // debug!(
                //     "📨 [SMROL-TCP] Node {} 发送消息给自己: 直接绕过TCP",
                //     self.node_id
                // );
                return self.deliver_to_local_processors(&message).await;
            }
        }

        // 生成消息ID用于去重
        if message.message_id.is_empty() {
            message.message_id = format!(
                "{}:{}:{}",
                self.node_id,
                message.to_node_id.unwrap_or(9999),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            );
        }

        // 检查是否已发送过
        {
            let mut sent = self.sent_messages.lock().unwrap();
            if sent.contains_key(&message.message_id) {
                warn!(
                    "❗️ [SMROL-TCP] Node {} 跳过重复消息: {}",
                    self.node_id,
                    &message.message_id[0..8]
                );
                return Ok(());
            }
            sent.insert(
                message.message_id.clone(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            );
        }

        self.outbound_tx
            .send(message)
            .map_err(|e| format!("发送消息到队列失败: {}", e))
    }

    // pub async fn receive_message(&self) -> Option<SmrolNetworkMessage> {
    //     let mut rx = self.inbound_rx.lock().await;
    //     rx.recv().await
    // }

    async fn deliver_to_local_processors(
        &self,
        message: &SmrolNetworkMessage,
    ) -> Result<(), String> {
        let sender_id = message.from_node_id;
        match &message.message {
            SmrolMessage::PnfifoProposal { .. }
            | SmrolMessage::PnfifoVote { .. }
            | SmrolMessage::PnfifoFinal { .. } => self
                .pnfifo_tx
                .send((sender_id, message.message.clone()))
                .map_err(|e| format!("本地PNFIFO消息投递失败: {}", e)),
            SmrolMessage::SeqRequest { .. }
            | SmrolMessage::SeqResponse { .. }
            | SmrolMessage::SeqOrder { .. }
            | SmrolMessage::SeqFinal { .. } => self
                .sequencing_tx
                .send((sender_id, message.message.clone()))
                .map_err(|e| format!("本地Sequencing消息投递失败: {}", e)),
            SmrolMessage::ConsensusProposal { .. } | SmrolMessage::ConsensusVote { .. } => self
                .consensus_tx
                .send((sender_id, message.message.clone()))
                .map_err(|e| format!("本地Consensus消息投递失败: {}", e)),
            SmrolMessage::Warmup => Ok(()),
        }
    }

    // 预热连接 (与Pompe一致)
    pub fn warm_up_connections(&self) {
        let peers: Vec<usize> = self
            .peer_nodes
            .keys()
            .cloned()
            .filter(|&nid| nid != self.node_id)
            .collect();
        let net = self.clone();

        self.spawn_on_runtime(async move {
            for nid in peers {
                // 发送预热消息 (假设SmrolMessage有Warmup变体)
                let warmup_msg = SmrolNetworkMessage {
                    from_node_id: net.node_id,
                    to_node_id: Some(nid),
                    message: SmrolMessage::default(), // 需要根据实际SmrolMessage调整
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_micros() as u64,
                    message_id: format!("warmup_{}_{}", net.node_id, nid),
                };
                let _ = net.send_message(warmup_msg).await;
            }
            info!("🔌 [SMROL-TCP] Node {} 连接预热任务完成", net.node_id);
        });
    }

    // 启动统计报告 (使用专用运行时)
    pub async fn start_stats_reporter(&self) {
        let sent_messages_count = Arc::clone(&self.sent_messages_count);
        let received_messages_count = Arc::clone(&self.received_messages_count);
        let connections = Arc::clone(&self.connections);
        let node_id = self.node_id;

        self.spawn_on_runtime(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));

            loop {
                interval.tick().await;

                let sent = sent_messages_count.load(std::sync::atomic::Ordering::Relaxed);
                let received = received_messages_count.load(std::sync::atomic::Ordering::Relaxed);
                let active_connections = connections.read().await.len();

                if sent > 0 || received > 0 {
                    info!(
                        "📊 [SMROL-TCP] Node {} 网络统计: 发送={}, 接收={}, 活跃连接={}",
                        node_id, sent, received, active_connections
                    );
                }
            }
        });
    }

    // 获取网络统计
    pub async fn get_stats(&self) -> (u64, u64, usize) {
        let sent = self
            .sent_messages_count
            .load(std::sync::atomic::Ordering::Relaxed);
        let received = self
            .received_messages_count
            .load(std::sync::atomic::Ordering::Relaxed);
        let active_connections = self.connections.read().await.len();

        (sent, received, active_connections)
    }

    // 获取连接池统计 (与Pompe一致)
    pub async fn get_connection_stats(&self) -> (usize, usize) {
        let connections = self.connections.read().await;
        let active_connections = connections.len();
        let mut total_messages: usize = 0;
        for conn in connections.values() {
            total_messages += conn.lock().await.send_count;
        }

        if active_connections > 0 {
            info!(
                "🔗 [SMROL-TCP] Node {} 连接池状态: 活跃连接={}, 总发送数={}",
                self.node_id, active_connections, total_messages
            );
        }

        (active_connections, total_messages)
    }

    // spawn方法 (与Pompe一致)
    pub fn spawn<F>(&self, fut: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        self.spawn_on_runtime(fut);
    }

    // for sequencing
    /// 广播SEQ-REQUEST消息
    pub async fn multicast_seq_request(
        &self,
        req: crate::smrol::sequencing::SeqRequest,
    ) -> Result<(), String> {
        // 将Transaction转换为SmrolTransaction
        let smrol_tx: SmrolTransaction = bincode::deserialize(&req.tx.payload)
            .map_err(|e| format!("反序列化SmrolTransaction失败: {}", e))?;

        let message = SmrolMessage::SeqRequest {
            tx_hash: hex::encode(&req.tx.payload[0..std::cmp::min(8, req.tx.payload.len())]),
            transaction: smrol_tx,
            sender_id: self.node_id,
            sequence_number: req.seq_num,
        };

        let network_msg = SmrolNetworkMessage {
            from_node_id: self.node_id,
            to_node_id: None, // 广播消息
            message,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            message_id: format!("seq_req_{}_{}", self.node_id, uuid::Uuid::new_v4()),
        };

        debug!(
            "📤 [SMROL-TCP] Node {} 广播SEQ-REQUEST, k = {}",
            self.node_id, req.seq_num
        );

        self.send_message(network_msg).await
    }

    /// 广播SEQ-RESPONSE消息  
    pub async fn multicast_seq_response(
        &self,
        resp: crate::smrol::sequencing::SeqResponse,
    ) -> Result<(), String> {
        use crate::smrol::message::SmrolMessage;

        let message = SmrolMessage::SeqResponse {
            vc: resp.vc,
            signature_share: resp.sigma,
            sender_id: self.node_id,
            sequence_number: resp.s,
        };

        let network_msg = SmrolNetworkMessage {
            from_node_id: self.node_id,
            to_node_id: None, // 广播消息
            message,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            message_id: format!("seq_resp_{}_{}", self.node_id, uuid::Uuid::new_v4()),
        };

        debug!(
            "📤 [SMROL-TCP] Node {} 广播SEQ-RESPONSE, sequence s = {}",
            self.node_id, resp.s
        );

        self.send_message(network_msg).await
    }

    /// 广播SEQ-ORDER消息
    pub async fn multicast_seq_order(
        &self,
        order: crate::smrol::sequencing::SeqOrder,
    ) -> Result<(), String> {
        use crate::smrol::message::SmrolMessage;

        let responses: Vec<(usize, u64, Vec<u8>)> = order
            .records
            .iter()
            .map(|record| (record.sender, record.sequence, record.signature.clone()))
            .collect();

        let sequences: Vec<u64> = responses.iter().map(|(_, seq, _)| *seq).collect();
        let median = self.calculate_median(&sequences);

        let message = SmrolMessage::SeqOrder {
            vc: order.vc.clone(),
            responses,
            sender_id: self.node_id,
        };

        let network_msg = SmrolNetworkMessage {
            from_node_id: self.node_id,
            to_node_id: None, // 广播消息
            message,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            message_id: format!("seq_order_{}_{}", self.node_id, uuid::Uuid::new_v4()),
        };

        debug!(
            "📤 [SMROL-TCP] Node {} 广播SEQ-ORDER, median: {}",
            self.node_id, median
        );

        self.send_message(network_msg).await
    }

    /// 发送SEQ-MEDIAN消息到指定节点 (注意：现有message.rs没有SeqMedian，用SeqOrder代替)
    pub async fn send_seq_median(
        &self,
        to: usize,
        median: crate::smrol::sequencing::SeqMedian,
    ) -> Result<(), String> {
        use crate::smrol::message::SmrolMessage;

        // 由于现有message.rs没有SeqMedian，我们复用SeqOrder结构来携带单个记录
        let message = SmrolMessage::SeqOrder {
            vc: median.vc.clone(),
            responses: vec![(self.node_id, median.s_tx, median.sigma_seq.clone())],
            sender_id: self.node_id,
        };

        let network_msg = SmrolNetworkMessage {
            from_node_id: self.node_id,
            to_node_id: Some(to), // 点对点消息
            message,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            message_id: format!(
                "seq_median_{}_{}_{}",
                self.node_id,
                to,
                uuid::Uuid::new_v4()
            ),
        };

        debug!(
            "📤 [SMROL-TCP] Node {} 发送SEQ-MEDIAN到节点{}, s_tx: {}",
            self.node_id, to, median.s_tx
        );

        self.send_message(network_msg).await
    }

    /// 广播SEQ-FINAL消息
    pub async fn multicast_seq_final(
        &self,
        final_msg: crate::smrol::sequencing::SeqFinal,
    ) -> Result<(), String> {
        use crate::smrol::message::SmrolMessage;

        let message = SmrolMessage::SeqFinal {
            vc: final_msg.vc,
            final_sequence: final_msg.s_tx,
            combined_signature: final_msg.sigma,
            sender_id: self.node_id,
        };

        let network_msg = SmrolNetworkMessage {
            from_node_id: self.node_id,
            to_node_id: None, // 广播消息
            message,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            message_id: format!("seq_final_{}_{}", self.node_id, uuid::Uuid::new_v4()),
        };

        debug!(
            "📤 [SMROL-TCP] Node {} 广播SEQ-FINAL, s_tx: {}",
            self.node_id, final_msg.s_tx
        );

        self.send_message(network_msg).await
    }

    // 辅助方法：计算中位数
    fn calculate_median(&self, s_vec: &[u64]) -> u64 {
        if s_vec.is_empty() {
            return 0;
        }
        let mut sorted = s_vec.to_vec();
        sorted.sort();
        sorted[sorted.len() / 2]
    }
}

impl Drop for SmrolTcpNetwork {
    fn drop(&mut self) {
        // Gracefully tear down the dedicated runtime even when we are currently
        // executing inside another Tokio runtime (e.g. #[tokio::test]).
        if let Some(rt) = self.rt.take() {
            if Arc::strong_count(&rt) == 1 {
                if let Ok(runtime) = Arc::try_unwrap(rt) {
                    runtime.shutdown_background();
                }
            }
        }
    }
}

// Clone实现 (与Pompe一致)
impl Clone for SmrolTcpNetwork {
    fn clone(&self) -> Self {
        Self {
            node_id: self.node_id,
            listen_port: self.listen_port,
            peer_nodes: self.peer_nodes.clone(),
            connections: Arc::clone(&self.connections),
            pnfifo_tx: self.pnfifo_tx.clone(),
            pnfifo_rx: Arc::clone(&self.pnfifo_rx),
            sequencing_tx: self.sequencing_tx.clone(),
            sequencing_rx: Arc::clone(&self.sequencing_rx),
            consensus_tx: self.consensus_tx.clone(),
            consensus_rx: Arc::clone(&self.consensus_rx),
            outbound_tx: self.outbound_tx.clone(),
            outbound_rx: Arc::clone(&self.outbound_rx),
            peer_writers: Arc::clone(&self.peer_writers),
            sent_messages: Arc::clone(&self.sent_messages),
            sent_messages_count: Arc::clone(&self.sent_messages_count),
            received_messages_count: Arc::clone(&self.received_messages_count),
            rt: self.rt.as_ref().map(Arc::clone),
        }
    }
}

async fn peer_writer_task(
    peer_id: usize,
    addr: SocketAddr,
    mut rx: mpsc::Receiver<Vec<u8>>,
    retry_tx: mpsc::Sender<Vec<u8>>,
) {
    let mut backoff = Duration::from_millis(100);
    let mut ping = interval(Duration::from_secs(15));

    loop {
        let mut stream = match TcpStream::connect(addr).await {
            Ok(s) => {
                let _ = s.set_nodelay(true);
                backoff = Duration::from_millis(100);
                debug!("peer {} 已连接 {}", peer_id, addr);
                s
            }
            Err(e) => {
                warn!(
                    "peer {} 连接失败 {}: {}，等待 {:?}",
                    peer_id, addr, e, backoff
                );
                sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(5));
                continue;
            }
        };

        loop {
            tokio::select! {
                biased;
                _ = ping.tick() => {
                    if let Err(e) = stream.write_all(&0u32.to_be_bytes()).await {
                        warn!("peer {} ping 写失败: {}", peer_id, e);
                        break;
                    }
                }
                maybe = rx.recv() => {
                    match maybe {
                        Some(frame) => {
                            if let Err(e) = stream.write_all(&frame).await {
                                warn!("peer {} 写失败: {}，重连", peer_id, e);
                                if let Err(send_err) = retry_tx.send(frame).await {
                                    warn!(
                                        "peer {} 重投队列失败: {} (消息丢弃)",
                                        peer_id, send_err
                                    );
                                }
                                break;
                            }
                        }
                        None => {
                            debug!("peer {} 发送端关闭", peer_id);
                            return;
                        }
                    }
                }
            }
        }
    }
}
