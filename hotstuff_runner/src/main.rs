use std::{sync::Arc, time::Duration, fs};
use tokio::sync::Mutex;
use tokio::io::AsyncWriteExt;

// 简化版本，使用自定义日志而不是tracing，避免全局订阅器冲突

// 自定义日志宏
macro_rules! node_log {
    ($logger:expr, $($arg:tt)*) => {
        $logger.log(format!($($arg)*)).await;
    };
}

// 简单的日志记录器
#[derive(Clone)]
struct NodeLogger {
    node_id: u32,
    file: Arc<Mutex<tokio::fs::File>>,
}

impl NodeLogger {
    async fn new(node_id: u32) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        fs::create_dir_all("logs")?;
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(format!("logs/log{}.log", node_id))
            .await?;
        
        Ok(Self {
            node_id,
            file: Arc::new(Mutex::new(file)),
        })
    }

    async fn log(&self, message: String) {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let log_line = format!("[{}] Node {}: {}\n", timestamp, self.node_id, message);
        
        let mut file = self.file.lock().await;
        if let Err(e) = file.write_all(log_line.as_bytes()).await {
            eprintln!("Failed to write to log file for node {}: {}", self.node_id, e);
        }
        if let Err(e) = file.flush().await {
            eprintln!("Failed to flush log file for node {}: {}", self.node_id, e);
        }
    }
}

// 简单的计数器应用
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum CounterTransaction {
    Increment,
    Decrement,
    Set(u64),
}

#[derive(Clone)]
pub struct CounterApp {
    node_id: u32,
    state: Arc<Mutex<u64>>,
    logger: NodeLogger,
}

impl CounterApp {
    pub async fn new(node_id: u32, logger: NodeLogger) -> Self {
        Self {
            node_id,
            state: Arc::new(Mutex::new(0)),
            logger,
        }
    }

    pub async fn get_value(&self) -> u64 {
        *self.state.lock().await
    }

    async fn apply_transaction(&self, tx: &CounterTransaction) {
        let mut state = self.state.lock().await;
        let old_value = *state;
        match tx {
            CounterTransaction::Increment => *state += 1,
            CounterTransaction::Decrement => *state = state.saturating_sub(1),
            CounterTransaction::Set(value) => *state = *value,
        }
        node_log!(self.logger, "Applied transaction {:?}: {} -> {}", tx, old_value, *state);
    }
}

// 简化的网络模拟
#[derive(Clone)]
struct MockNetwork {
    node_id: u32,
    peers: Vec<u32>,
    logger: NodeLogger,
}

impl MockNetwork {
    fn new(node_id: u32, peers: Vec<u32>, logger: NodeLogger) -> Self {
        Self { node_id, peers, logger }
    }

    async fn send_to_peer(&self, peer_id: u32, message: &str) {
        node_log!(self.logger, "Sending to Node {}: {}", peer_id, message);
        // 模拟网络延迟
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    async fn broadcast(&self, message: &str) {
        node_log!(self.logger, "Broadcasting: {}", message);
        for &peer_id in &self.peers {
            if peer_id != self.node_id {
                self.send_to_peer(peer_id, message).await;
            }
        }
    }

    async fn simulate_receive(&self) -> Option<(u32, String)> {
        // 模拟偶尔接收到消息
        if rand::random::<f32>() < 0.2 { // 20%概率
            let sender_idx = (rand::random::<u32>() as usize) % self.peers.len();
            let sender = self.peers[sender_idx];
            if sender != self.node_id {
                let message = format!("Hello from Node {}", sender);
                node_log!(self.logger, "Received message from Node {}: {}", sender, message);
                return Some((sender, message));
            }
        }
        None
    }
}

// 节点结构体
struct Node {
    id: u32,
    app: CounterApp,
    network: MockNetwork,
    transaction_queue: Arc<Mutex<Vec<CounterTransaction>>>,
    logger: NodeLogger,
}

impl Node {
    async fn new(id: u32, peers: Vec<u32>) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let logger = NodeLogger::new(id).await?;
        let app = CounterApp::new(id, logger.clone()).await;
        let network = MockNetwork::new(id, peers, logger.clone());
        
        Ok(Self {
            id,
            app,
            network,
            transaction_queue: Arc::new(Mutex::new(Vec::new())),
            logger,
        })
    }

    async fn submit_transaction(&self, tx: CounterTransaction) {
        node_log!(self.logger, "Received transaction: {:?}", tx);
        let mut queue = self.transaction_queue.lock().await;
        queue.push(tx);
    }

    async fn process_transactions(&self) {
        let mut queue = self.transaction_queue.lock().await;
        if !queue.is_empty() {
            node_log!(self.logger, "Processing {} transactions", queue.len());
            for tx in queue.drain(..) {
                self.app.apply_transaction(&tx).await;
                
                // 广播交易给其他节点
                let tx_json = serde_json::to_string(&tx).unwrap_or_default();
                self.network.broadcast(&format!("TX: {}", tx_json)).await;
            }
        }
    }

    async fn simulate_consensus_round(&self, round: u32) {
        node_log!(self.logger, "Starting consensus round {}", round);
        
        // 模拟提议阶段
        if self.id == (round % 4) { // 轮流做leader
            node_log!(self.logger, "Is leader for round {}", round);
            self.network.broadcast(&format!("PROPOSE: Round {}", round)).await;
        }
        
        // 模拟投票阶段
        tokio::time::sleep(Duration::from_millis(100)).await;
        self.network.broadcast(&format!("VOTE: Round {}", round)).await;
        
        // 模拟提交阶段
        tokio::time::sleep(Duration::from_millis(100)).await;
        if self.id == (round % 4) {
            node_log!(self.logger, "Committing round {}", round);
            self.network.broadcast(&format!("COMMIT: Round {}", round)).await;
        }
    }

    async fn run(&self) {
        node_log!(self.logger, "Starting main loop");
        
        for round in 0..5 {
            // 模拟接收网络消息
            if let Some((sender, message)) = self.network.simulate_receive().await {
                node_log!(self.logger, "Processed message from Node {}: {}", sender, message);
            }
            
            // 处理交易队列
            self.process_transactions().await;
            
            // 模拟共识轮次
            self.simulate_consensus_round(round).await;
            
            // 随机提交交易
            if rand::random::<f32>() < 0.3 { // 30%概率
                match rand::random::<u32>() % 3 {
                    0 => self.submit_transaction(CounterTransaction::Increment).await,
                    1 => self.submit_transaction(CounterTransaction::Decrement).await,
                    _ => self.submit_transaction(CounterTransaction::Set((rand::random::<u32>() % 100) as u64)).await,
                }
            }
            
            // 输出当前状态
            let current_value = self.app.get_value().await;
            node_log!(self.logger, "Current counter value: {}", current_value);
            
            // 轮次间隔
            tokio::time::sleep(Duration::from_millis(800)).await;
        }
        
        let final_value = self.app.get_value().await;
        node_log!(self.logger, "Completed. Final counter value: {}", final_value);
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("🚀 Starting HotStuff 4-Node Network Simulation");

    // 创建logs目录
    fs::create_dir_all("logs")?;
    println!("📁 Created logs directory");

    // 定义四个节点的网络拓扑
    let all_peers = vec![0, 1, 2, 3];
    println!("🌐 Network topology: 4 fully connected nodes");

    // 创建并启动4个节点任务
    let mut handles = Vec::new();
    
    for node_id in 0..4u32 {
        let peers = all_peers.clone();
        
        let handle = tokio::spawn(async move {
            // 创建并运行节点
            match Node::new(node_id, peers).await {
                Ok(node) => {
                    node_log!(node.logger, "🎬 Starting up");
                    node_log!(node.logger, "Connected to peers: {:?}", 
                             node.network.peers.iter().filter(|&&p| p != node_id).collect::<Vec<_>>());
                    
                    node_log!(node.logger, "Initialized successfully");
                    
                    // 运行节点主循环
                    node.run().await;
                    
                    node_log!(node.logger, "Shutting down");
                },
                Err(e) => {
                    eprintln!("Failed to create node {}: {}", node_id, e);
                }
            }
        });
        
        handles.push(handle);
        println!("✅ Started node {}", node_id);
    }

    println!("⏰ All 4 nodes started, running simulation...");
    
    // 等待所有节点完成
    for (i, handle) in handles.into_iter().enumerate() {
        if let Err(e) = handle.await {
            println!("❌ Node {} error: {}", i, e);
        } else {
            println!("✅ Node {} completed", i);
        }
    }

    println!("🎉 All nodes completed successfully!");
    println!("📊 Simulation Results:");
    println!("   - 4 nodes ran in parallel");
    println!("   - Each node processed transactions independently");
    println!("   - Mock consensus rounds were executed");
    println!("   - Network communication was simulated");
    println!();
    println!("📋 Check the detailed logs:");
    for i in 0..4 {
        println!("   - Node {}: logs/log{}.log", i, i);
    }

    Ok(())
}