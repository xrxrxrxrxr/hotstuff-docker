use std::{collections::HashMap, sync::Arc, time::Duration, fs};
use tokio::sync::Mutex;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

// 简化版本，先不实现复杂的HotStuff traits，专注于四节点并行运行和日志分离

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
}

impl CounterApp {
    pub fn new(node_id: u32) -> Self {
        Self {
            node_id,
            state: Arc::new(Mutex::new(0)),
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
        info!("Node {} applied transaction {:?}: {} -> {}", 
              self.node_id, tx, old_value, *state);
    }
}

// 简化的网络模拟
#[derive(Clone)]
struct MockNetwork {
    node_id: u32,
    peers: Vec<u32>,
}

impl MockNetwork {
    fn new(node_id: u32, peers: Vec<u32>) -> Self {
        Self { node_id, peers }
    }

    async fn send_to_peer(&self, peer_id: u32, message: &str) {
        info!("Node {} sending to Node {}: {}", self.node_id, peer_id, message);
        // 模拟网络延迟
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    async fn broadcast(&self, message: &str) {
        info!("Node {} broadcasting: {}", self.node_id, message);
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
                info!("Node {} received message from Node {}: {}", 
                      self.node_id, sender, message);
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
}

impl Node {
    fn new(id: u32, peers: Vec<u32>) -> Self {
        Self {
            id,
            app: CounterApp::new(id),
            network: MockNetwork::new(id, peers),
            transaction_queue: Arc::new(Mutex::new(Vec::new())),
        }
    }

    async fn submit_transaction(&self, tx: CounterTransaction) {
        info!("Node {} received transaction: {:?}", self.id, tx);
        let mut queue = self.transaction_queue.lock().await;
        queue.push(tx);
    }

    async fn process_transactions(&self) {
        let mut queue = self.transaction_queue.lock().await;
        if !queue.is_empty() {
            info!("Node {} processing {} transactions", self.id, queue.len());
            for tx in queue.drain(..) {
                self.app.apply_transaction(&tx).await;
                
                // 广播交易给其他节点
                let tx_json = serde_json::to_string(&tx).unwrap_or_default();
                self.network.broadcast(&format!("TX: {}", tx_json)).await;
            }
        }
    }

    async fn simulate_consensus_round(&self, round: u32) {
        info!("Node {} starting consensus round {}", self.id, round);
        
        // 模拟提议阶段
        if self.id == (round % 4) { // 轮流做leader
            info!("Node {} is leader for round {}", self.id, round);
            self.network.broadcast(&format!("PROPOSE: Round {}", round)).await;
        }
        
        // 模拟投票阶段
        tokio::time::sleep(Duration::from_millis(100)).await;
        self.network.broadcast(&format!("VOTE: Round {}", round)).await;
        
        // 模拟提交阶段
        tokio::time::sleep(Duration::from_millis(100)).await;
        if self.id == (round % 4) {
            info!("Node {} committing round {}", self.id, round);
            self.network.broadcast(&format!("COMMIT: Round {}", round)).await;
        }
    }

    async fn run(&self) {
        info!("Node {} starting main loop", self.id);
        
        for round in 0..5 {
            // 模拟接收网络消息
            if let Some((sender, message)) = self.network.simulate_receive().await {
                info!("Node {} processed message from Node {}: {}", self.id, sender, message);
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
            info!("Node {} current counter value: {}", self.id, current_value);
            
            // 轮次间隔
            tokio::time::sleep(Duration::from_millis(800)).await;
        }
        
        let final_value = self.app.get_value().await;
        info!("Node {} completed. Final counter value: {}", self.id, final_value);
    }
}

// 设置每个节点的日志输出
fn setup_node_logging(node_id: u32) -> Result<(), Box<dyn std::error::Error>> {
    // 创建logs目录
    fs::create_dir_all("logs")?;
    
    // 创建日志文件
    let log_file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(format!("logs/log{}.log", node_id))?;

    // 设置tracing subscriber，只输出到文件
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(log_file)
                .with_ansi(false) // 文件中不需要颜色代码
                .with_target(false)
                .with_level(true)
                .with_thread_ids(true)
                .with_thread_names(true)
        )
        .init();

    Ok(())
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
            // 为每个节点设置独立的日志
            if let Err(e) = setup_node_logging(node_id) {
                eprintln!("Failed to setup logging for node {}: {}", node_id, e);
                return;
            }

            info!("🎬 Node {} starting up", node_id);
            info!("Node {} connected to peers: {:?}", node_id, 
                  peers.iter().filter(|&&p| p != node_id).collect::<Vec<_>>());
            
            // 创建并运行节点
            let node = Node::new(node_id, peers);
            
            info!("Node {} initialized successfully", node_id);
            
            // 运行节点主循环
            node.run().await;
            
            info!("Node {} shutting down", node_id);
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