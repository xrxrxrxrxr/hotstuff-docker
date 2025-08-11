// hotstuff_runner/src/network.rs
use hotstuff_rs::{
    networking::{
        network::Network,
        messages::Message,
    },
    types::{
        validator_set::ValidatorSet,
        update_sets::ValidatorSetUpdates,
        crypto_primitives::VerifyingKey,
    },
};
use std::sync::{Arc, Mutex, mpsc};
use std::collections::HashMap;
use log::{debug, info, error};

// 全局网络管理器
pub struct TestNetwork {
    senders: HashMap<VerifyingKey, mpsc::Sender<(VerifyingKey, Message)>>,
}

impl TestNetwork {
    pub fn new() -> Self {
        Self {
            senders: HashMap::new(),
        }
    }

    pub fn register(&mut self, key: VerifyingKey, sender: mpsc::Sender<(VerifyingKey, Message)>) {
        self.senders.insert(key, sender);
        info!("🔗 注册节点: {:?}", key.to_bytes()[0..4].to_vec());
    }

    pub fn registered_count(&self) -> usize {
        self.senders.len()
    }
}

// 每个节点的网络接口 - 简化版本，避免过度克隆
#[derive(Clone)]
pub struct NodeNetwork {
    my_key: VerifyingKey,
    shared_network: Arc<Mutex<TestNetwork>>,
    receiver: Arc<Mutex<mpsc::Receiver<(VerifyingKey, Message)>>>,
}

impl NodeNetwork {
    pub fn new(my_key: VerifyingKey, shared_network: Arc<Mutex<TestNetwork>>) -> Self {
        let (sender, receiver) = mpsc::channel();
        
        // 立即注册，避免延迟
        {
            let mut network = shared_network.lock().unwrap();
            network.register(my_key, sender);
        }

        Self {
            my_key,
            shared_network,
            receiver: Arc::new(Mutex::new(receiver)),
        }
    }
}

impl Network for NodeNetwork {
    fn init_validator_set(&mut self, validator_set: ValidatorSet) {
        info!("🏗️ 节点 {:?} 初始化验证者集合: {} 个验证者", 
              self.my_key.to_bytes()[0..4].to_vec(),
              validator_set.len());
        // 不做额外的网络配置，保持简单
    }

    fn update_validator_set(&mut self, _updates: ValidatorSetUpdates) {
        info!("🔄 节点 {:?} 更新验证者集合", 
              self.my_key.to_bytes()[0..4].to_vec());
        // 不做额外的网络配置，保持简单
    }

    fn broadcast(&mut self, message: Message) {
        let network = self.shared_network.lock().unwrap();
        let sent_count = network.senders.len();
        
        if sent_count == 0 {
            error!("❌ 节点 {:?} 广播失败：没有接收者", 
                   self.my_key.to_bytes()[0..4].to_vec());
            return;
        }
        
        let mut success_count = 0;
        for (_, sender) in &network.senders {
            if sender.send((self.my_key, message.clone())).is_ok() {
                success_count += 1;
            }
        }
        
        debug!("📡 节点 {:?} 广播给 {}/{} 个节点", 
               self.my_key.to_bytes()[0..4].to_vec(), 
               success_count, sent_count);
    }

    fn send(&mut self, peer: VerifyingKey, message: Message) {
        let network = self.shared_network.lock().unwrap();
        
        if let Some(sender) = network.senders.get(&peer) {
            let success = sender.send((self.my_key, message)).is_ok();
            if success {
                debug!("📨 节点 {:?} 发送成功给 {:?}", 
                       self.my_key.to_bytes()[0..4].to_vec(),
                       peer.to_bytes()[0..4].to_vec());
            } else {
                error!("❌ 节点 {:?} 发送失败给 {:?}", 
                       self.my_key.to_bytes()[0..4].to_vec(),
                       peer.to_bytes()[0..4].to_vec());
            }
        } else {
            error!("❌ 节点 {:?} 找不到目标 {:?}", 
                   self.my_key.to_bytes()[0..4].to_vec(),
                   peer.to_bytes()[0..4].to_vec());
        }
    }

    fn recv(&mut self) -> Option<(VerifyingKey, Message)> {
        let receiver = self.receiver.lock().unwrap();
        match receiver.try_recv() {
            Ok(msg) => {
                debug!("📬 节点 {:?} 接收消息来自 {:?}", 
                      self.my_key.to_bytes()[0..4].to_vec(),
                      msg.0.to_bytes()[0..4].to_vec());
                Some(msg)
            }
            Err(mpsc::TryRecvError::Empty) => None,
            Err(mpsc::TryRecvError::Disconnected) => {
                error!("❌ 节点 {:?} 接收通道断开", 
                       self.my_key.to_bytes()[0..4].to_vec());
                None
            }
        }
    }
}

// 创建网络的便利函数
pub fn create_mock_network(verifying_keys: Vec<VerifyingKey>) -> (Arc<Mutex<TestNetwork>>, Vec<NodeNetwork>) {
    let shared_network = Arc::new(Mutex::new(TestNetwork::new()));
    
    info!("🌐 创建网络，准备 {} 个节点", verifying_keys.len());
    
    let node_networks: Vec<NodeNetwork> = verifying_keys
        .into_iter()
        .enumerate()
        .map(|(i, key)| {
            info!("   创建节点 {} 网络: {:?}", i, key.to_bytes()[0..4].to_vec());
            NodeNetwork::new(key, shared_network.clone())
        })
        .collect();
    
    // 验证注册
    {
        let network = shared_network.lock().unwrap();
        let count = network.registered_count();
        if count == node_networks.len() {
            info!("✅ 网络初始化成功：{} 个节点已注册", count);
        } else {
            error!("❌ 网络初始化失败：期望 {} 个节点，实际 {} 个", 
                   node_networks.len(), count);
        }
    }
    
    (shared_network, node_networks)
}