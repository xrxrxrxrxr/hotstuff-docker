use borsh::BorshSerialize;
// hotstuff_runner/src/app.rs
use hotstuff_rs::{
    app::{App, ProduceBlockRequest, ProduceBlockResponse, ValidateBlockRequest, ValidateBlockResponse},
    block_tree::pluggables::KVStore,
    types::{
        data_types::{CryptoHash, Data, Datum},
        update_sets::AppStateUpdates,
    },
};
use log::info;
use sha2::{Sha256, Digest};
use std::{convert::Infallible, sync::{Arc, Mutex}};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone)]
pub struct TestApp {
    node_id: usize,
    block_count: u64,
    // 模拟的交易池
    // pending_transactions: Vec<String>,
    // 改为外部传入的共享交易队列
    tx_queue: Arc<Mutex<Vec<String>>>,
}

const NUMBER_KEY: [u8; 1] = [0];

impl TestApp {
    pub fn new(node_id: usize, tx_queue: Arc<Mutex<Vec<String>>>) -> Self {
        info!("🆕 创建 TestApp 实例 for Node {} (队列地址: {:p})", node_id, &*tx_queue);
        Self {
            node_id,
            block_count: 0,
            tx_queue,
        }
    }

    // 添加详细的状态日志
    fn log_consensus_state(&self, operation: &str, view: u64, height: u64) {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        info!("Node {} [{}] 🔄 共识状态追踪:", self.node_id, operation);
        info!("Node {}   📍 时间戳: {}", self.node_id, timestamp);
        info!("Node {}   🔢 当前View: {}", self.node_id, view);
        info!("Node {}   📏 区块高度: {}", self.node_id, height);
        info!("Node {}   📊 节点区块计数: {}", self.node_id, self.block_count);
        info!("Node {}   🎯 待处理交易: {}", self.node_id, {
            let queue = self.tx_queue.lock().unwrap();
            queue.len()
        });
    }

    // pub fn add_transaction(&mut self, tx: String) {
    //     self.pending_transactions.push(tx.clone());
    //     info!("[{}] ➕ 添加交易到 TestApp: {} (总数: {}, 线程: {:?})", 
    //           self.node_id, 
    //           tx, 
    //           self.pending_transactions.len(),
    //           std::thread::current().id());
    // }

    pub fn initial_app_state() -> AppStateUpdates {
        let mut state = AppStateUpdates::new();
        state.insert(NUMBER_KEY.to_vec(), u32::to_le_bytes(0).to_vec());
        state
    }

    fn compute_data_hash(data: &Data) -> CryptoHash {
        let mut hasher = Sha256::new();
        
        // 遍历 Data 中的每个 Datum
        for datum in data.iter() {
            hasher.update(datum.bytes());
        }
        
        let result = hasher.finalize();
        // CryptoHash 需要32字节数组
        let mut hash_bytes = [0u8; 32];
        hash_bytes.copy_from_slice(&result);
        CryptoHash::new(hash_bytes)
    }
}

impl<K: KVStore> App<K> for TestApp {
    fn produce_block(&mut self, request: ProduceBlockRequest<K>) -> ProduceBlockResponse {
        info!("[produce_block] 🔨 Node {} Producing block for view {}",  self.node_id,request.cur_view());
        self.log_consensus_state("PRODUCE_BLOCK_START", request.cur_view().int(), self.block_count);

        if request.cur_view().int() > 0 && self.block_count + 1 != request.cur_view().int() {
            info!("Node {} [produce_block] ⚠️ View/Height 不匹配! View: {}, 期望Height: {}, 实际Count: {}", 
                  self.node_id, request.cur_view().int(), request.cur_view().int(), self.block_count);
        }


        // 从共享队列获取并清空交易
        let transactions = {
            let mut queue = self.tx_queue.lock().unwrap();
            let tx_count = queue.len().min(300); // 每个区块最多300个交易
            info!("Node {} [produce_block] 📊 queue.len = {}, tx_count = {}", self.node_id, queue.len(), tx_count);

            let mut batch = Vec::new();
            for _ in 0..tx_count {
                if let Some(tx) = queue.pop() {
                    batch.push(tx);
                }
            }
            batch
        };

        let tx_count = transactions.len();
        info!("Node {} [produce_block] 📊 从共享队列获取 tx_count = {} 个交易", self.node_id, tx_count);


        // 创建区块数据
        let mut data_vec = Vec::new();
        
        // 添加视图号作为第一个Datum
        // 直接序列化 u64 而不是 ViewNumber
        let view_number = request.cur_view().int();
        let view_bytes = view_number.to_le_bytes().to_vec();
        data_vec.push(Datum::new(view_bytes));
        
        
        // 添加交易计数, 在data中作为第二个Datum
        // 使用 u32 来存储交易计数
        let tx_count_bytes = (tx_count as u32).to_le_bytes().to_vec();
        data_vec.push(Datum::new(tx_count_bytes));
        
        for tx in &transactions {
            data_vec.push(Datum::new(tx.as_bytes().to_vec()));
            // info!("Node {} [produce_block] 🔨  - 交易: {}", self.node_id, tx);
        }
        

        // 创建Data
        let data = Data::new(data_vec);

        // 计算数据哈希
        let data_hash = Self::compute_data_hash(&data);

        // 创建应用状态更新
        let mut app_state_updates = AppStateUpdates::new();
        
        // 更新区块计数
        let block_count_key = format!("block_count_{}", self.node_id);
        let block_count_value = (self.block_count + 1).to_string();
        app_state_updates.insert(
            block_count_key.into_bytes(), 
            block_count_value.into_bytes()
        );
        
        // 存储区块哈希
        let block_hash_key = format!("block_{}", request.cur_view());
        app_state_updates.insert(
            block_hash_key.into_bytes(), 
            data_hash.bytes().to_vec()
        );

        info!("Node {} [produce_block] 📊 交易池状态:",self.node_id);
        info!("Node {} [produce_block]  - 本地待处理交易: {}", self.node_id,self.tx_queue.lock().unwrap().len());
        info!("Node {} [produce_block]  - 本区块将包含交易: {}",self.node_id, tx_count);
        info!("Node {} [produce_block]  - 本区块数据哈希: {:?}",self.node_id, &data_hash.bytes()[0..8]);

        info!("[Node {}] Produced block with {} transactions", self.node_id, tx_count);

        ProduceBlockResponse {
            data_hash,
            data,
            app_state_updates: Some(app_state_updates),
            validator_set_updates: None, // 不更新验证者集合
        }
    }

    fn validate_block(&mut self, request: ValidateBlockRequest<K>) -> ValidateBlockResponse {
        let block = request.proposed_block();
        info!("[Node {}] Validating block at height {}", self.node_id, block.height);
        let tx_count = if block.data.vec().len() >= 2 {
            let tx_count_bytes = block.data.vec()[1].bytes();
            if tx_count_bytes.len() >= 4 {
                let mut bytes = [0u8; 4];
                bytes.copy_from_slice(&tx_count_bytes[0..4]);
                        u32::from_le_bytes(bytes)
                    } else {
                        0
                    }
                } else {
                    0
                };

        // 添加更详细的日志
        // info!("Node {} [validate_block] 🔍 验证区块详情:",self.node_id);
        // info!("Node {} [validate_block]  - 区块高度: {}",self.node_id, block.height);
        // info!("Node {} [validate_block]  - 交易数量: {}",self.node_id, tx_count);
        // info!("Node {} [validate_block]  - 区块哈希: {:?}",self.node_id, &block.hash);
        // info!("Node {} [validate_block]  - 当前节点区块计数: {}", self.node_id,self.block_count);


        // 基本验证逻辑
        // 1. 检查区块数据是否为空
        if block.data.len().int() == 0 {
            info!("[Node {}] ❌ Block validation failed: empty data", self.node_id);
            return ValidateBlockResponse::Invalid;
        }

        // 2. 验证数据哈希
        let computed_hash = Self::compute_data_hash(&block.data);
        if computed_hash != block.data_hash {
            info!("[Node {}] ❌ Block validation failed: hash mismatch", self.node_id);
            return ValidateBlockResponse::Invalid;
        }

        // 3. 验证数据格式 - 至少需要两个 Datum（视图号和交易计数）
        if block.data.len().int() < 2 {
            info!("[Node {}] ❌ Block validation failed: invalid data format", self.node_id);
            return ValidateBlockResponse::Invalid;
        }

        // 简化验证逻辑 - 暂时移除视图验证，因为API限制
        // 在实际应用中，我们可能需要用其他方式获取当前视图信息
        
        // 解析并验证基本数据结构
        let data_vec = block.data.vec();
        if let Some(view_datum) = data_vec.get(0) {
            // 验证视图号数据格式
            let datum_bytes = view_datum.bytes();
            if datum_bytes.len() < 8 {
                info!("[Node {}] Block validation failed: invalid view datum size", self.node_id);
                return ValidateBlockResponse::Invalid;
            }
        } else {
            info!("[Node {}] Block validation failed: no view datum", self.node_id);
            return ValidateBlockResponse::Invalid;
        }

        // 验证交易计数数据
        if let Some(tx_count_datum) = data_vec.get(1) {
            let datum_bytes = tx_count_datum.bytes();
            if datum_bytes.len() < 4 {
                info!("[Node {}] Block validation failed: invalid transaction count datum", self.node_id);
                return ValidateBlockResponse::Invalid;
            }
            
            // 解析交易计数
            let mut tx_count_bytes = [0u8; 4];
            tx_count_bytes.copy_from_slice(&datum_bytes[0..4]);
            let tx_count = u32::from_le_bytes(tx_count_bytes) as usize;
            
            // 验证数据项数量：视图号 + 交易计数 + 交易数据
            let expected_items = 2 + tx_count;
            if data_vec.len() != expected_items {
                info!("[Node {}] Block validation failed: data item count mismatch. Expected: {}, Got: {}", 
                      self.node_id, expected_items, data_vec.len());
                return ValidateBlockResponse::Invalid;
            }
        } else {
            info!("[Node {}] Block validation failed: no transaction count datum", self.node_id);
            return ValidateBlockResponse::Invalid;
        }

        // 创建状态更新
        let mut app_state_updates = AppStateUpdates::new();
        
        // 更新区块计数
        self.block_count += 1;
        let block_count_key = format!("block_count_{}", self.node_id);
        let block_count_value = self.block_count.to_string();
        app_state_updates.insert(
            block_count_key.into_bytes(), 
            block_count_value.into_bytes()
        );
        
        // 存储区块哈希（使用区块高度作为键）
        let block_hash_key = format!("block_height_{}", block.height);
        let block_hash_key_clone=block_hash_key.clone();
        app_state_updates.insert(
            block_hash_key.into_bytes(), 
            block.data_hash.bytes().to_vec()
        );

        // 检查insert的内容
        // app_state_updates.get_insert(&block_hash_key_clone.into_bytes()).map(|value| {
            // info!("[Node {}] Block hash stored in app state: {:?}", self.node_id, value);
        // });

        info!("[Node {}] Block validation passed, height: {}, TxCount: {}", self.node_id, block.height, tx_count );
        // info!("[Node {}] ✅ 区块验证通过 - 高度: {}", self.node_id, block.height);

        ValidateBlockResponse::Valid {
            app_state_updates: Some(app_state_updates),
            validator_set_updates: None,
        }
    }

    fn validate_block_for_sync(&mut self, request: ValidateBlockRequest<K>) -> ValidateBlockResponse {
        // 同步时的验证逻辑与普通验证相同
        self.validate_block(request)
    }
}