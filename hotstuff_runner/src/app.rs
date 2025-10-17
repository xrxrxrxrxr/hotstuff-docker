// hotstuff_runner/src/app.rs - 无锁版本
use borsh::BorshSerialize;
use crossbeam::channel::{Receiver, TryRecvError};
use crossbeam::queue::{self, SegQueue};
use hotstuff_rs::{
    app::{
        App, ProduceBlockRequest, ProduceBlockResponse, ValidateBlockRequest, ValidateBlockResponse,
    },
    block_tree::pluggables::KVStore,
    types::{
        data_types::{CryptoHash, Data, Datum},
        update_sets::AppStateUpdates,
    },
};
use log::{debug, info, warn};
use sha2::{Digest, Sha256};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{convert::Infallible, sync::Arc};

#[derive(Clone)]
pub struct TestApp {
    node_id: usize,
    block_count: u64,
    // 使用无锁队列替代 Mutex<Vec<String>>
    tx_queue: Arc<SegQueue<String>>,
}

const NUMBER_KEY: [u8; 1] = [0];

impl TestApp {
    pub fn new(node_id: usize, tx_queue: Arc<SegQueue<String>>) -> Self {
        info!(
            "创建 TestApp 实例 for Node {} (队列地址: {:p})",
            node_id, &*tx_queue
        );
        Self {
            node_id,
            block_count: 0,
            tx_queue,
        }
    }

    fn log_consensus_state(&self, operation: &str, view: u64, height: u64) {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        info!("Node {} [{}] 共识状态追踪:", self.node_id, operation);
        info!("Node {}   时间戳: {}", self.node_id, timestamp);
        info!("Node {}   当前View: {}", self.node_id, view);
        info!("Node {}   区块高度: {}", self.node_id, height);
        info!("Node {}   节点区块计数: {}", self.node_id, self.block_count);
        info!(
            "Node {}   待处理交易: {}",
            self.node_id,
            self.tx_queue.len()
        );
    }

    pub fn initial_app_state() -> AppStateUpdates {
        let mut state = AppStateUpdates::new();
        state.insert(NUMBER_KEY.to_vec(), u32::to_le_bytes(0).to_vec());
        state
    }

    fn compute_data_hash(data: &Data) -> CryptoHash {
        let mut hasher = Sha256::new();

        for datum in data.iter() {
            hasher.update(datum.bytes());
        }

        let result = hasher.finalize();
        let mut hash_bytes = [0u8; 32];
        hash_bytes.copy_from_slice(&result);
        CryptoHash::new(hash_bytes)
    }
}

impl<K: KVStore> App<K> for TestApp {
    fn produce_block(&mut self, request: ProduceBlockRequest<K>) -> ProduceBlockResponse {
        thread::sleep(Duration::from_millis(25));
        let view_number = request.cur_view().int();
        let produce_start = std::time::Instant::now();
        // warn!("[produce_block] Node {} Producing block for view {} (only current view)", self.node_id, view_number);

        // 从无锁队列获取交易 - 无需锁定
        let mut transactions = Vec::new();
        // 每个区块最多交易数，可通过 APP_BLOCK_MAX_TX 配置，默认 500
        let max_tx_count: usize = std::env::var("APP_BLOCK_MAX_TX")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(800);

        // 先检查队列大小，避免无效循环
        let queue_size = self.tx_queue.len();
        let actual_max = std::cmp::min(max_tx_count, queue_size);
        info!(
            "Node {} [produce_block] 当前队列大小: {}, 本区块将尝试获取最多 {} 个交易",
            self.node_id, queue_size, actual_max
        );
        if queue_size != 0 {
            warn!(
                "Node {} [produce_block] 当前队列大小: {}, 本区块将尝试获取最多 {} 个交易",
                self.node_id, queue_size, actual_max
            );
        }

        if actual_max > 0 {
            transactions.reserve(actual_max); // 预分配容量

            // 使用更紧凑的循环
            while transactions.len() < max_tx_count {
                if let Some(tx) = self.tx_queue.pop() {
                    debug!(
                        "Node {} [produce_block] 🔨 从队列获取交易: {}",
                        self.node_id, &tx
                    );
                    transactions.push(tx);
                } else {
                    break;
                }
            }
        }

        let tx_count = transactions.len();
        info!(
            "Node {} [produce_block] 从无锁队列获取 {} 个交易 (上限 {})",
            self.node_id, tx_count, max_tx_count
        );

        // 创建区块数据
        let mut data_vec = Vec::new();

        // 添加视图号作为第一个Datum
        let view_number = request.cur_view().int();
        let view_bytes = view_number.to_le_bytes().to_vec();
        data_vec.push(Datum::new(view_bytes));

        // debug 视图号与区块计数的关系
        if self.block_count > 0 && view_number > self.block_count + 100 {
            warn!(
                "Node {} view({})远超区块计数({}), 可能存在同步问题",
                self.node_id, view_number, self.block_count
            );
        }

        // 添加交易计数
        let tx_count_bytes = (tx_count as u32).to_le_bytes().to_vec();
        data_vec.push(Datum::new(tx_count_bytes));

        for tx in &transactions {
            data_vec.push(Datum::new(tx.as_bytes().to_vec()));
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
        app_state_updates.insert(block_count_key.into_bytes(), block_count_value.into_bytes());

        // 存储区块哈希
        let block_hash_key = format!("block_{}", request.cur_view());
        app_state_updates.insert(block_hash_key.into_bytes(), data_hash.bytes().to_vec());

        info!("Node {} [produce_block] 交易池状态:", self.node_id);
        info!(
            "Node {} [produce_block]  - 本地待处理交易: {}",
            self.node_id,
            self.tx_queue.len()
        );
        info!(
            "Node {} [produce_block]  - 本区块将包含交易: {}",
            self.node_id, tx_count
        );
        info!(
            "Node {} [produce_block]  - 本区块数据哈希: {:?}",
            self.node_id,
            &data_hash.bytes()[0..8]
        );

        let produce_elapsed = produce_start.elapsed();
        // warn!("[produce_block] Node {} cost {:?} (tx={})", self.node_id, produce_elapsed, tx_count);
        debug!(
            "[Node {}] Produced block at view {} with {} transactions",
            self.node_id,
            request.cur_view().int(),
            tx_count
        );

        ProduceBlockResponse {
            data_hash,
            data,
            // 方案A：不写入非必要状态，保持确定性（仅由区块数据决定）
            app_state_updates: None,
            validator_set_updates: None,
        }
    }

    fn validate_block_for_sync(
        &mut self,
        request: ValidateBlockRequest<K>,
    ) -> ValidateBlockResponse {
        let validate_start = std::time::Instant::now();
        let block = request.proposed_block();
        info!(
            "[Node {}] Validating block at height {}",
            self.node_id, block.height
        );

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

        // 基本验证逻辑
        if block.data.len().int() == 0 {
            info!(
                "[Node {}] Block validation failed: empty data",
                self.node_id
            );
            return ValidateBlockResponse::Invalid;
        }

        // 验证数据哈希
        let computed_hash = Self::compute_data_hash(&block.data);
        if computed_hash != block.data_hash {
            info!(
                "[Node {}] Block validation failed: hash mismatch",
                self.node_id
            );
            return ValidateBlockResponse::Invalid;
        }

        // 验证数据格式
        if block.data.len().int() < 2 {
            info!(
                "[Node {}] Block validation failed: invalid data format",
                self.node_id
            );
            return ValidateBlockResponse::Invalid;
        }

        let data_vec = block.data.vec();
        let mut produced_view_in_block: Option<u64> = None;
        if let Some(view_datum) = data_vec.get(0) {
            let datum_bytes = view_datum.bytes();
            if datum_bytes.len() < 8 {
                info!(
                    "[Node {}] Block validation failed: invalid view datum size",
                    self.node_id
                );
                return ValidateBlockResponse::Invalid;
            }
            let mut vb = [0u8; 8];
            vb.copy_from_slice(&datum_bytes[0..8]);
            produced_view_in_block = Some(u64::from_le_bytes(vb));
        } else {
            info!(
                "[Node {}] Block validation failed: no view datum",
                self.node_id
            );
            return ValidateBlockResponse::Invalid;
        }

        // 验证交易计数数据
        if let Some(tx_count_datum) = data_vec.get(1) {
            let datum_bytes = tx_count_datum.bytes();
            if datum_bytes.len() < 4 {
                info!(
                    "[Node {}] Block validation failed: invalid transaction count datum",
                    self.node_id
                );
                return ValidateBlockResponse::Invalid;
            }

            let mut tx_count_bytes = [0u8; 4];
            tx_count_bytes.copy_from_slice(&datum_bytes[0..4]);
            let tx_count = u32::from_le_bytes(tx_count_bytes) as usize;

            let expected_items = 2 + tx_count;
            if data_vec.len() != expected_items {
                info!("[Node {}] Block validation failed: data item count mismatch. Expected: {}, Got: {}", 
                      self.node_id, expected_items, data_vec.len());
                return ValidateBlockResponse::Invalid;
            }
        } else {
            info!(
                "[Node {}] Block validation failed: no transaction count datum",
                self.node_id
            );
            return ValidateBlockResponse::Invalid;
        }

        let validate_elapsed = validate_start.elapsed();
        // warn!("[validate_block] Node {} cost {:?}", self.node_id, validate_elapsed);

        // 统一键空间：仅写入由区块元数据唯一决定的键值（各副本一致）
        let mut app_state_updates = AppStateUpdates::new();
        let block_hash_key = format!("block_hash_at_height_{}", block.height.int());
        app_state_updates.insert(
            block_hash_key.into_bytes(),
            block.data_hash.bytes().to_vec(),
        );

        if let Some(vv) = produced_view_in_block {
            info!(
                "[Node {}] Block validation passed, height: {}, TxCount: {}, produced_view: {}, view_gap: {}",
                self.node_id,
                block.height,
                tx_count,
                vv,
                block.height.int().saturating_sub(vv)
            );
        } else {
            info!(
                "[Node {}] Block validation passed, height: {}, TxCount: {}",
                self.node_id, block.height, tx_count
            );
        }

        ValidateBlockResponse::Valid {
            app_state_updates: Some(app_state_updates),
            validator_set_updates: None,
        }
    }

    fn validate_block(&mut self, request: ValidateBlockRequest<K>) -> ValidateBlockResponse {
        thread::sleep(Duration::from_millis(25));
        self.validate_block_for_sync(request)
    }
}
