// hotstuff_runner/src/app.rs - lock-free version
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
    // Use a lock-free queue instead of Mutex<Vec<String>>
    tx_queue: Arc<SegQueue<String>>,
}

const NUMBER_KEY: [u8; 1] = [0];

impl TestApp {
    pub fn new(node_id: usize, tx_queue: Arc<SegQueue<String>>) -> Self {
        info!(
            "Creating TestApp for Node {} (queue address: {:p})",
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

        info!("Node {} [{}] consensus state:", self.node_id, operation);
        info!("Node {}   timestamp: {}", self.node_id, timestamp);
        info!("Node {}   current view: {}", self.node_id, view);
        info!("Node {}   block height: {}", self.node_id, height);
        info!(
            "Node {}   node block count: {}",
            self.node_id, self.block_count
        );
        info!(
            "Node {}   pending transactions: {}",
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
        // thread::sleep(Duration::from_millis(25));
        let view_number = request.cur_view().int();
        let produce_start = std::time::Instant::now();
        // warn!("[produce_block] Node {} Producing block for view {} (only current view)", self.node_id, view_number);

        // Pull transactions from the lock-free queue without locking
        let mut transactions = Vec::new();
        // Maximum transactions per block; configurable via APP_BLOCK_MAX_TX (default 800)
        let max_tx_count: usize = std::env::var("APP_BLOCK_MAX_TX")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(800);

        // Check queue size first to avoid unnecessary loops
        let queue_size = self.tx_queue.len();
        let actual_max = std::cmp::min(max_tx_count, queue_size);
        debug!(
            "Node {} [produce_block] queue size: {}, attempting up to {} transactions this block",
            self.node_id, queue_size, actual_max
        );
        // if queue_size != 0 {
        //     info!(
        //         "Node {} [produce_block] queue size: {}, attempting up to {} transactions this block",
        //         self.node_id, queue_size, actual_max
        //     );
        // }

        if actual_max > 0 {
            transactions.reserve(actual_max); // Pre-allocate capacity

            // Use a tighter loop
            while transactions.len() < max_tx_count {
                if let Some(tx) = self.tx_queue.pop() {
                    debug!(
                        "Node {} [produce_block] fetched transaction from queue: {}",
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
            "Node {} [produce_block] pulled {} transactions from the lock-free queue (limit {})",
            self.node_id, tx_count, max_tx_count
        );

        // Build block data
        let mut data_vec = Vec::new();

        // Add the view number as the first datum
        let view_number = request.cur_view().int();
        let view_bytes = view_number.to_le_bytes().to_vec();
        data_vec.push(Datum::new(view_bytes));

        // Log the relationship between view number and block count
        if self.block_count > 0 && view_number > self.block_count + 100 {
            warn!(
                "Node {} view({}) far exceeds block count ({}); potential sync issue",
                self.node_id, view_number, self.block_count
            );
        }

        // Add the transaction count
        let tx_count_bytes = (tx_count as u32).to_le_bytes().to_vec();
        data_vec.push(Datum::new(tx_count_bytes));

        for tx in &transactions {
            data_vec.push(Datum::new(tx.as_bytes().to_vec()));
        }

        // Create Data
        let data = Data::new(data_vec);

        // Compute data hash
        let data_hash = Self::compute_data_hash(&data);

        // Create app state updates
        let mut app_state_updates = AppStateUpdates::new();

        // Update block count
        let block_count_key = format!("block_count_{}", self.node_id);
        let block_count_value = (self.block_count + 1).to_string();
        app_state_updates.insert(block_count_key.into_bytes(), block_count_value.into_bytes());

        // Store block hash
        let block_hash_key = format!("block_{}", request.cur_view());
        app_state_updates.insert(block_hash_key.into_bytes(), data_hash.bytes().to_vec());

        info!(
            "Node {} [produce_block] transaction pool state:",
            self.node_id
        );
        info!(
            "Node {} [produce_block]  - local pending transactions: {}",
            self.node_id,
            self.tx_queue.len()
        );
        info!(
            "Node {} [produce_block]  - transactions included in this block: {}",
            self.node_id, tx_count
        );
        info!(
            "Node {} [produce_block]  - block data hash: {:?}",
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
            // Option A: avoid non-essential state writes; determinism derives solely from block data
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

        // Basic validation logic
        if block.data.len().int() == 0 {
            info!(
                "[Node {}] Block validation failed: empty data",
                self.node_id
            );
            return ValidateBlockResponse::Invalid;
        }

        // Validate data hash
        let computed_hash = Self::compute_data_hash(&block.data);
        if computed_hash != block.data_hash {
            info!(
                "[Node {}] Block validation failed: hash mismatch",
                self.node_id
            );
            return ValidateBlockResponse::Invalid;
        }

        // Validate data format
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

        // Validate transaction count datum
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

        // Normalize key space: only store keys uniquely determined by block metadata (consistent across replicas)
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
        // thread::sleep(Duration::from_millis(25));
        self.validate_block_for_sync(request)
    }
}
