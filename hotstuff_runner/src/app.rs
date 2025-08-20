// hotstuff_runner/src/app.rs
use borsh::BorshSerialize;
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
use std::{convert::Infallible, sync::{Arc, atomic::{AtomicU64, Ordering}}};
use std::time::{SystemTime, UNIX_EPOCH};

// 导入共享的无锁数据结构
use crate::lockfree_types::LockFreeTransactionQueue;

pub struct TestApp {
    node_id: usize,
    block_count: AtomicU64,  // Lock-free atomic counter
    // Replace mutex-based queue with lock-free version
    tx_queue: Arc<LockFreeTransactionQueue>,
}

impl Clone for TestApp {
    fn clone(&self) -> Self {
        Self {
            node_id: self.node_id,
            block_count: AtomicU64::new(self.block_count.load(Ordering::Relaxed)),
            tx_queue: Arc::clone(&self.tx_queue),
        }
    }
}

const NUMBER_KEY: [u8; 1] = [0];

impl TestApp {
    // Constructor that accepts lock-free queue
    pub fn new(node_id: usize, tx_queue: Arc<LockFreeTransactionQueue>) -> Self {
        info!("Creating TestApp instance for Node {} (lock-free queue: {:p})", node_id, &*tx_queue);
        Self {
            node_id,
            block_count: AtomicU64::new(0),
            tx_queue,
        }
    }
    
    // Alternative constructor for compatibility with mutex-based interface
    pub fn new_with_mutex_queue(node_id: usize, _mutex_queue: Arc<std::sync::Mutex<Vec<String>>>) -> Self {
        // Create a new lock-free queue instead of using the mutex one
        let lockfree_queue = Arc::new(LockFreeTransactionQueue::new());
        info!("Creating TestApp instance for Node {} (converted to lock-free)", node_id);
        Self {
            node_id,
            block_count: AtomicU64::new(0),
            tx_queue: lockfree_queue,
        }
    }

    // Lock-free state logging
    fn log_consensus_state(&self, operation: &str, view: u64, height: u64) {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        let current_block_count = self.block_count.load(Ordering::Relaxed);
        let pending_tx_count = self.tx_queue.len();

        info!("Node {} [{}] Consensus state tracking:", self.node_id, operation);
        info!("Node {}   Timestamp: {}", self.node_id, timestamp);
        info!("Node {}   Current View: {}", self.node_id, view);
        info!("Node {}   Block height: {}", self.node_id, height);
        info!("Node {}   Node block count: {}", self.node_id, current_block_count);
        info!("Node {}   Pending transactions: {}", self.node_id, pending_tx_count);
    }

    pub fn initial_app_state() -> AppStateUpdates {
        let mut state = AppStateUpdates::new();
        state.insert(NUMBER_KEY.to_vec(), u32::to_le_bytes(0).to_vec());
        state
    }

    fn compute_data_hash(data: &Data) -> CryptoHash {
        let mut hasher = Sha256::new();
        
        // Iterate through each Datum in Data
        for datum in data.iter() {
            hasher.update(datum.bytes());
        }
        
        let result = hasher.finalize();
        // CryptoHash requires 32-byte array
        let mut hash_bytes = [0u8; 32];
        hash_bytes.copy_from_slice(&result);
        CryptoHash::new(hash_bytes)
    }
    
    // Get reference to lock-free queue for external access
    pub fn get_lockfree_queue(&self) -> Arc<LockFreeTransactionQueue> {
        Arc::clone(&self.tx_queue)
    }
    
    // Lock-free transaction submission
    pub fn submit_transaction(&self, tx: String) {
        self.tx_queue.push(tx.clone());
        info!("Node {} submitted transaction to lock-free queue: {}", self.node_id, tx);
    }
    
    // Lock-free batch transaction submission
    pub fn submit_transactions(&self, transactions: Vec<String>) {
        let tx_count = transactions.len();
        for tx in transactions {
            self.tx_queue.push(tx.clone());
        }
        info!("Node {} submitted {} transactions to lock-free queue", self.node_id, tx_count);
    }
}

impl<K: KVStore> App<K> for TestApp {
    fn produce_block(&mut self, request: ProduceBlockRequest<K>) -> ProduceBlockResponse {
        info!("[produce_block] Node {} Producing block for view {}", self.node_id, request.cur_view());
        
        let current_block_count = self.block_count.load(Ordering::Relaxed);
        self.log_consensus_state("PRODUCE_BLOCK_START", request.cur_view().int(), current_block_count);

        if request.cur_view().int() > 0 && current_block_count + 1 != request.cur_view().int() {
            info!("Node {} [produce_block] View/Height mismatch! View: {}, Expected Height: {}, Actual Count: {}", 
                  self.node_id, request.cur_view().int(), request.cur_view().int(), current_block_count);
        }

        // Lock-free transaction batch retrieval
        let max_tx_per_block = 300; // Maximum 300 transactions per block
        let queue_size = self.tx_queue.len();
        let tx_count = queue_size.min(max_tx_per_block);
        
        info!("Node {} [produce_block] Queue size: {}, taking: {} transactions", 
              self.node_id, queue_size, tx_count);

        // Use lock-free batch drain
        let transactions = self.tx_queue.drain_batch(tx_count);
        let actual_tx_count = transactions.len();
        
        info!("Node {} [produce_block] Retrieved {} transactions from lock-free queue", 
              self.node_id, actual_tx_count);

        // Create block data
        let mut data_vec = Vec::new();
        
        // Add view number as first Datum
        let view_number = request.cur_view().int();
        let view_bytes = view_number.to_le_bytes().to_vec();
        data_vec.push(Datum::new(view_bytes));
        
        // Add transaction count as second Datum
        let tx_count_bytes = (actual_tx_count as u32).to_le_bytes().to_vec();
        data_vec.push(Datum::new(tx_count_bytes));
        
        // Add transaction data
        for tx in &transactions {
            data_vec.push(Datum::new(tx.as_bytes().to_vec()));
            info!("Node {} [produce_block] - Transaction: {}", self.node_id, tx);
        }

        // Create Data
        let data = Data::new(data_vec);

        // Compute data hash
        let data_hash = Self::compute_data_hash(&data);

        // Create application state updates
        let mut app_state_updates = AppStateUpdates::new();
        
        // Update block count atomically
        let new_block_count = self.block_count.fetch_add(1, Ordering::Relaxed) + 1;
        let block_count_key = format!("block_count_{}", self.node_id);
        let block_count_value = new_block_count.to_string();
        app_state_updates.insert(
            block_count_key.into_bytes(), 
            block_count_value.into_bytes()
        );
        
        // Store block hash
        let block_hash_key = format!("block_{}", request.cur_view());
        app_state_updates.insert(
            block_hash_key.into_bytes(), 
            data_hash.bytes().to_vec()
        );

        info!("Node {} [produce_block] Transaction pool status:", self.node_id);
        info!("Node {} [produce_block]  - Remaining pending transactions: {}", self.node_id, self.tx_queue.len());
        info!("Node {} [produce_block]  - Block contains transactions: {}", self.node_id, actual_tx_count);
        info!("Node {} [produce_block]  - Block data hash: {:?}", self.node_id, &data_hash.bytes()[0..8]);

        info!("[Node {}] Produced block with {} transactions", self.node_id, actual_tx_count);

        ProduceBlockResponse {
            data_hash,
            data,
            app_state_updates: Some(app_state_updates),
            validator_set_updates: None, // Don't update validator set
        }
    }

    fn validate_block(&mut self, request: ValidateBlockRequest<K>) -> ValidateBlockResponse {
        let block = request.proposed_block();
        info!("[Node {}] Validating block at height {}", self.node_id, block.height);
        
        // Parse transaction count
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
        // 1. Check if block data is empty
        if block.data.len().int() == 0 {
            info!("[Node {}] Block validation failed: empty data", self.node_id);
            return ValidateBlockResponse::Invalid;
        }

        // 2. Verify data hash
        let computed_hash = Self::compute_data_hash(&block.data);
        if computed_hash != block.data_hash {
            info!("[Node {}] Block validation failed: hash mismatch", self.node_id);
            return ValidateBlockResponse::Invalid;
        }

        // 3. Verify data format - need at least two Datum (view number and transaction count)
        if block.data.len().int() < 2 {
            info!("[Node {}] Block validation failed: invalid data format", self.node_id);
            return ValidateBlockResponse::Invalid;
        }

        // Parse and verify basic data structure
        let data_vec = block.data.vec();
        if let Some(view_datum) = data_vec.get(0) {
            // Verify view number data format
            let datum_bytes = view_datum.bytes();
            if datum_bytes.len() < 8 {
                info!("[Node {}] Block validation failed: invalid view datum size", self.node_id);
                return ValidateBlockResponse::Invalid;
            }
        } else {
            info!("[Node {}] Block validation failed: no view datum", self.node_id);
            return ValidateBlockResponse::Invalid;
        }

        // Verify transaction count data
        if let Some(tx_count_datum) = data_vec.get(1) {
            let datum_bytes = tx_count_datum.bytes();
            if datum_bytes.len() < 4 {
                info!("[Node {}] Block validation failed: invalid transaction count datum", self.node_id);
                return ValidateBlockResponse::Invalid;
            }
            
            // Parse transaction count
            let mut tx_count_bytes = [0u8; 4];
            tx_count_bytes.copy_from_slice(&datum_bytes[0..4]);
            let tx_count = u32::from_le_bytes(tx_count_bytes) as usize;
            
            // Verify data item count: view number + transaction count + transaction data
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

        // Create state updates
        let mut app_state_updates = AppStateUpdates::new();
        
        // Update block count atomically
        let new_block_count = self.block_count.fetch_add(1, Ordering::Relaxed) + 1;
        let block_count_key = format!("block_count_{}", self.node_id);
        let block_count_value = new_block_count.to_string();
        app_state_updates.insert(
            block_count_key.into_bytes(), 
            block_count_value.into_bytes()
        );
        
        // Store block hash (using block height as key)
        let block_hash_key = format!("block_height_{}", block.height);
        app_state_updates.insert(
            block_hash_key.into_bytes(), 
            block.data_hash.bytes().to_vec()
        );

        info!("[Node {}] Block validation passed, height: {}, TxCount: {}", self.node_id, block.height, tx_count);

        ValidateBlockResponse::Valid {
            app_state_updates: Some(app_state_updates),
            validator_set_updates: None,
        }
    }

    fn validate_block_for_sync(&mut self, request: ValidateBlockRequest<K>) -> ValidateBlockResponse {
        // Sync validation logic is the same as normal validation
        self.validate_block(request)
    }
}