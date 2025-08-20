// crate/src/node.rs
use hotstuff_rs::{
    block_tree::{pluggables::KVGet, variables::HIGHEST_COMMITTED_BLOCK}, 
    events::*, 
    replica::{self, Configuration, Replica, ReplicaSpec}, 
    types::{
        crypto_primitives::VerifyingKey,
        data_types::{BufferSize, ChainID, EpochLength, ViewNumber, Data},
        block::Block,
        update_sets::{AppStateUpdates, ValidatorSetUpdates},
        validator_set::{ValidatorSet, ValidatorSetState},
    }
};
use crate::{
    app::TestApp,
    network::NodeNetwork,
    tcp_network::TcpNetwork,
    kv_store::MemoryKVStore,
    stats::PerformanceStats,
    lockfree_types::{LockFreeTransactionQueue, LockFreePerformanceStats, LockFreeQueueAdapter},
};
use std::time::Duration;
use std::sync::Arc;
use ed25519_dalek::SigningKey;
use tracing::{info, warn};

pub struct Node {
    verifying_key: VerifyingKey,
    replica: Replica<MemoryKVStore>,
    node_id: usize,
    // Replace mutex-based queue with lock-free version
    tx_queue: Arc<LockFreeTransactionQueue>,
    // Replace mutex-based stats with lock-free version
    lockfree_stats: Arc<LockFreePerformanceStats>,
}

impl Node {
    pub fn new(
        node_id: usize,
        keypair: SigningKey,
        network: TcpNetwork,
        init_app_state_updates: AppStateUpdates,
        init_validator_set_updates: ValidatorSetUpdates,
        _compatible_queue: Arc<std::sync::Mutex<Vec<String>>>, // Keep for compatibility but don't use
        original_stats: Arc<std::sync::Mutex<PerformanceStats>>, // Keep for compatibility
    ) -> Self {
        let verifying_key: VerifyingKey = keypair.verifying_key().into();
        
        info!("Creating lock-free Node, verifying key: {:?}", verifying_key.to_bytes()[0..8].to_vec());
        
        // Create lock-free replacements
        let tx_queue = Arc::new(LockFreeTransactionQueue::new());
        let lockfree_stats = Arc::new(LockFreePerformanceStats::new());
        
        // Setup validator set
        let mut initial_validator_set = ValidatorSet::new();
        initial_validator_set.apply_updates(&init_validator_set_updates);
        
        info!("Node validator set: {} validators, total power: {}", 
             initial_validator_set.len(), 
             initial_validator_set.total_power().int());
        
        let validator_set_state = ValidatorSetState::new(
            initial_validator_set.clone(),
            initial_validator_set.clone(),
            None,
            true,
        );
        
        let kv_store = MemoryKVStore::new();
        
        Replica::initialize(
            kv_store.clone(),
            init_app_state_updates,
            validator_set_state,
        );
        
        let app = TestApp::new(node_id, Arc::clone(&tx_queue));
        
        let config = Configuration::builder()
            .me(keypair)
            .chain_id(ChainID::new(0))
            .block_sync_request_limit(10)
            .block_sync_server_advertise_time(Duration::new(10, 0))
            .block_sync_response_timeout(Duration::new(3, 0))
            .block_sync_blacklist_expiry_time(Duration::new(10, 0))
            .block_sync_trigger_min_view_difference(2)
            .block_sync_trigger_timeout(Duration::new(60, 0))
            .progress_msg_buffer_capacity(BufferSize::new(1024))
            .epoch_length(EpochLength::new(50))
            .max_view_time(Duration::from_millis(2000))
            .log_events(false)
            .build();

        let kv_clone_commit = kv_store.clone();
        let stats_for_commit = lockfree_stats.clone();

        let replica = ReplicaSpec::builder()
            .app(app)
            .network(network)
            .kv_store(kv_store)
            .configuration(config)
            .on_start_view({
                move |event| {
                    let msg = format!("🚀 Node {} started View {}", node_id, event.view);
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_propose({
                move |event| {
                    let msg = format!(
                        "📤 Node {} proposed block, View: {}, height: {:?}, hash: {:?}",
                        node_id,
                        event.proposal.view,
                        event.proposal.block.height,
                        event.proposal.block.hash
                    );
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_receive_proposal({
                move |event| {
                    let msg = format!(
                        "📥 Node {} received proposal, from: {:?}, View: {}",
                        node_id,
                        event.origin.to_bytes()[0..4].to_vec(),
                        event.proposal.view
                    );
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_phase_vote({
                move |event| {
                    let msg = format!(
                        "🗳️ Node {} phase vote, View: {}, phase: {:?}",
                        node_id,
                        event.vote.view,
                        event.vote.phase
                    );
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_receive_phase_vote({
                move |event| {
                    let msg = format!(
                        "📨 Node {} received vote, from: {:?}, View: {}, phase: {:?}",
                        node_id,
                        event.origin.to_bytes()[0..4].to_vec(),
                        event.phase_vote.view,
                        event.phase_vote.phase
                    );
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_collect_pc({
                move |event| {
                    let msg = format!(
                        "🎯 Node {} collected PC, View: {}, signatures: {}",
                        node_id,
                        event.phase_certificate.view,
                        event.phase_certificate.signatures.iter().filter(|sig| sig.is_some()).count()
                    );
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_commit_block({
                move |event| {
                    let block_hash = event.block;
                    let commit_time = event.timestamp;
                    
                    match kv_clone_commit.block(&block_hash) {
                        Ok(Some(block)) => {
                            let height = block.height.int();
                            
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
                            
                            // Lock-free statistics update
                            stats_for_commit.record_confirmed(tx_count as usize);
                            
                            let submission_tps = stats_for_commit.get_submission_tps();
                            let confirmation_tps = stats_for_commit.get_confirmation_tps();
                            let total_confirmed_txs = stats_for_commit.get_confirmed_transactions();
                            let total_confirmed_blocks = stats_for_commit.get_confirmed_blocks();
                            
                            let msg = format!(
                                "💎 Node {} Commit block - Height: {}, TxCount: {}, Submit_TPS: {:.2}, Confirm_TPS: {:.2}, TotalTxs: {}, TotalBlocks: {}",
                                node_id, height, tx_count, submission_tps, confirmation_tps, total_confirmed_txs, total_confirmed_blocks
                            );
                            crate::log_node(node_id, log::Level::Info, &msg);

                            // Periodic detailed analysis
                            if total_confirmed_blocks % 10 == 0 {
                                info!("📊 Node {} consensus statistics report (block #{}):", node_id, total_confirmed_blocks);
                                info!("  📥 Submission TPS: {:.2}", submission_tps);
                                info!("  🎯 Confirmation TPS: {:.2}", confirmation_tps);
                                info!("  📈 Confirmed transactions: {}", total_confirmed_txs);
                                info!("  📦 Confirmed blocks: {}", total_confirmed_blocks);
                                
                                if submission_tps > confirmation_tps * 1.2 {
                                    warn!("⚠️ Transaction backlog detected: submission({:.1}) > confirmation({:.1})", 
                                        submission_tps, confirmation_tps);
                                }
                            }
                        },
                        Ok(None) => {
                            let msg = format!(
                                "💎 Node {} committed block - hash: {:?} (block details not found)",
                                node_id, &block_hash.bytes()[0..8]
                            );
                            crate::log_node(node_id, log::Level::Warn, &msg);
                        },
                        Err(e) => {
                            let msg = format!(
                                "💎 Node {} committed block - hash: {:?} (read error: {:?})",
                                node_id, &block_hash.bytes()[0..8], e
                            );
                            crate::log_node(node_id, log::Level::Error, &msg);
                        }
                    }
                }
            })
            .on_update_highest_pc({
                move |event| {
                    let msg = format!(
                        "📈 Node {} updated highest PC, View: {}, phase: {:?}",
                        node_id,
                        event.highest_pc.view,
                        event.highest_pc.phase
                    );
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_view_timeout({
                move |event| {
                    let msg = format!(
                        "⏱️ Node {} View {} timeout!",
                        node_id,
                        event.view
                    );
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_timeout_vote({
                move |event| {
                    let msg = format!(
                        "⏰ Node {} sent timeout vote, View: {}",
                        node_id,
                        event.timeout_vote.view
                    );
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_receive_timeout_vote({
                move |event| {
                    let msg = format!(
                        "📩 Node {} received timeout vote, from: {:?}, View: {}",
                        node_id,
                        event.origin.to_bytes()[0..4].to_vec(),
                        event.timeout_vote.view
                    );
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_collect_tc({
                move |event| {
                    let msg = format!(
                        "🔄 Node {} collected TC, View: {}",
                        node_id,
                        event.timeout_certificate.view
                    );
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_advance_view({
                move |event| {
                    let msg = format!(
                        "⏭️ Node {} advanced view to: {}",
                        node_id,
                        event.advance_view.progress_certificate.view()
                    );
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_new_view({
                move |event| {
                    let msg = format!(
                        "🆕 Node {} sent new view message, View: {}",
                        node_id,
                        event.new_view.view
                    );
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_receive_new_view({
                move |event| {
                    let msg = format!(
                        "📬 Node {} received new view message, from: {:?}, View: {}",
                        node_id,
                        event.origin.to_bytes()[0..4].to_vec(),
                        event.new_view.view
                    );
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_insert_block({
                move |event| {
                    let msg = format!(
                        "🔗 Node {} inserted block, height: {}, hash: {:?}",
                        node_id,
                        event.block.height.int(),
                        event.block.hash,
                    );
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .build()
            .start();
        
        info!("✅ Lock-free Node {} started", node_id);
        
        Self {
            verifying_key,
            replica,
            node_id,
            tx_queue,
            lockfree_stats,
        }
    }

    pub fn verifying_key(&self) -> VerifyingKey {
        self.verifying_key
    }

    pub fn committed_validator_set(&self) -> ValidatorSet {
        self.replica
            .block_tree_camera()
            .snapshot()
            .committed_validator_set()
            .expect("Should be able to get committed validator set from block tree")
    }

    pub fn highest_view_entered(&self) -> ViewNumber {
        self.replica
            .block_tree_camera()
            .snapshot()
            .highest_view_entered()
            .expect("Should be able to get highest view entered from block tree")
    }

    /// Submit transactions using lock-free queue
    pub fn submit_transactions(&self, transactions: Vec<String>) {
        for tx in transactions {
            self.tx_queue.push(tx.clone());
            self.lockfree_stats.record_submitted();
            info!("📝 Submitted transaction to lock-free queue: {}", tx);
        }
    }
    
    /// Get lock-free transaction queue for external access
    pub fn get_transaction_queue(&self) -> Arc<LockFreeTransactionQueue> {
        Arc::clone(&self.tx_queue)
    }
    
    /// Get lock-free statistics
    pub fn get_lockfree_stats(&self) -> Arc<LockFreePerformanceStats> {
        Arc::clone(&self.lockfree_stats)
    }
}