// crate/src/node.rs
use crate::{
    app::TestApp, event::SystemEvent, kv_store::MemoryKVStore, stats::PerformanceStats,
    tx_utils::parse_transaction_id,
};
use dashmap::DashSet;
use ed25519_dalek::SigningKey;
use hotstuff_rs::networking::network::Network;
use hotstuff_rs::{
    block_tree::{pluggables::KVGet, variables::HIGHEST_COMMITTED_BLOCK},
    events::*,
    replica::{self, Configuration, Replica, ReplicaSpec},
    types::{
        block::Block,
        crypto_primitives::VerifyingKey,
        data_types::{BufferSize, ChainID, Data, EpochLength, ViewNumber},
        update_sets::{AppStateUpdates, ValidatorSetUpdates},
        validator_set::{ValidatorSet, ValidatorSetState},
    },
};
use socket2::MsgHdr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
// use log::info;
use crossbeam::queue::SegQueue;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{error, info, warn};

pub struct Node {
    verifying_key: VerifyingKey,
    replica: Replica<MemoryKVStore>,
    node_id: usize,
    // Keep an application handle for transaction submission support
    // app_handle: Arc<Mutex<TestApp>>,
    tx_queue: Arc<SegQueue<String>>,
    stats: Arc<PerformanceStats>,
    event_tx: broadcast::Sender<SystemEvent>, // Event broadcaster
}

impl Node {
    /// Create a node following the reference hotstuff_rs pattern
    pub fn new<N: Network + 'static>(
        node_id: usize, // Explicit node ID parameter
        keypair: SigningKey,
        network: N, // Generic network implementation (Tokio/TCP/mock)
        init_app_state_updates: AppStateUpdates,
        init_validator_set_updates: ValidatorSetUpdates,
        tx_queue: Arc<SegQueue<String>>, // External transaction queue
        stats: Arc<PerformanceStats>,    // Performance statistics handle
        event_tx: broadcast::Sender<SystemEvent>,
        confirmed_txs: Arc<DashSet<u64>>,
        in_flight_txs: Arc<DashSet<u64>>,
    ) -> Self {
        let verifying_key: VerifyingKey = keypair.verifying_key().into();

        info!(
            "Creating node, verifying key bytes: {:?}",
            verifying_key.to_bytes()[0..8].to_vec()
        );

        // 1. Build validator set from the provided updates
        let mut initial_validator_set = ValidatorSet::new();
        initial_validator_set.apply_updates(&init_validator_set_updates);

        info!(
            "Node validator set: {} validators, total power {}",
            initial_validator_set.len(),
            initial_validator_set.total_power().int()
        );

        // 2. Create validator-set state
        let validator_set_state = ValidatorSetState::new(
            initial_validator_set.clone(),
            initial_validator_set.clone(),
            None,
            true, // is_genesis
        );

        // 3. Construct KV store
        let kv_store = MemoryKVStore::new();

        // 4. Initialize replica storage
        Replica::initialize(
            kv_store.clone(),
            init_app_state_updates,
            validator_set_state,
        );

        // 5. Create the application and keep a handle
        let last_seen_height = Arc::new(AtomicU64::new(0));
        let app = TestApp::new(
            node_id,
            tx_queue.clone(),
            Arc::clone(&confirmed_txs),
            Arc::clone(&last_seen_height),
            Arc::clone(&in_flight_txs),
        );
        // let app_handle = Arc::new(Mutex::new(app.clone()));

        // 6. Build configuration mirroring the upstream defaults, allowing env overrides
        // Align HotStuff view timeout with Pompe stable period when HS_MAX_VIEW_TIME_MS is unset
        // let hs_view_env: Option<u64> = std::env::var("HS_MAX_VIEW_TIME_MS").ok().and_then(|s| s.parse().ok());
        // let pompe_stable_env: Option<u64> = std::env::var("POMPE_STABLE_PERIOD_MS").ok().and_then(|s| s.parse().ok());
        // let max_view_time_ms: u64 = match (hs_view_env, pompe_stable_env) {
        //     (Some(hs), _) => hs,
        //     (None, Some(stable)) => stable,
        //     (None, None) => 500,
        // };
        let max_view_time_ms: u64 = std::env::var("HS_MAX_VIEW_TIME_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(2000);
        warn!(
            "Node {} view timeout set to {} ms",
            node_id, max_view_time_ms
        );
        let progress_buf_cap: usize = std::env::var("HS_PROGRESS_BUF_CAP")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1024);
        let config = Configuration::builder()
            .me(keypair)
            .chain_id(ChainID::new(0))
            .block_sync_request_limit(10)
            .block_sync_server_advertise_time(Duration::new(2, 0)) // Upstream default: 10 seconds
            .block_sync_response_timeout(Duration::new(3, 0)) // Upstream default: 3 seconds
            // .block_sync_response_timeout(Duration::from_millis(500))
            .block_sync_blacklist_expiry_time(Duration::new(10, 0)) // Upstream default: 10 seconds
            .block_sync_trigger_min_view_difference(2) // Upstream default: 2
            .block_sync_trigger_timeout(Duration::new(60, 0)) // Upstream default: 60 seconds
            .progress_msg_buffer_capacity(BufferSize::new(progress_buf_cap.try_into().unwrap()))
            .epoch_length(EpochLength::new(50)) // Upstream default: 50
            // View timeout can be adjusted via HS_MAX_VIEW_TIME_MS
            .max_view_time(Duration::from_millis(max_view_time_ms))
            .log_events(false) // Upstream default: false
            .build();

        let event_tx_for_commit = event_tx.clone(); // Clone event broadcaster
        let kv_clone_commit = kv_store.clone();
        let kv_clone_receive = kv_store.clone();
        let stats_for_commit = stats.clone();
        let last_seen_height_for_commit = Arc::clone(&last_seen_height);

        // 7. Start the replica with detailed event handlers (mirroring upstream)
        let replica = ReplicaSpec::builder()
            .app(app)
            .network(network)
            .kv_store(kv_store)
            .configuration(config)
            // === Core events of interest ===
            .on_start_view({
                let event_tx_start_view = event_tx.clone();
                let last_seen = Arc::clone(&last_seen_height);
                move |event| {
                    let msg = format!("Node {} starting view {}", node_id, event.view);
                    crate::log_node(node_id, log::Level::Info, &msg);
                    let prev_view = event.view.int().saturating_sub(1);
                    last_seen.fetch_max(prev_view, Ordering::Relaxed);
                    let _ = event_tx_start_view.send(crate::event::SystemEvent::StartView { view: event.view.int() });
                }
            })
            .on_propose({
                move |event| {
                    // let msg = format!(
                    //     "Node {} proposing block, view: {}, height: {:?}, hash: {:?}",
                    //     node_id,
                    //     event.proposal.view,
                    //     event.proposal.block.height,
                    //     event.proposal.block.hash
                    // );
                    // crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_receive_proposal({
                let in_flight = Arc::clone(&in_flight_txs);
                let last_seen = Arc::clone(&last_seen_height);
                move |event| {
                    for tx_id in extract_transaction_ids_from_block(&event.proposal.block) {
                        in_flight.insert(tx_id);
                    }
                    last_seen.fetch_max(event.proposal.block.height.int(), Ordering::Relaxed);
                }
            })
            .on_phase_vote({
                move |event| {
                    // let msg = format!(
                    //     "Node {} voting in phase {:?} for view {}",
                    //     node_id,
                    //     event.vote.view,
                    //     event.vote.phase
                    // );
                    // crate::log_node(node_id, log::Level::Debug, &msg);
                }
            })
            .on_receive_phase_vote({
                move |event| {
                    // let msg = format!(
                    //     "Node {} received vote for view {}, phase {:?}",
                    //     node_id,
                    //     event.phase_vote.view,
                    //     event.phase_vote.phase
                    // );
                    // crate::log_node(node_id, log::Level::Debug, &msg);
                }
            })
            .on_collect_pc({
                move |event| {
                    // let msg = format!(
                    //     "Node {} collected PC at view {}, signature count {}",
                    //     node_id,
                    //     event.phase_certificate.view,
                    //     event.phase_certificate.signatures.iter().filter(|sig| sig.is_some()).count()
                    // );
                    // crate::log_node(node_id, log::Level::Info, &msg);
                    // // EWNL: end of the view's critical path
                    // let ewnl_end = format!("[EWNL] END view={}", event.phase_certificate.view.int());
                    // warn!(target = "hotstuff_runner::ewnl", "{}", ewnl_end);
                }
            })
            // .on_commit_block({
            //     move |event|{
            //         let msg = format!(
            //             "💎 Node {} Commit block, Hash: {:?}",
            //             node_id, event.block
            //         );
            //         crate::log_node(node_id, log::Level::Info, &msg);
            //     }
            // })
            .on_commit_block({
                move |event| {
                    let block_hash = event.block;
                    let commit_time = event.timestamp;

                    match kv_clone_commit.block(&block_hash) {
                        Ok(Some(block)) => {
                            let height = block.height.int();

                            // Key fix: parse the transaction count correctly
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

                            // Update statistics and compute various TPS metrics
                            let (end_to_end_tps, pure_consensus_tps, submission_tps, total_confirmed_txs, total_confirmed_blocks, is_first_commit) = {
                                // let mut stats = stats_for_commit.lock().unwrap();

                                // Check if this is the first commit
                                let is_first = stats_for_commit.get_confirmed_blocks() == 0;

                                // Record the committed block
                                stats_for_commit.record_block_committed(tx_count.into());

                                (
                                    stats_for_commit.get_end_to_end_tps(),        // end-to-end TPS
                                    stats_for_commit.get_pure_consensus_tps(),    // pure consensus TPS
                                    stats_for_commit.get_submission_tps(),        // submission TPS
                                    stats_for_commit.get_confirmed_transactions(),
                                    stats_for_commit.get_confirmed_blocks(),
                                    is_first
                                )
                            };

                            // Critical path: send the HotStuff commit event to trigger client responses
                            // Extract transaction IDs for client notifications
                            // let extract_transaction_ids_from_block_start = Instant::now();
                            let tx_ids: Vec<u64> = extract_transaction_ids_from_block(&block);
                                // .into_iter()
                                // .filter(|tx_id| *tx_id % 10 == 0)// Optionally sample transaction IDs
                                // .collect();
                            // info!("extracting tx_ids took {} ms", extract_transaction_ids_from_block_start.elapsed().as_millis());
                            if !tx_ids.is_empty() {
                                if let Err(e) = event_tx_for_commit.send(SystemEvent::HotStuffCommitted {
                                    block_height: height,
                                    tx_ids: tx_ids.clone(),
                                }) {
                                    error!("Node {} failed to send HotStuff commit event: {}", node_id, e);
                                }
                            }
                            info!(
                                "[Event sent] node {} HotStuffCommitted: height={}, tx_ids.len={}, tx_ids={:?}",
                                node_id,
                                height,
                                tx_ids.len(),
                                tx_ids
                            );
                            last_seen_height_for_commit.fetch_max(height, Ordering::Relaxed);
                            // Critical path: HotStuff commit event sent

                            // Primary statistics log entry
                            let msg = format!(
                                "💎 Node {} Commit block - Height: {}, TxCount: {}, E2E_TPS: {:.2}, Pure_TPS: {:.2}, Submit_TPS: {:.2}, TotalTxs: {}, TotalBlocks: {}, tx_ids.len= {}",
                                node_id, height, tx_count, end_to_end_tps, pure_consensus_tps, submission_tps, total_confirmed_txs, total_confirmed_blocks, tx_ids.len()
                            );
                            crate::log_node(node_id, log::Level::Info, &msg);

                            // Emit detailed metrics every 10 blocks
                            if total_confirmed_blocks % 10 == 0 {
                                // let stats_guard = stats_for_commit.lock().unwrap();
                                let recent_tps = stats_for_commit.get_recent_consensus_tps(30.0);

                                info!("Node {} consensus report (block #{}):", node_id, total_confirmed_blocks);
                                info!("  Submit TPS: {:.2} (client -> queue)", submission_tps);
                                info!("  End-to-end TPS: {:.2} (queue -> commit)", end_to_end_tps);
                                info!("  Pure consensus TPS: {:.2} (consensus layer)", pure_consensus_tps);
                                info!("  Recent TPS (30s): {:.2}", recent_tps);
                                info!("  Confirmed transactions: {}", total_confirmed_txs);
                                info!("  Confirmed blocks: {}", total_confirmed_blocks);

                                // Performance advisories
                                if submission_tps > end_to_end_tps * 1.2 {
                                    warn!(
                                        "Detected transaction backlog: submit rate ({:.1}) > confirm rate ({:.1})",
                                        submission_tps,
                                        end_to_end_tps
                                    );
                                }

                                // if pure_consensus_tps > 0.0 {
                                //     let queue_overhead = (end_to_end_tps / pure_consensus_tps - 1.0) * 100.0;
                                //     if queue_overhead > 10.0 {
                                //         warn!("Queue overhead is high: {:.1}%", queue_overhead);
                                //     } else {
                                //         info!("Queue overhead: {:.1}%", queue_overhead);
                                //     }
                                // }

                                // drop(stats_guard);
                            }
                        },
                        Ok(None) => {
                            // let msg = format!(
                            //     "Node {} committed block - hash {:?} (details not found)",
                            //     node_id,
                            //     &block_hash.bytes()[0..8]
                            // );
                            // crate::log_node(node_id, log::Level::Warn, &msg);
                        },
                        Err(e) => {
                            let msg = format!(
                                "Node {} committed block - hash {:?} (read error: {:?})",
                                node_id,
                                &block_hash.bytes()[0..8],
                                e
                            );
                            crate::log_node(node_id, log::Level::Error, &msg);
                        }
                    }
                }
            })
            .on_update_highest_pc({
                move |event| {
                    // let msg = format!(
                    //     "Node {} updated highest PC, view: {}, phase: {:?}",
                    //     node_id,
                    //     event.highest_pc.view,
                    //     event.highest_pc.phase
                    // );
                    // crate::log_node(node_id, log::Level::Info, &msg);
                    // warn!("[on_update_highest_pc] Node {} updated highest PC: view = {}", node_id, event.highest_pc.view);
                }
            })
            // === Timeout and view-change events ===
            .on_view_timeout({
                let node_id_copy = node_id;
                move |event| {
                    warn!("Node {} view {} timed out; latency may accumulate", node_id_copy, event.view);
                    let msg = format!("Node {} view {} timed out!", node_id, event.view.int());
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_timeout_vote({
                move |event| {
                    // let msg = format!(
                    //     "Node {} sent timeout vote for view {}",
                    //     node_id,
                    //     event.timeout_vote.view
                    // );
                    // crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_receive_timeout_vote({
                move |event| {
                    // let msg = format!(
                    //     "Node {} received timeout vote from {:?}, view {}",
                    //     node_id,
                    //     event.origin.to_bytes()[0..4].to_vec(),
                    //     event.timeout_vote.view
                    // );
                    // crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_collect_tc({
                move |event| {
                    // let msg = format!(
                    //     "Node {} collected TC for view {}",
                    //     node_id,
                    //     event.timeout_certificate.view
                    // );
                    // crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_advance_view({
                move |event| {
                    // Note: the view here comes from the progress certificate (PC/TC) and differs from the local "entered" view.
                    let pc_view = event.advance_view.progress_certificate.view();
                    // let msg = format!(
                    //     "Node {} received AdvanceView: PC.view={}",
                    //     node_id,
                    //     pc_view
                    // );
                    // crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_new_view({
                move |event| {
                    // Semantic note: NewView denotes sending a NewView message for the current (old) view to the next leader.
                    // It does not mean "enter the new view"—see StartView for that transition.
                    let cur_view = event.new_view.view.int();
                    let next_view = cur_view + 1;
                    // let msg = format!(
                    //     "Node {} sent NewView: cur_view={}, expected next_view={}",
                    //     node_id,
                    //     cur_view,
                    //     next_view
                    // );
                    // crate::log_node(node_id, log::Level::Info, &msg);
                    // warn!("[on_new_view] Node {} broadcast NewView for prior view {} (about to enter {})", node_id, cur_view, next_view);
                }
            })
            .on_receive_new_view({
                move |event| {
                    // let msg = format!(
                    //     "Node {} received NewView message from {:?}, view {}",
                    //     node_id,
                    //     event.origin.to_bytes()[0..4].to_vec(),
                    //     event.new_view.view
                    // );
                    // crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_insert_block({
                move |event| {
                    // let msg = format!(
                    //     "Node {} inserted block, height: {}, hash: {:?}",
                    //     node_id,
                    //     event.block.height.int(),
                    //     event.block.hash,
                    // );
                    // crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .build()
            .start();

        info!("Node {} started", node_id);

        Self {
            verifying_key,
            replica,
            node_id,
            // app_handle,  // Application handle placeholder
            tx_queue, // Shared transaction queue reference
            stats,
            event_tx, // Event broadcaster reference
        }
    }

    /// Return the node's verifying key
    pub fn verifying_key(&self) -> VerifyingKey {
        self.verifying_key
    }

    /// Retrieve the committed validator set
    pub fn committed_validator_set(&self) -> ValidatorSet {
        self.replica
            .block_tree_camera()
            .snapshot()
            .committed_validator_set()
            .expect("Committed validator set should be available from the block tree")
    }

    /// Retrieve the highest entered view number
    pub fn highest_view_entered(&self) -> ViewNumber {
        self.replica
            .block_tree_camera()
            .snapshot()
            .highest_view_entered()
            .expect("Highest entered view should be available from the block tree")
    }

    // /// Submit a single transaction to the node
    // pub fn submit_transaction(&self, transaction: String) {
    //     let mut app = self.app_handle.lock().unwrap();
    //     app.add_transaction(transaction.clone());
    //     crate::log_node(self.node_id, log::Level::Info,
    //                               &format!("Received transaction: {}", transaction));
    // }

    /// Submit a batch of transactions
    pub fn submit_transactions(&self, transactions: Vec<String>) {
        // Push directly into the shared queue
        // let mut queue = self.tx_queue.lock().unwrap();
        for tx in transactions {
            self.tx_queue.push(tx.clone());
            info!("Queued transaction: {}", tx);
        }

        // let mut app = self.app_handle.lock().unwrap();
        // for tx in &transactions {
        //     app.add_transaction(tx.clone());
        //     info!("Queued transaction via add_tx: {}", tx);
        // }
        // crate::log_node(self.node_id, log::Level::Info,
        //                           &format!("Received {} transactions", transactions.len()));
    }
}

// Core helper: extract transaction IDs from a block
// Refined extraction logic with additional diagnostics
fn extract_transaction_ids_from_block(block: &Block) -> Vec<u64> {
    let mut tx_ids = Vec::new();

    // debug!("[debug] extracting transaction IDs, block data length: {}", block.data.vec().len());

    // Key improvement: iterate over every datum instead of only the first
    for (index, data_item) in block.data.vec().iter().enumerate() {
        let tx_data_bytes = data_item.bytes();
        // debug!("[debug] data item {} length: {}", index, tx_data_bytes.len());

        // Skip very short data items (e.g., 8-byte padding)
        if tx_data_bytes.len() <= 10 {
            // debug!("[debug] data item {} too short, skipping", index);
            continue;
        }

        if let Ok(tx_data_str) = std::str::from_utf8(tx_data_bytes) {
            let preview = &tx_data_str[0..std::cmp::min(100, tx_data_str.len())];
            // debug!("[debug] data item {} string preview: {}", index, preview);

            // Skip blank or invalid content
            if tx_data_str.trim().is_empty() {
                // debug!("[debug] data item {} empty, skipping", index);
                continue;
            }

            // Parse transactions contained in this datum
            let item_tx_ids = parse_transaction_data_item(tx_data_str, index);
            tx_ids.extend(item_tx_ids);
        } else {
            // debug!("[debug] data item {} is not valid UTF-8", index);
        }
    }

    // debug!("[debug] extracted {} transaction IDs: {:?}", tx_ids.len(),
    //   &tx_ids[0..std::cmp::min(5, tx_ids.len())]);
    tx_ids
}

// Parse transactions embedded within a single data item
fn parse_transaction_data_item(tx_data_str: &str, data_index: usize) -> Vec<u64> {
    let mut tx_ids = Vec::new();

    // Method 1: split by line to handle multiple transactions
    let lines: Vec<&str> = tx_data_str.lines().collect();
    if lines.len() > 1 {
        // debug!("[debug] data item {} contains {} lines", data_index, lines.len());

        for (line_idx, line) in lines.iter().enumerate() {
            if line.trim().is_empty() {
                continue;
            }

            if let Some(tx_id) = parse_transaction_id(line) {
                tx_ids.push(tx_id);
                // debug!("[debug] data item {} line {} parsed tx ID {} from {}",
                //   data_index, line_idx, tx_id, line);
            } else {
                // warn!("[debug] data item {} line {} could not be parsed: {}", data_index, line_idx, line);
            }
        }
    }
    // Method 2: treat entire datum as a single transaction string
    else if let Some(tx_id) = parse_transaction_id(tx_data_str) {
        tx_ids.push(tx_id);
        // debug!("[debug] data item {} parsed single tx ID {}", data_index, tx_id);
    }
    // Method 3: attempt JSON array parsing
    else if let Ok(transactions) = serde_json::from_str::<Vec<String>>(tx_data_str) {
        // debug!("[debug] data item {} parsed JSON array with {} entries", data_index, transactions.len());

        for tx_str in transactions {
            if let Some(tx_id) = parse_transaction_id(&tx_str) {
                tx_ids.push(tx_id);
            }
        }
    }
    // Method 4: fallback to comma-separated values
    else if tx_data_str.contains(',') {
        // debug!("[debug] data item {} trying comma split", data_index);

        for part in tx_data_str.split(',') {
            if let Some(tx_id) = parse_transaction_id(part.trim()) {
                tx_ids.push(tx_id);
            }
        }
    } else {
        // warn!("[debug] data item {} has unrecognized format", data_index);
    }

    // debug!("[debug] data item {} produced {} transaction IDs", data_index, tx_ids.len());
    tx_ids
}
