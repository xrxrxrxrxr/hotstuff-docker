use crate::event::SystemEvent;
use crate::smrol::{
    adapter::SmrolHotStuffAdapter,
    consensus::{Consensus, TransactionEntry},
    crypto::derive_threshold_keys,
    finalization::OutputFinalization,
    message::{SmrolMessage, SmrolTransaction},
    network::SmrolTcpNetwork,
    pnfifo::PnfifoBc,
    sequencing::TransactionSequencing,
};
use ed25519_dalek::{SigningKey, VerifyingKey};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex};
use tracing::{debug, error, info};

#[derive(Debug, Clone)]
pub struct SmrolConfig {
    pub enable: bool,
    pub f: usize,
    pub capital_k: usize,
    pub epoch_timeout_ms: u64,
    pub pnfifo_threshold: usize,
}

impl Default for SmrolConfig {
    fn default() -> Self {
        Self {
            enable: true,
            f: 1,
            // k: 3, // 🔥🔥 文中 k=O(n)
            capital_k: 1, // 🔥🔥 修改点
            epoch_timeout_ms: 100,
            pnfifo_threshold: 3,
        }
    }
}

pub struct SmrolManager {
    pub node_id: usize,
    pub config: SmrolConfig,
    pub n: usize,

    pub network: Arc<SmrolTcpNetwork>,
    pub consensus: Arc<Mutex<Consensus>>,
    pub sequencing: Arc<Mutex<TransactionSequencing>>,
    pub finalization: Arc<Mutex<OutputFinalization>>,
    hotstuff_adapter: Mutex<Option<Arc<SmrolHotStuffAdapter>>>,
    pub event_tx: broadcast::Sender<SystemEvent>,

    pub signing_key: SigningKey,
    pub verifying_keys: HashMap<usize, VerifyingKey>,
}

impl SmrolManager {
    pub async fn new(
        node_id: usize,
        config: SmrolConfig,
        signing_key: SigningKey,
        verifying_keys: HashMap<usize, VerifyingKey>,
        peer_addrs: HashMap<usize, SocketAddr>,
        event_tx: broadcast::Sender<SystemEvent>,
    ) -> Result<Self, String> {
        let n = verifying_keys.len();

        let (threshold_share, threshold_public) =
            derive_threshold_keys(node_id, config.f, &verifying_keys)
                .map_err(|e| format!("derive threshold keys failed: {}", e))?;

        let pnfifo = Arc::new(
            PnfifoBc::new(
                node_id,
                n,
                signing_key.clone(),
                verifying_keys.clone(),
                peer_addrs,
            )
            .await?,
        );

        // start PNFIFO network listener
        pnfifo.start().await?;
        info!("✅ [SMROL] PNFIFO自动启动完成");

        let network = pnfifo.network();

        let finalization = Arc::new(Mutex::new(OutputFinalization::new(
            node_id,
            config.f,
            Arc::clone(&network),
            signing_key.clone(),
        )));

        let sequencing = TransactionSequencing::new(
            node_id,
            n,
            config.f,
            Arc::clone(&network),
            Arc::clone(&pnfifo),
            threshold_share,
            threshold_public.clone(),
            Arc::clone(&finalization),
        );

        let consensus = Consensus::new(
            node_id,
            n,
            config.f,
            Arc::clone(&network),
            signing_key.clone(),
            verifying_keys.clone(),
            Arc::clone(&finalization),
            event_tx.clone(),
        );

        Ok(Self {
            node_id,
            config,
            n,
            network,
            consensus: Arc::new(Mutex::new(consensus)),
            sequencing: Arc::new(Mutex::new(sequencing)),
            finalization,
            hotstuff_adapter: Mutex::new(None),
            event_tx,
            signing_key,
            verifying_keys,
        })
    }

    pub async fn set_hotstuff_adapter(&self, adapter: Arc<SmrolHotStuffAdapter>) {
        {
            let mut consensus = self.consensus.lock().await;
            consensus.set_hotstuff_adapter(adapter.clone());
        }
        let mut slot = self.hotstuff_adapter.lock().await;
        *slot = Some(adapter);
        info!("🔗 [SMROL] Connected to HotStuff adapter");
    }

    // public interface to process incoming SMROL transactions
    pub async fn process_smrol_transaction(
        &self,
        transaction: SmrolTransaction,
    ) -> Result<(), String> {
        info!(
            "📥 [SMROL] Processing transaction: {}:{}->{}:{}",
            transaction.id, transaction.from, transaction.to, transaction.amount
        );

        let tx_id = transaction.id;
        let sequencing_handle = Arc::clone(&self.sequencing);
        tokio::spawn(async move {
            let mut sequencing = sequencing_handle.lock().await;
            match sequencing.smrol_broadcast(transaction).await {
                Ok(_) => debug!("✅ [SMROL] Broadcast completed. Transaction tx_id={} dispatched to sequencing layer", tx_id),
                Err(e) => error!("[SMROL] Sequencing broadcast failed: {}", e),
            }
        });
        Ok(())
    }

    pub async fn start_message_loop(self: Arc<Self>) -> Result<(), String> {
        info!("🔄 [SMROL] Message loop starting for node {}", self.node_id);

        let sequencing_rx = self.network.get_sequencing_receiver();
        let sequencing_handle = Arc::clone(&self.sequencing);
        let manager_for_seq = Arc::clone(&self);
        tokio::spawn(async move {
            let mut rx = sequencing_rx.lock().await;
            while let Some((sender_id, message)) = rx.recv().await {
                let result = TransactionSequencing::handle_smrol_message(
                    Arc::clone(&sequencing_handle),
                    sender_id,
                    message,
                )
                .await;

                // result is Result<Option<TransactionEntry>, String> ready for consensus if Some
                match result {
                    Ok(Some(entry)) => {
                        // line 38: add_sequenced_transaction(entry) to Mi and finalization
                        info!("[Manager] Sequencing output ready add_sequenced_transaction: vc_tx={} s_tx={}", hex::encode(&entry.vc_tx[..std::cmp::min(8, entry.vc_tx.len())]), entry.s_tx);
                        if let Err(e) = manager_for_seq.add_sequenced_transaction(entry).await {
                            error!("[SMROL] 共识输入登记失败: {}", e);
                        }
                    }
                    Ok(None) => {}
                    Err(e) => error!("处理Sequencing消息失败: {}", e),
                }
            }
            debug!(
                "ℹ️ [SMROL] Sequencing loop exited for node {}",
                manager_for_seq.node_id
            );
        });

        let consensus_rx = self.network.get_consensus_receiver();
        let consensus_handle = Arc::clone(&self.consensus);
        let manager_for_consensus = Arc::clone(&self);
        info!("🔄 [Manager] Node {} start_message_loop spawning handle_consensus_message", self.node_id);
        tokio::spawn(async move {
            info!("🔄 [Manager] handle_consensus_message started for node {}", manager_for_consensus.node_id);
            let mut rx = consensus_rx.lock().await;
            while let Some((sender_id, message)) = rx.recv().await {
                let mut consensus = consensus_handle.lock().await;
                if let Err(e) = consensus.handle_consensus_message(sender_id, message).await {
                    error!("处理Consensus消息失败: {}", e);
                }
            }
            debug!(
                "ℹ️ [SMROL] Consensus loop exited for node {}",
                manager_for_consensus.node_id
            );
        });

        info!("✅ [SMROL] 消息处理循环已启动");
        Ok(())
    }

    pub async fn invoke_consensus_and_finalize(&self, epoch: u64) -> Result<(), String> {
        info!("🏛️ [SMROL] Starting consensus for epoch {}", epoch);

        {
            let mut consensus = self.consensus.lock().await;
            consensus.run_consensus(epoch).await?;
        }
        Ok(())
    }

    pub async fn should_invoke_consensus(&self, epoch: u64) -> bool {
        let consensus = self.consensus.lock().await;
        consensus.get_mi_size(epoch) >= self.config.capital_k // line 39
    }

    // line 38: take over sequenced transaction and add to Mi and finalization
    pub async fn add_sequenced_transaction(&self, entry: TransactionEntry) -> Result<(), String> {
        let entry_meta = (entry.vc_tx.len(), entry.s_tx);
        let entry_for_finalization = entry.clone();

        let (epoch, pending) = {
            let mut consensus = self.consensus.lock().await;
            let epoch = consensus.get_current_epoch();
            consensus.add_to_mi(epoch, entry);
            let pending = consensus.get_mi_size(epoch);
            (epoch, pending)
        };

        {
            let mut finalization = self.finalization.lock().await;
            finalization.add_to_mi(epoch, entry_for_finalization);
        }

        info!(
            "🧮 [Manager] 登记Sequencing输出, add to finalization M_i: epoch={} vc_bytes={} s_tx={} pending={} K={}",
            epoch, entry_meta.0, entry_meta.1, pending, self.config.capital_k
        );

        if self.should_invoke_consensus(epoch).await {
            self.invoke_consensus_and_finalize(epoch).await?;
        }

        Ok(())
    }

    pub async fn debug_status(&self) {
        let consensus = self.consensus.lock().await;
        info!("📊 [SMROL Status] Node {}", self.node_id);
        info!("  - Current epoch: {}", consensus.get_current_epoch());
        drop(consensus);
        let adapter_connected = self.hotstuff_adapter.lock().await.is_some();
        info!("  - HotStuff adapter connected: {}", adapter_connected);
        info!(
            "  - Network strong refs: {}",
            Arc::strong_count(&self.network)
        );
    }
}

pub fn load_smrol_config() -> SmrolConfig {
    use std::env;

    SmrolConfig {
        enable: env::var("SMROL_ENABLE")
            .unwrap_or_else(|_| "true".to_string())
            .parse()
            .unwrap_or(true),
        f: env::var("SMROL_F")
            .unwrap_or_else(|_| "1".to_string())
            .parse()
            .unwrap_or(1),
        capital_k: env::var("SMROL_K")
            .unwrap_or_else(|_| "1".to_string())
            .parse()
            .unwrap_or(1),
        epoch_timeout_ms: env::var("SMROL_EPOCH_TIMEOUT_MS")
            .unwrap_or_else(|_| "100".to_string())
            .parse()
            .unwrap_or(100),
        pnfifo_threshold: env::var("SMROL_PNFIFO_THRESHOLD")
            .unwrap_or_else(|_| "3".to_string())
            .parse()
            .unwrap_or(3),
    }
}
