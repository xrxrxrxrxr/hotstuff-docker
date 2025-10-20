use crossbeam::queue::SegQueue;
use std::sync::{Arc, OnceLock};
use tracing::{debug, warn};

/// Adapter that forwards finalized SMROL transactions into the HotStuff input
/// queue. This mirrors Pompe's `output_to_hotstuff` behaviour so the HotStuff
/// layer can consume either source transparently.
#[derive(Debug)]
pub struct SmrolHotStuffAdapter {
    hotstuff_queue: OnceLock<Arc<SegQueue<String>>>,
}

impl SmrolHotStuffAdapter {
    pub fn new() -> Self {
        Self {
            hotstuff_queue: OnceLock::new(),
        }
    }

    pub fn connect_to_queue(&self, queue: Arc<SegQueue<String>>) {
        if self.hotstuff_queue.set(queue).is_err() {
            warn!("⚠️ [SMROL Adapter] HotStuff queue already connected");
        } else {
            debug!("🔗 [SMROL Adapter] Connected to HotStuff queue");
        }
    }

    /// Push finalized transactions to HotStuff. Each item keeps a small SMROL
    /// prefix so downstream observers can distinguish the origin if needed.
    pub fn output_to_hotstuff(&self, transactions: Vec<String>, epoch: u64) {
        if let Some(queue) = self.hotstuff_queue.get() {
            let count = transactions.len();
            for tx in transactions {
                queue.push(tx);
            }
            debug!(
                "🎉 [SMROL→HotStuff] delivered {} transactions for epoch {}",
                count, epoch
            );
        } else {
            warn!(
                "⚠️ [SMROL Adapter] HotStuff queue not connected, dropping {} transactions",
                transactions.len()
            );
        }
    }
}
