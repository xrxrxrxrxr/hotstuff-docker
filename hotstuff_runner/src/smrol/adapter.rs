use crossbeam::queue::SegQueue;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Adapter that forwards finalized SMROL transactions into the HotStuff input
/// queue. This mirrors Pompe's `output_to_hotstuff` behaviour so the HotStuff
/// layer can consume either source transparently.
#[derive(Debug)]
pub struct SmrolHotStuffAdapter {
    hotstuff_queue: Option<Arc<SegQueue<String>>>,
}

impl SmrolHotStuffAdapter {
    pub fn new() -> Self {
        Self {
            hotstuff_queue: None,
        }
    }

    pub fn connect_to_queue(&mut self, queue: Arc<SegQueue<String>>) {
        self.hotstuff_queue = Some(queue);
        debug!("🔗 [SMROL Adapter] Connected to HotStuff queue");
    }

    /// Push finalized transactions to HotStuff. Each item keeps a small SMROL
    /// prefix so downstream observers can distinguish the origin if needed.
    pub fn output_to_hotstuff(&self, transactions: Vec<String>, epoch: u64) {
        if let Some(ref queue) = self.hotstuff_queue {
            let count = transactions.len();
            for tx in transactions {
                // adapter从smrol获取交易push到hotstuff队列
                // queue.push(format!("smrol:{}:{}", epoch, tx)); // 不需要重复添加前缀
                queue.push(tx);
            }
            warn!(
                "📤 [SMROL→HotStuff] delivered {} transactions for epoch {}",
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
