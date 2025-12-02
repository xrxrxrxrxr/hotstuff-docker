use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
// #[derive(Debug, Clone)]
pub struct PerformanceStats {
    // Use atomics instead of a Mutex
    submitted_count: AtomicU64,
    confirmed_transactions: AtomicU64,
    confirmed_blocks: AtomicU64,

    // Store timestamps as atomic u64 values measured in milliseconds
    start_time_ms: AtomicU64,
    first_submit_time_ms: AtomicU64,
    first_confirm_time_ms: AtomicU64,
    last_confirm_time_ms: AtomicU64,
    pompe_confirmed_count: u64,
    pompe_start_time: Option<std::time::Instant>,
}

impl PerformanceStats {
    pub fn new() -> Self {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        Self {
            submitted_count: AtomicU64::new(0),
            confirmed_transactions: AtomicU64::new(0),
            confirmed_blocks: AtomicU64::new(0),
            start_time_ms: AtomicU64::new(now_ms),
            first_submit_time_ms: AtomicU64::new(0),
            first_confirm_time_ms: AtomicU64::new(0),
            last_confirm_time_ms: AtomicU64::new(0),
            pompe_confirmed_count: 0,
            pompe_start_time: None,
        }
    }

    pub fn record_submitted(&self) {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let count = self.submitted_count.fetch_add(1, Ordering::Relaxed) + 1;

        // Record the first submission time
        if count == 1 {
            self.first_submit_time_ms.store(now_ms, Ordering::Relaxed);
        }
    }

    /// Record a committed block
    pub fn record_block_committed(&self, tx_count: u64) {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        self.confirmed_blocks.fetch_add(1, Ordering::Relaxed);
        let prev_txs = self
            .confirmed_transactions
            .fetch_add(tx_count, Ordering::Relaxed);

        // Ignore early empty blocks; begin timing once transactions are confirmed
        if tx_count > 0 {
            if prev_txs == 0 {
                self.first_confirm_time_ms.store(now_ms, Ordering::Relaxed);
            }

            // Update the most recent confirmation time
            self.last_confirm_time_ms.store(now_ms, Ordering::Relaxed);
        }
    }
    // Consensus TPS: derived from the confirmation interval
    pub fn get_pure_consensus_tps(&self) -> f64 {
        let confirmed = self.confirmed_transactions.load(Ordering::Relaxed);
        let first_confirm = self.first_confirm_time_ms.load(Ordering::Relaxed);
        let last_confirm = self.last_confirm_time_ms.load(Ordering::Relaxed);

        if confirmed <= 1 || first_confirm == 0 || last_confirm == 0 {
            return 0.0;
        }

        let elapsed_ms = last_confirm.saturating_sub(first_confirm);
        if elapsed_ms == 0 {
            return 0.0;
        }

        ((confirmed - 1) as f64) / (elapsed_ms as f64 / 1000.0)
    }

    pub fn get_submitted_count(&self) -> u64 {
        self.submitted_count.load(Ordering::Relaxed)
    }

    pub fn get_confirmed_transactions(&self) -> u64 {
        self.confirmed_transactions.load(Ordering::Relaxed)
    }

    pub fn get_confirmed_blocks(&self) -> u64 {
        self.confirmed_blocks.load(Ordering::Relaxed)
    }

    // End-to-end TPS: overall throughput from first submission to confirmation
    pub fn get_end_to_end_tps(&self) -> f64 {
        let confirmed = self.confirmed_transactions.load(Ordering::Relaxed);
        // Timestamp of the first submitted transaction
        let first_submit = self.first_submit_time_ms.load(Ordering::Relaxed);

        if confirmed == 0 || first_submit == 0 {
            return 0.0;
        }

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let elapsed_ms = now_ms.saturating_sub(first_submit);
        if elapsed_ms == 0 {
            return 0.0;
        }

        (confirmed as f64) / (elapsed_ms as f64 / 1000.0)
    }

    // Submission-based TPS (previous logic)
    pub fn get_submission_tps(&self) -> f64 {
        let submitted = self.submitted_count.load(Ordering::Relaxed);
        let first_submit = self.first_submit_time_ms.load(Ordering::Relaxed);

        if submitted == 0 || first_submit == 0 {
            return 0.0;
        }

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let elapsed_ms = now_ms.saturating_sub(first_submit);
        if elapsed_ms == 0 {
            return 0.0;
        }

        (submitted as f64) / (elapsed_ms as f64 / 1000.0)
    }

    // Recent TPS (based on the latest window)
    pub fn get_recent_consensus_tps(&self, seconds: f64) -> f64 {
        let confirmed = self.confirmed_transactions.load(Ordering::Relaxed);
        let first_confirm = self.first_confirm_time_ms.load(Ordering::Relaxed);
        let last_confirm = self.last_confirm_time_ms.load(Ordering::Relaxed);

        if confirmed == 0 || first_confirm == 0 || last_confirm == 0 {
            return 0.0;
        }

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let window_ms = (seconds * 1000.0) as u64;
        let window_start = now_ms.saturating_sub(window_ms);

        // Return 0 if every confirmation is outside the window
        if last_confirm < window_start {
            return 0.0;
        }

        // Determine the effective time range within the window
        let effective_start = window_start.max(first_confirm);
        let effective_end = last_confirm.min(now_ms);

        if effective_end <= effective_start {
            return 0.0;
        }

        // Estimate how many transactions fall within the window (assumes uniform spacing)
        let total_duration = last_confirm.saturating_sub(first_confirm);
        if total_duration == 0 {
            return confirmed as f64 / seconds; // Handles instantaneous confirmations
        }

        let window_duration = effective_end - effective_start;
        let ratio = window_duration as f64 / total_duration as f64;
        let estimated_txs = (confirmed as f64) * ratio;

        // Return the TPS value
        estimated_txs / (window_duration as f64 / 1000.0)
    }

    /// Get recent TPS (simplified version)
    pub fn get_recent_tps(&self, seconds: f64) -> f64 {
        self.get_recent_consensus_tps(seconds)
    }

    pub fn calculate_pompe_tps(&self) -> f64 {
        if let Some(start) = self.pompe_start_time {
            let elapsed = start.elapsed().as_secs_f64();
            if elapsed > 0.0 {
                // Count only transactions whose identifier starts with "pompe:"
                self.pompe_confirmed_count as f64 / elapsed
            } else {
                0.0
            }
        } else {
            0.0
        }
    }

    // TODO: add more fields for detailed Pompe transaction tracking
    pub fn record_pompe_confirmed(&mut self) {
        self.pompe_confirmed_count += 1;
    }

    pub fn get_pompe_confirmed_count(&self) -> u64 {
        self.pompe_confirmed_count
    }
}
