use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
// #[derive(Debug, Clone)]
pub struct PerformanceStats {
    // 使用原子操作替代 Mutex
    submitted_count: AtomicU64,
    confirmed_transactions: AtomicU64,
    confirmed_blocks: AtomicU64,

    // 时间戳使用原子 u64 存储毫秒数
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

        // 记录第一次提交时间
        if count == 1 {
            self.first_submit_time_ms.store(now_ms, Ordering::Relaxed);
        }
    }

    /// 记录区块确认
    pub fn record_block_committed(&self, tx_count: u64) {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        self.confirmed_blocks.fetch_add(1, Ordering::Relaxed);
        let prev_txs = self
            .confirmed_transactions
            .fetch_add(tx_count, Ordering::Relaxed);

        // 忽略前期空块，直到真正确认到交易才开始计时
        if tx_count > 0 {
            if prev_txs == 0 {
                self.first_confirm_time_ms.store(now_ms, Ordering::Relaxed);
            }

            // 更新最后确认时间
            self.last_confirm_time_ms.store(now_ms, Ordering::Relaxed);
        }
    }
    // 共识TPS：基于确认时间段
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

    // 👈 端到端TPS：从真正有交易提交到确认的总体性能，主要看这个
    pub fn get_end_to_end_tps(&self) -> f64 {
        let confirmed = self.confirmed_transactions.load(Ordering::Relaxed);
        // 第一笔交易提交提交的时间
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

    // 基于提交交易数的TPS（原有逻辑）
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

    // 最近的TPS（基于最后一段时间）
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

        // 如果所有确认都在窗口外，返回0
        if last_confirm < window_start {
            return 0.0;
        }

        // 计算窗口内的有效时间范围
        let effective_start = window_start.max(first_confirm);
        let effective_end = last_confirm.min(now_ms);

        if effective_end <= effective_start {
            return 0.0;
        }

        // 估算窗口内的交易数（假设均匀分布）
        let total_duration = last_confirm.saturating_sub(first_confirm);
        if total_duration == 0 {
            return confirmed as f64 / (seconds); // 如果只有瞬时确认
        }

        let window_duration = effective_end - effective_start;
        let ratio = window_duration as f64 / total_duration as f64;
        let estimated_txs = (confirmed as f64) * ratio;

        // 返回TPS
        estimated_txs / (window_duration as f64 / 1000.0)
    }

    /// 获取最近TPS（简化版本）
    pub fn get_recent_tps(&self, seconds: f64) -> f64 {
        self.get_recent_consensus_tps(seconds)
    }

    pub fn calculate_pompe_tps(&self) -> f64 {
        if let Some(start) = self.pompe_start_time {
            let elapsed = start.elapsed().as_secs_f64();
            if elapsed > 0.0 {
                // 只统计以"pompe:"开头的交易
                self.pompe_confirmed_count as f64 / elapsed
            } else {
                0.0
            }
        } else {
            0.0
        }
    }

    // 🚨 还需要添加字段来跟踪Pompe交易
    pub fn record_pompe_confirmed(&mut self) {
        self.pompe_confirmed_count += 1;
    }

    pub fn get_pompe_confirmed_count(&self) -> u64 {
        self.pompe_confirmed_count
    }
}
