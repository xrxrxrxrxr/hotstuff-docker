// hotstuff_runner/src/stats.rs - 无锁版本
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

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
        }
    }

    /// 记录交易提交
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
            
        let prev_blocks = self.confirmed_blocks.fetch_add(1, Ordering::Relaxed);
        let prev_txs = self.confirmed_transactions.fetch_add(tx_count, Ordering::Relaxed);
        
        // 记录第一次确认时间
        if prev_blocks == 0 {
            self.first_confirm_time_ms.store(now_ms, Ordering::Relaxed);
        }
        
        // 更新最后确认时间
        self.last_confirm_time_ms.store(now_ms, Ordering::Relaxed);
    }

    /// 获取提交TPS（客户端到队列）
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

    /// 获取端到端TPS（队列到确认）
    pub fn get_end_to_end_tps(&self) -> f64 {
        let confirmed = self.confirmed_transactions.load(Ordering::Relaxed);
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

    /// 获取纯共识TPS（第一次确认到最后确认）
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

    /// 获取最近时间段的TPS
    // pub fn get_recent_consensus_tps(&self, seconds: f64) -> f64 {
    //     let confirmed = self.confirmed_transactions.load(Ordering::Relaxed);
    //     let last_confirm = self.last_confirm_time_ms.load(Ordering::Relaxed);
        
    //     if confirmed == 0 || last_confirm == 0 {
    //         return 0.0;
    //     }
        
    //     let now_ms = SystemTime::now()
    //         .duration_since(UNIX_EPOCH)
    //         .unwrap()
    //         .as_millis() as u64;
            
    //     let window_ms = (seconds * 1000.0) as u64;
    //     let recent_period_start = now_ms.saturating_sub(window_ms);
        
    //     // 简化实现：假设交易在最近时间段内均匀分布
    //     if last_confirm > recent_period_start {
    //         let actual_window = now_ms.saturating_sub(last_confirm.min(recent_period_start));
    //         if actual_window > 0 {
    //             // 估算最近时间段的交易数量
    //             let recent_ratio = (actual_window as f64) / (window_ms as f64);
    //             let estimated_recent_txs = (confirmed as f64) * recent_ratio.min(1.0);
    //             return estimated_recent_txs / (actual_window as f64 / 1000.0);
    //         }
    //     }
        
    //     0.0
    // }

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

    // Getter方法
    pub fn get_submitted_count(&self) -> u64 {
        self.submitted_count.load(Ordering::Relaxed)
    }

    pub fn get_confirmed_transactions(&self) -> u64 {
        self.confirmed_transactions.load(Ordering::Relaxed)
    }

    pub fn get_confirmed_blocks(&self) -> u64 {
        self.confirmed_blocks.load(Ordering::Relaxed)
    }
}