// #[derive(Debug, Clone)]
// struct PerformanceStats {
//     submitted_count: u64,
//     confirmed_count: u64,
//     start_time: Option<std::time::Instant>,
// }

// impl PerformanceStats {
//     pub fn new() -> Self {
//         Self {
//             submitted_count: 0,
//             confirmed_count: 0,
//             start_time: None,
//         }
//     }

//     pub fn record_submitted(&mut self) {
//         if self.start_time.is_none() {
//             self.start_time = Some(std::time::Instant::now());
//         }
//         self.submitted_count += 1;
//     }

//     pub fn record_confirmed(&mut self, count: u64) {
//         self.confirmed_count += count;
//     }

//     pub fn get_tps(&self) -> f64 {
//         if let Some(start) = self.start_time {
//             let elapsed = start.elapsed().as_secs_f64();
//             if elapsed > 0.0 {
//                 return self.confirmed_count as f64 / elapsed;
//             }
//         }
//         0.0
//     }
// }

#[derive(Debug, Clone)]
pub struct PerformanceStats {
    submitted_count: u64,
    confirmed_count: u64,
    confirmed_transactions: u64,  // 新增：实际确认的交易数
    start_time: Option<std::time::Instant>,
    first_commit_time: Option<std::time::Instant>, 
    last_commit_time: Option<std::time::Instant>,  // 新增：最后一次提交时间
}

impl PerformanceStats {
    pub fn new() -> Self {
        Self {
            submitted_count: 0,
            confirmed_count: 0,
            confirmed_transactions: 0,
            start_time: None,
            first_commit_time: None,
            last_commit_time: None,
        }
    }

    pub fn record_submitted(&mut self) {
        if self.start_time.is_none() {
            self.start_time = Some(std::time::Instant::now());
        }
        self.submitted_count += 1;
    }

    // 记录区块确认（包含交易数量）
    pub fn record_block_committed(&mut self, tx_count: u64) {
        // 👈 记录第一个区块确认时间
        if self.first_commit_time.is_none() {
            self.first_commit_time = Some(std::time::Instant::now());
        }
        self.confirmed_count += 1;  // 确认的区块数
        self.confirmed_transactions += tx_count;  // 确认的交易数
        self.last_commit_time = Some(std::time::Instant::now());
    }
    // 👈 纯粹的共识TPS：基于确认时间段
    pub fn get_pure_consensus_tps(&self) -> f64 {
        if let (Some(first_commit), Some(last_commit)) = (self.first_commit_time, self.last_commit_time) {
            let consensus_duration = last_commit.duration_since(first_commit).as_secs_f64();
            if consensus_duration > 0.0 {
                return self.confirmed_transactions as f64 / consensus_duration;
            }
        }
        0.0
    }
    
    pub fn get_submitted_count(&self) -> u64 {
        self.submitted_count
    }

    pub fn get_confirmed_transactions(&self) -> u64 {
        self.confirmed_transactions
    }

    pub fn get_confirmed_blocks(&self) -> u64 {
        self.confirmed_count
    }

    // 👈 端到端TPS：从提交到确认的总体性能
    pub fn get_end_to_end_tps(&self) -> f64 {
        if let Some(start) = self.start_time {
            let elapsed = start.elapsed().as_secs_f64();
            if elapsed > 0.0 {
                return self.confirmed_transactions as f64 / elapsed;
            }
        }
        0.0
    }

    // 基于提交交易数的TPS（原有逻辑）
    pub fn get_submission_tps(&self) -> f64 {
        if let Some(start) = self.start_time {
            let elapsed = start.elapsed().as_secs_f64();
            if elapsed > 0.0 {
                return self.submitted_count as f64 / elapsed;
            }
        }
        0.0
    }

    // 最近的TPS（基于最后一段时间）
    pub fn get_recent_tps(&self, window_seconds: f64) -> f64 {
        if let (Some(start), Some(last_commit)) = (self.start_time, self.last_commit_time) {
            let total_elapsed = start.elapsed().as_secs_f64();
            if total_elapsed >= window_seconds {
                // 计算最近window_seconds内的TPS
                let recent_start = total_elapsed - window_seconds;
                // 这里简化处理，实际可能需要更复杂的滑动窗口
                return self.confirmed_transactions as f64 / total_elapsed;
            }
        }
        {
            let this = &self;
            if let Some(start) = this.start_time {
                let elapsed = start.elapsed().as_secs_f64();
                if elapsed > 0.0 {
                    return this.confirmed_transactions as f64 / elapsed;
                }
            }
            0.0
        }
    }

    // 👈 最近时间窗口的TPS
    pub fn get_recent_consensus_tps(&self, window_seconds: f64) -> f64 {
        if let Some(last_commit) = self.last_commit_time {
            // 这需要更复杂的实现来跟踪时间窗口内的确认数
            // 简化版本：假设最近都在确认
            if let Some(first_commit) = self.first_commit_time {
                let total_duration = last_commit.duration_since(first_commit).as_secs_f64();
                if total_duration >= window_seconds {
                    // 估算最近窗口的确认数（需要更精确的实现）
                    let recent_rate = self.confirmed_transactions as f64 / total_duration;
                    return recent_rate;
                }
            }
        }
        self.get_pure_consensus_tps()
    }
}