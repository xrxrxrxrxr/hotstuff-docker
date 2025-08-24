// 详细的性能指标监控实现
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use std::collections::VecDeque;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct DetailedLatencyTracker {
    // 延迟记录
    latency_samples: Mutex<VecDeque<Duration>>,
    max_samples: usize,
    
    // 计数器
    total_processed: AtomicUsize,
    total_processing_time_nanos: AtomicU64,
    
    // 时间戳
    start_time: Instant,
    last_reset_time: AtomicU64,
}

impl DetailedLatencyTracker {
    pub fn new(max_samples: usize) -> Self {
        Self {
            latency_samples: Mutex::new(VecDeque::with_capacity(max_samples)),
            max_samples,
            total_processed: AtomicUsize::new(0),
            total_processing_time_nanos: AtomicU64::new(0),
            start_time: Instant::now(),
            last_reset_time: AtomicU64::new(0),
        }
    }
    
    pub async fn record_processing(&self, processing_time: Duration) {
        // 更新计数器
        self.total_processed.fetch_add(1, Ordering::Relaxed);
        self.total_processing_time_nanos.fetch_add(
            processing_time.as_nanos() as u64, 
            Ordering::Relaxed
        );
        
        // 记录样本
        let mut samples = self.latency_samples.lock().await;
        if samples.len() >= self.max_samples {
            samples.pop_front();
        }
        samples.push_back(processing_time);
    }
    
    pub async fn get_statistics(&self) -> LatencyStatistics {
        let samples = self.latency_samples.lock().await;
        let total_processed = self.total_processed.load(Ordering::Relaxed);
        let total_time_nanos = self.total_processing_time_nanos.load(Ordering::Relaxed);
        
        let elapsed = self.start_time.elapsed();
        let tps = if elapsed.as_secs_f64() > 0.0 {
            total_processed as f64 / elapsed.as_secs_f64()
        } else {
            0.0
        };
        
        let average_latency = if total_processed > 0 {
            Duration::from_nanos(total_time_nanos / total_processed as u64)
        } else {
            Duration::ZERO
        };
        
        // 计算最近延迟统计
        let recent_statistics = if !samples.is_empty() {
            let mut sorted_samples: Vec<Duration> = samples.iter().cloned().collect();
            sorted_samples.sort();
            
            let len = sorted_samples.len();
            let recent_avg = sorted_samples.iter().sum::<Duration>() / len as u32;
            let recent_p50 = sorted_samples[len / 2];
            let recent_p95 = sorted_samples[(len as f64 * 0.95) as usize];
            let recent_p99 = sorted_samples[(len as f64 * 0.99) as usize];
            let recent_max = *sorted_samples.last().unwrap();
            let recent_min = *sorted_samples.first().unwrap();
            
            Some(RecentLatencyStats {
                avg: recent_avg,
                p50: recent_p50,
                p95: recent_p95,
                p99: recent_p99,
                max: recent_max,
                min: recent_min,
                sample_count: len,
            })
        } else {
            None
        };
        
        LatencyStatistics {
            tps,
            total_processed,
            average_latency,
            recent: recent_statistics,
            elapsed_time: elapsed,
        }
    }
    
    pub async fn reset_recent_samples(&self) {
        let mut samples = self.latency_samples.lock().await;
        samples.clear();
        self.last_reset_time.store(
            self.start_time.elapsed().as_secs(), 
            Ordering::Relaxed
        );
    }
}

#[derive(Debug, Clone)]
pub struct LatencyStatistics {
    pub tps: f64,
    pub total_processed: usize,
    pub average_latency: Duration,
    pub recent: Option<RecentLatencyStats>,
    pub elapsed_time: Duration,
}

#[derive(Debug, Clone)]
pub struct RecentLatencyStats {
    pub avg: Duration,
    pub p50: Duration,
    pub p95: Duration,
    pub p99: Duration,
    pub max: Duration,
    pub min: Duration,
    pub sample_count: usize,
}

// Pompe 队列专用性能监控
#[derive(Debug)]
pub struct PompeQueueMetrics {
    // 不同阶段的延迟跟踪
    pub batch_processing_tracker: DetailedLatencyTracker,
    pub ordering1_tracker: DetailedLatencyTracker,
    pub ordering2_tracker: DetailedLatencyTracker,
    pub commit_tracker: DetailedLatencyTracker,
    
    // 队列大小记录
    queue_size_samples: Mutex<VecDeque<(Instant, usize)>>,
    
    // 吞吐量统计
    pub transactions_submitted: AtomicUsize,
    pub transactions_completed: AtomicUsize,
    
    start_time: Instant,
}

impl PompeQueueMetrics {
    pub fn new() -> Self {
        Self {
            batch_processing_tracker: DetailedLatencyTracker::new(100),
            ordering1_tracker: DetailedLatencyTracker::new(100),
            ordering2_tracker: DetailedLatencyTracker::new(100),
            commit_tracker: DetailedLatencyTracker::new(100),
            queue_size_samples: Mutex::new(VecDeque::with_capacity(200)),
            transactions_submitted: AtomicUsize::new(0),
            transactions_completed: AtomicUsize::new(0),
            start_time: Instant::now(),
        }
    }
    
    pub async fn record_queue_size(&self, size: usize) {
        let mut samples = self.queue_size_samples.lock().await;
        let now = Instant::now();
        
        if samples.len() >= 200 {
            samples.pop_front();
        }
        samples.push_back((now, size));
    }
    
    pub fn record_transaction_submitted(&self, count: usize) {
        self.transactions_submitted.fetch_add(count, Ordering::Relaxed);
    }
    
    pub fn record_transaction_completed(&self, count: usize) {
        self.transactions_completed.fetch_add(count, Ordering::Relaxed);
    }
    
    pub async fn get_comprehensive_stats(&self) -> PompeQueueStats {
        let batch_stats = self.batch_processing_tracker.get_statistics().await;
        let ordering1_stats = self.ordering1_tracker.get_statistics().await;
        let ordering2_stats = self.ordering2_tracker.get_statistics().await;
        let commit_stats = self.commit_tracker.get_statistics().await;
        
        let submitted = self.transactions_submitted.load(Ordering::Relaxed);
        let completed = self.transactions_completed.load(Ordering::Relaxed);
        let elapsed = self.start_time.elapsed();
        
        let submission_tps = if elapsed.as_secs_f64() > 0.0 {
            submitted as f64 / elapsed.as_secs_f64()
        } else {
            0.0
        };
        
        let completion_tps = if elapsed.as_secs_f64() > 0.0 {
            completed as f64 / elapsed.as_secs_f64()
        } else {
            0.0
        };
        
        // 计算平均队列大小
        let samples = self.queue_size_samples.lock().await;
        let avg_queue_size = if !samples.is_empty() {
            samples.iter().map(|(_, size)| *size).sum::<usize>() as f64 / samples.len() as f64
        } else {
            0.0
        };
        
        PompeQueueStats {
            submission_tps,
            completion_tps,
            submitted_total: submitted,
            completed_total: completed,
            avg_queue_size,
            batch_processing: batch_stats,
            ordering1: ordering1_stats,
            ordering2: ordering2_stats,
            commit: commit_stats,
            elapsed_time: elapsed,
        }
    }
}

#[derive(Debug)]
pub struct PompeQueueStats {
    pub submission_tps: f64,
    pub completion_tps: f64,
    pub submitted_total: usize,
    pub completed_total: usize,
    pub avg_queue_size: f64,
    pub batch_processing: LatencyStatistics,
    pub ordering1: LatencyStatistics,
    pub ordering2: LatencyStatistics,
    pub commit: LatencyStatistics,
    pub elapsed_time: Duration,
}

// HotStuff 队列专用性能监控
#[derive(Debug)]
pub struct HotStuffQueueMetrics {
    pub block_production_tracker: DetailedLatencyTracker,
    pub block_validation_tracker: DetailedLatencyTracker,
    pub consensus_tracker: DetailedLatencyTracker,
    
    // 队列和区块统计
    queue_size_samples: Mutex<VecDeque<(Instant, usize)>>,
    pub blocks_produced: AtomicUsize,
    pub blocks_committed: AtomicUsize,
    pub transactions_in_blocks: AtomicUsize,
    
    start_time: Instant,
}

impl HotStuffQueueMetrics {
    pub fn new() -> Self {
        Self {
            block_production_tracker: DetailedLatencyTracker::new(50),
            block_validation_tracker: DetailedLatencyTracker::new(50),
            consensus_tracker: DetailedLatencyTracker::new(50),
            queue_size_samples: Mutex::new(VecDeque::with_capacity(200)),
            blocks_produced: AtomicUsize::new(0),
            blocks_committed: AtomicUsize::new(0),
            transactions_in_blocks: AtomicUsize::new(0),
            start_time: Instant::now(),
        }
    }
    
    pub async fn record_queue_size(&self, size: usize) {
        let mut samples = self.queue_size_samples.lock().await;
        let now = Instant::now();
        
        if samples.len() >= 200 {
            samples.pop_front();
        }
        samples.push_back((now, size));
    }
    
    pub fn record_block_produced(&self, tx_count: usize) {
        self.blocks_produced.fetch_add(1, Ordering::Relaxed);
        self.transactions_in_blocks.fetch_add(tx_count, Ordering::Relaxed);
    }
    
    pub fn record_block_committed(&self, tx_count: usize) {
        self.blocks_committed.fetch_add(1, Ordering::Relaxed);
    }
    
    pub async fn get_comprehensive_stats(&self) -> HotStuffQueueStats {
        let production_stats = self.block_production_tracker.get_statistics().await;
        let validation_stats = self.block_validation_tracker.get_statistics().await;
        let consensus_stats = self.consensus_tracker.get_statistics().await;
        
        let blocks_produced = self.blocks_produced.load(Ordering::Relaxed);
        let blocks_committed = self.blocks_committed.load(Ordering::Relaxed);
        let total_transactions = self.transactions_in_blocks.load(Ordering::Relaxed);
        let elapsed = self.start_time.elapsed();
        
        let block_production_tps = if elapsed.as_secs_f64() > 0.0 {
            blocks_produced as f64 / elapsed.as_secs_f64()
        } else {
            0.0
        };
        
        let transaction_tps = if elapsed.as_secs_f64() > 0.0 {
            total_transactions as f64 / elapsed.as_secs_f64()
        } else {
            0.0
        };
        
        // 计算平均队列大小
        let samples = self.queue_size_samples.lock().await;
        let avg_queue_size = if !samples.is_empty() {
            samples.iter().map(|(_, size)| *size).sum::<usize>() as f64 / samples.len() as f64
        } else {
            0.0
        };
        
        HotStuffQueueStats {
            block_production_tps,
            transaction_tps,
            blocks_produced_total: blocks_produced,
            blocks_committed_total: blocks_committed,
            transactions_total: total_transactions,
            avg_queue_size,
            block_production: production_stats,
            block_validation: validation_stats,
            consensus: consensus_stats,
            elapsed_time: elapsed,
        }
    }
}

#[derive(Debug)]
pub struct HotStuffQueueStats {
    pub block_production_tps: f64,
    pub transaction_tps: f64,
    pub blocks_produced_total: usize,
    pub blocks_committed_total: usize,
    pub transactions_total: usize,
    pub avg_queue_size: f64,
    pub block_production: LatencyStatistics,
    pub block_validation: LatencyStatistics,
    pub consensus: LatencyStatistics,
    pub elapsed_time: Duration,
}

// 格式化输出函数
pub fn format_duration_micros(duration: Duration) -> String {
    let micros = duration.as_micros();
    if micros < 1000 {
        format!("{}μs", micros)
    } else if micros < 1_000_000 {
        format!("{:.1}ms", micros as f64 / 1000.0)
    } else {
        format!("{:.2}s", micros as f64 / 1_000_000.0)
    }
}

pub fn print_pompe_detailed_stats(node_id: usize, stats: &PompeQueueStats) {
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("📊 [Node {}] Pompe队列详细性能报告", node_id);
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    
    // 总体TPS
    println!("🎯 总体吞吐量:");
    println!("   提交TPS: {:.2} tx/s | 完成TPS: {:.2} tx/s | 平均队列大小: {:.1}", 
             stats.submission_tps, stats.completion_tps, stats.avg_queue_size);
    println!("   总提交: {} | 总完成: {} | 运行时间: {:.1}s", 
             stats.submitted_total, stats.completed_total, stats.elapsed_time.as_secs_f64());
    
    // 各阶段延迟
    println!("\n⏱️ 各阶段处理延迟:");
    print_stage_latency("批处理", &stats.batch_processing);
    print_stage_latency("Ordering1", &stats.ordering1);
    print_stage_latency("Ordering2", &stats.ordering2);
    print_stage_latency("提交", &stats.commit);
}

pub fn print_hotstuff_detailed_stats(node_id: usize, stats: &HotStuffQueueStats) {
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("🔥 [Node {}] HotStuff队列详细性能报告", node_id);
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    
    // 总体TPS
    println!("🎯 总体吞吐量:");
    println!("   区块TPS: {:.2} blocks/s | 交易TPS: {:.2} tx/s | 平均队列大小: {:.1}", 
             stats.block_production_tps, stats.transaction_tps, stats.avg_queue_size);
    println!("   总区块: {} | 已提交: {} | 总交易: {} | 运行时间: {:.1}s", 
             stats.blocks_produced_total, stats.blocks_committed_total, 
             stats.transactions_total, stats.elapsed_time.as_secs_f64());
    
    // 各阶段延迟
    println!("\n⏱️ 各阶段处理延迟:");
    print_stage_latency("区块生产", &stats.block_production);
    print_stage_latency("区块验证", &stats.block_validation);
    print_stage_latency("共识确认", &stats.consensus);
}

fn print_stage_latency(stage_name: &str, stats: &LatencyStatistics) {
    println!("   📈 {}: TPS={:.2}, 平均延迟={}, 总处理={}", 
             stage_name, stats.tps, format_duration_micros(stats.average_latency), stats.total_processed);
    
    if let Some(recent) = &stats.recent {
        println!("      最近延迟分布: 平均={} | P50={} | P95={} | P99={} | 最大={} | 样本={}",
                 format_duration_micros(recent.avg),
                 format_duration_micros(recent.p50),
                 format_duration_micros(recent.p95),
                 format_duration_micros(recent.p99),
                 format_duration_micros(recent.max),
                 recent.sample_count);
    }
}