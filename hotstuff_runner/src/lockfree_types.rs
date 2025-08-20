// 新建文件: src/lockfree_types.rs
// 共享的无锁数据结构，避免重复定义

use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use crossbeam::queue::SegQueue;

/// 无锁交易队列，使用 crossbeam 的 SegQueue 实现
#[derive(Debug)]
pub struct LockFreeTransactionQueue {
    queue: Arc<SegQueue<String>>,
    size_counter: AtomicUsize,
}

impl LockFreeTransactionQueue {
    pub fn new() -> Self {
        Self {
            queue: Arc::new(SegQueue::new()),
            size_counter: AtomicUsize::new(0),
        }
    }
    
    pub fn push(&self, item: String) {
        self.queue.push(item);
        self.size_counter.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn pop(&self) -> Option<String> {
        if let Some(item) = self.queue.pop() {
            self.size_counter.fetch_sub(1, Ordering::Relaxed);
            Some(item)
        } else {
            None
        }
    }
    
    pub fn len(&self) -> usize {
        self.size_counter.load(Ordering::Relaxed)
    }
    
    pub fn drain_batch(&self, max_size: usize) -> Vec<String> {
        let mut batch = Vec::with_capacity(max_size);
        for _ in 0..max_size {
            if let Some(item) = self.pop() {
                batch.push(item);
            } else {
                break;
            }
        }
        batch
    }
    
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// 无锁性能统计
#[derive(Debug)]
pub struct LockFreePerformanceStats {
    submitted_count: AtomicUsize,
    confirmed_count: AtomicUsize,
    confirmed_blocks: AtomicUsize,
    start_time: std::time::Instant,
}

impl LockFreePerformanceStats {
    pub fn new() -> Self {
        Self {
            submitted_count: AtomicUsize::new(0),
            confirmed_count: AtomicUsize::new(0),
            confirmed_blocks: AtomicUsize::new(0),
            start_time: std::time::Instant::now(),
        }
    }
    
    pub fn record_submitted(&self) {
        self.submitted_count.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn record_confirmed(&self, count: usize) {
        self.confirmed_count.fetch_add(count, Ordering::Relaxed);
        self.confirmed_blocks.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn get_submission_tps(&self) -> f64 {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            self.submitted_count.load(Ordering::Relaxed) as f64 / elapsed
        } else {
            0.0
        }
    }
    
    pub fn get_confirmation_tps(&self) -> f64 {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            self.confirmed_count.load(Ordering::Relaxed) as f64 / elapsed
        } else {
            0.0
        }
    }
    
    pub fn get_confirmed_transactions(&self) -> usize {
        self.confirmed_count.load(Ordering::Relaxed)
    }
    
    pub fn get_confirmed_blocks(&self) -> usize {
        self.confirmed_blocks.load(Ordering::Relaxed)
    }
}

/// 兼容性适配器，为现有 API 提供兼容接口
pub struct LockFreeQueueAdapter;

impl LockFreeQueueAdapter {
    pub fn create_empty_mutex() -> Arc<std::sync::Mutex<Vec<String>>> {
        // 返回空的互斥包装器 - 实际操作绕过此包装器
        Arc::new(std::sync::Mutex::new(Vec::new()))
    }
}