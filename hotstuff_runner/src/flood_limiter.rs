use std::sync::{
    atomic::{AtomicU64, Ordering},
    Mutex,
};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tracing::warn;

#[derive(Debug)]
struct LimiterState {
    tokens: f64,
    last_refill: Instant,
}

/// Simple token-bucket rate limiter used to keep flood traffic under control.
#[derive(Debug)]
pub struct FloodLimiter {
    label: String,
    rate_per_sec: f64,
    burst_capacity: f64,
    state: Mutex<LimiterState>,
    dropped: AtomicU64,
    last_log_millis: AtomicU64,
}

impl FloodLimiter {
    pub fn unlimited(label: impl Into<String>) -> Self {
        Self::new(label, 0.0, 1.0)
    }

    pub fn new(label: impl Into<String>, rate_per_sec: f64, burst_capacity: f64) -> Self {
        let clamped_rate = if rate_per_sec.is_finite() {
            rate_per_sec.max(0.0)
        } else {
            0.0
        };
        let clamped_burst = if burst_capacity.is_finite() {
            burst_capacity.max(1.0)
        } else {
            1.0
        };

        Self {
            label: label.into(),
            rate_per_sec: clamped_rate,
            burst_capacity: clamped_burst,
            state: Mutex::new(LimiterState {
                tokens: clamped_burst,
                last_refill: Instant::now(),
            }),
            dropped: AtomicU64::new(0),
            last_log_millis: AtomicU64::new(0),
        }
    }

    pub fn allow(&self, cost: f64) -> bool {
        if self.rate_per_sec <= 0.0 {
            return true;
        }

        let mut state = self.state.lock().unwrap();
        let now = Instant::now();
        let elapsed = now.duration_since(state.last_refill).as_secs_f64();
        if elapsed > 0.0 {
            let refill = elapsed * self.rate_per_sec;
            state.tokens = (state.tokens + refill).min(self.burst_capacity);
            state.last_refill = now;
        }

        if state.tokens >= cost {
            state.tokens -= cost;
            true
        } else {
            drop(state);
            self.record_drop();
            false
        }
    }

    pub fn note_drop(&self) {
        self.record_drop();
    }

    fn record_drop(&self) {
        let drops = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
        const LOG_INTERVAL_MS: u64 = 2_000;
        let now_ms = current_millis();
        let last = self.last_log_millis.load(Ordering::Relaxed);
        if now_ms.saturating_sub(last) >= LOG_INTERVAL_MS {
            if self
                .last_log_millis
                .compare_exchange(last, now_ms, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                warn!(
                    "[flood-limiter] {} dropping traffic ({} drops so far, rate {:.0} msg/s, burst {:.0})",
                    self.label, drops, self.rate_per_sec, self.burst_capacity
                );
            }
        }
    }
}

fn current_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
