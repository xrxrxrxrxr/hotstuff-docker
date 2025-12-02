use std::sync::OnceLock;
use tracing::warn;

const DEFAULT_BYTES_PER_TX: usize = 57;
const BYTES_PER_TX_ENV: &str = "HOTSTUFF_EQUIV_TX_BYTES";
const SYNTHETIC_ID_MULTIPLIER: u64 = 100_000;
const MAX_SYNTHETIC_TX_PER_BATCH: usize = SYNTHETIC_ID_MULTIPLIER as usize;

pub fn parse_transaction_id(tx_str: &str) -> Option<u64> {
    let trimmed = tx_str.trim();

    // Format 1: pompe:timestamp:tx_id:from->to:amount
    let parts: Vec<&str> = trimmed.split(':').collect();
    if parts.len() >= 4 && parts[0] == "pompe" {
        return parts[2].parse::<u64>().ok();
    }

    // Format 1b: smrol:final_sequence:tx_id:from->to:amount
    if parts.len() >= 3 && parts[0] == "smrol" {
        return parts[2].parse::<u64>().ok();
    }

    // Format 2: tx_id:from->to:amount
    if parts.len() >= 3 {
        if let Ok(id) = parts[0].parse::<u64>() {
            return Some(id);
        }
    }

    // Format 3: "tx_123"
    if trimmed.starts_with("tx_") {
        return trimmed[3..].parse::<u64>().ok();
    }

    // Format 4: raw numeric ID
    trimmed.parse::<u64>().ok()
}

fn bytes_per_nominal_tx() -> usize {
    static CACHE: OnceLock<usize> = OnceLock::new();
    *CACHE.get_or_init(|| {
        std::env::var(BYTES_PER_TX_ENV)
            .ok()
            .and_then(|raw| raw.trim().parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_BYTES_PER_TX)
    })
}

pub fn equivalent_tx_count(payload_len_bytes: usize) -> usize {
    let bytes_per_tx = bytes_per_nominal_tx();
    let count = (payload_len_bytes + bytes_per_tx - 1) / bytes_per_tx;
    count.max(1)
}

pub fn synthetic_tx_ids(base_tx_id: u64, equiv_count: usize) -> Vec<u64> {
    if equiv_count == 0 {
        return vec![];
    }

    let capped = if equiv_count > MAX_SYNTHETIC_TX_PER_BATCH {
        warn!(
            "[synthetic-tx] capped equivalent count {} -> {} for tx {}",
            equiv_count, MAX_SYNTHETIC_TX_PER_BATCH, base_tx_id
        );
        MAX_SYNTHETIC_TX_PER_BATCH
    } else {
        equiv_count
    };

    let base = match base_tx_id.checked_mul(SYNTHETIC_ID_MULTIPLIER) {
        Some(value) => value,
        None => {
            warn!(
                "[synthetic-tx] overflow while scaling tx_id {}, using original id",
                base_tx_id
            );
            base_tx_id
        }
    };

    (0..capped)
        .map(|idx| base.saturating_add(idx as u64))
        .collect()
}

pub fn synthetic_id_multiplier() -> u64 {
    SYNTHETIC_ID_MULTIPLIER
}

pub fn original_tx_id_from_synthetic(tx_id: u64) -> u64 {
    if tx_id < SYNTHETIC_ID_MULTIPLIER {
        tx_id
    } else {
        tx_id / SYNTHETIC_ID_MULTIPLIER
    }
}
