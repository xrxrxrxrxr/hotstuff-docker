// hotstuff_runner/src/utils.rs
use log::{debug, warn};
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

pub fn format_system_time(time: SystemTime) -> String {
    match time.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(duration) => {
            let secs = duration.as_secs();
            let nanos = duration.subsec_nanos();
            format!("{}.{:09}", secs, nanos)
        }
        Err(_) => "invalid_time".to_string(),
    }
}

// 或者使用 chrono 格式化为可读时间
pub fn format_system_time_readable(time: SystemTime) -> String {
    use chrono::{DateTime, Local};
    let datetime: DateTime<Local> = time.into();
    datetime.format("%Y-%m-%d %H:%M:%S%.3f").to_string()
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DigitalSignature {
    pub node_id: usize,
    pub signature: Vec<u8>,
}

pub fn generate_dummy_signatures(nfaulty: usize) -> Vec<DigitalSignature> {
    let count = 2 * nfaulty + 1;
    (0..count)
        .map(|i| DigitalSignature {
            node_id: i,
            signature: vec![0u8; 64], // 64字节dummy签名
        })
        .collect()
}

pub fn verify_dummy_signatures(signatures: &[DigitalSignature], tx_hash: &str) -> bool {
    // 基本检查：签名数量和格式
    if signatures.is_empty() {
        warn!("⚠️ [签名验证] 签名列表为空");
        return false;
    }

    let mut valid_count = 0;
    for sig in signatures {
        // Dummy验证：检查签名长度是否为64字节
        if sig.signature.len() == 64 {
            valid_count += 1;
        } else {
            warn!(
                "⚠️ [签名验证] Node {} 签名长度异常: {} bytes",
                sig.node_id,
                sig.signature.len()
            );
        }
    }

    // 验证通过条件：所有签名格式正确
    let result = valid_count == signatures.len();

    if result {
        debug!(
            "✅ [签名验证] 验证通过: {} 个签名, tx_hash = {}",
            valid_count,
            &tx_hash[0..8]
        );
    } else {
        warn!(
            "❌ [签名验证] 验证失败: {}/{} 个有效签名",
            valid_count,
            signatures.len()
        );
    }

    result
}
