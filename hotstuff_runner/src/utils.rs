// hotstuff_runner/src/utils.rs
use ed25519_dalek::{Signature as Ed25519Signature, VerifyingKey};
use log::{debug, warn};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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

pub fn extract_signatures(responses: &[(usize, u64, Vec<u8>)]) -> Vec<DigitalSignature> {
    responses
        .iter()
        .map(|(node_id, _timestamp, signature)| DigitalSignature {
            node_id: *node_id,
            signature: signature.clone(),
        })
        .collect()
}

pub fn verify_signatures(
    signatures: &[DigitalSignature],
    tx_hash: &str,
    verifying_keys: &HashMap<usize, VerifyingKey>,
) -> bool {
    // 基本检查：签名数量和格式
    if signatures.is_empty() {
        warn!("⚠️ [签名验证] 签名列表为空");
        return false;
    }

    let message = format!("ordering1:{}", tx_hash);
    let mut valid_count = 0;

    for sig in signatures {
        let Some(verifying_key) = verifying_keys.get(&sig.node_id) else {
            warn!("⚠️ [签名验证] 缺少节点 {} 的公钥", sig.node_id);
            return false;
        };

        if sig.signature.len() != 64 {
            warn!(
                "⚠️ [签名验证] Node {} 签名长度异常: {} bytes",
                sig.node_id,
                sig.signature.len()
            );
            return false;
        }

        let mut sig_buf = [0u8; 64];
        sig_buf.copy_from_slice(&sig.signature);
        let signature = match Ed25519Signature::try_from(&sig_buf[..]) {
            Ok(sig_obj) => sig_obj,
            Err(_) => {
                warn!("⚠️ [签名验证] Node {} 签名解析失败", sig.node_id);
                return false;
            }
        };

        if verifying_key
            .verify_strict(message.as_bytes(), &signature)
            .is_err()
        {
            warn!("⚠️ [签名验证] Node {} 签名验证失败", sig.node_id);
            return false;
        }

        valid_count += 1;
    }

    debug!(
        "✅ [签名验证] 验证通过: {} 个签名, tx_hash = {}",
        valid_count,
        &tx_hash[0..std::cmp::min(8, tx_hash.len())]
    );
    true
}
