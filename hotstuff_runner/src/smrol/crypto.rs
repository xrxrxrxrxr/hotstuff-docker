use ed25519_dalek::{Signature, VerifyingKey};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SmrolShare {
    pub signer: usize,
    pub signature: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SmrolCombinedSignature {
    pub threshold: usize,
    pub shares: Vec<SmrolShare>,
}

impl SmrolCombinedSignature {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        bincode::deserialize(bytes).map_err(|e| e.to_string())
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, String> {
        bincode::serialize(self).map_err(|e| e.to_string())
    }

    pub fn verify(
        &self,
        message: &[u8],
        verifying_keys: &HashMap<usize, VerifyingKey>,
        required: usize,
    ) -> Result<bool, String> {
        if self.shares.len() < required {
            return Ok(false);
        }

        let mut seen = HashSet::new();
        for share in &self.shares {
            if !seen.insert(share.signer) {
                return Ok(false);
            }

            let key = verifying_keys
                .get(&share.signer)
                .ok_or_else(|| format!("verifying key missing for signer {}", share.signer))?;

            if !verify_signature_share(&share.signature, message, key) {
                return Ok(false);
            }
        }

        Ok(true)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SmrolThresholdSig {
    pub threshold: usize,
    pub shares: HashMap<usize, Vec<u8>>,
}

impl SmrolThresholdSig {
    pub fn new(threshold: usize) -> Self {
        Self {
            threshold,
            shares: HashMap::new(),
        }
    }

    pub fn add_share(&mut self, node_id: usize, share_bytes: Vec<u8>) -> bool {
        self.shares.entry(node_id).or_insert(share_bytes);
        self.shares.len() >= self.threshold
    }

    pub fn combine(&self) -> Result<Vec<u8>, String> {
        if self.shares.len() < self.threshold {
            return Err("share inadequate".to_string());
        }

        let mut entries: Vec<_> = self.shares.iter().collect();
        entries.sort_by_key(|(id, _)| *id);

        let shares = entries
            .into_iter()
            .take(self.threshold)
            .map(|(signer, sig)| SmrolShare {
                signer: *signer,
                signature: (*sig).clone(),
            })
            .collect::<Vec<_>>();

        SmrolCombinedSignature {
            threshold: self.threshold,
            shares,
        }
        .to_bytes()
    }
}

pub fn verify_signature_share(
    signature_bytes: &[u8],
    message: &[u8],
    verifying_key: &VerifyingKey,
) -> bool {
    if signature_bytes.len() != 64 {
        return false;
    }

    let mut sig_array = [0u8; 64];
    sig_array.copy_from_slice(signature_bytes);

    let signature = Signature::from_bytes(&sig_array);
    verifying_key.verify_strict(message, &signature).is_ok()
}

pub fn verify_combined_signature_bytes(
    combined_signature: &[u8],
    message: &[u8],
    verifying_keys: &HashMap<usize, VerifyingKey>,
    required: usize,
) -> Result<bool, String> {
    let combined = SmrolCombinedSignature::from_bytes(combined_signature)?;
    combined.verify(message, verifying_keys, required)
}
