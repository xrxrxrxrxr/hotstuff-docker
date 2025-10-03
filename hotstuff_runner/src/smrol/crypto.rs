use ed25519_dalek::{Signature, VerifyingKey};
use rand::SeedableRng;
use rand_chacha::ChaChaRng;
use reed_solomon_erasure::galois_8::ReedSolomon;
use rs_merkle::{algorithms::Sha256, Hasher, MerkleTree};
use serde::{Deserialize, Serialize};
use sha2::Digest;
use sha2::Sha256 as Sha256Digest;
use std::collections::{HashMap, HashSet};
use threshold_crypto::{PublicKeySet, SecretKeySet, SecretKeyShare};

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
        self.shares.len() == self.threshold
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

pub fn derive_threshold_keys(
    node_id: usize,
    f: usize,
    verifying_keys: &HashMap<usize, VerifyingKey>,
) -> Result<(SecretKeyShare, PublicKeySet), String> {
    if verifying_keys.len() < f + 1 {
        return Err("verifying key count smaller than threshold".to_string());
    }

    let mut ids: Vec<_> = verifying_keys.keys().cloned().collect();
    ids.sort_unstable();

    let mut hasher = Sha256Digest::new();
    hasher.update((ids.len() as u32).to_be_bytes());
    hasher.update((f as u32).to_be_bytes());

    for id in &ids {
        let vk = verifying_keys
            .get(id)
            .ok_or_else(|| format!("missing verifying key for node {}", id))?;
        hasher.update((*id as u64).to_be_bytes());
        hasher.update(vk.to_bytes());
    }

    let seed: [u8; 32] = hasher.finalize().into();
    let mut rng = ChaChaRng::from_seed(seed);
    let secret_set = SecretKeySet::random(f, &mut rng);
    let share = secret_set.secret_key_share(node_id);
    let public = secret_set.public_keys();
    Ok((share, public))
}

/// Holds the shards produced by Reed-Solomon erasure coding together with
/// metadata required to rebuild the original payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErasurePackage {
    pub data_shards: usize,
    pub parity_shards: usize,
    pub shard_size: usize,
    pub original_len: usize,
    pub shards: Vec<Vec<u8>>,
}

impl ErasurePackage {
    /// Encode `payload` into `data_shards + parity_shards` shards.
    pub fn encode(payload: &[u8], data_shards: usize, total_shards: usize) -> Result<Self, String> {
        if data_shards == 0 {
            return Err("data_shards must be positive".to_string());
        }
        if total_shards < data_shards {
            return Err("total_shards must be >= data_shards".to_string());
        }

        let parity_shards = total_shards - data_shards;
        let shard_size = std::cmp::max(1, (payload.len() + data_shards - 1) / data_shards);
        let shard_count = data_shards + parity_shards;
        let mut shards: Vec<Vec<u8>> = vec![vec![0u8; shard_size]; shard_count];

        for i in 0..data_shards {
            let start = i * shard_size;
            let end = std::cmp::min(start + shard_size, payload.len());
            if start < payload.len() {
                let slice = &payload[start..end];
                shards[i][..slice.len()].copy_from_slice(slice);
            }
        }

        if parity_shards > 0 {
            let mut shard_refs: Vec<&mut [u8]> = shards
                .iter_mut()
                .map(|shard| shard.as_mut_slice())
                .collect();
            ReedSolomon::new(data_shards, parity_shards)
                .map_err(|e| e.to_string())?
                .encode(&mut shard_refs)
                .map_err(|e| e.to_string())?;
        }

        Ok(Self {
            data_shards,
            parity_shards,
            shard_size,
            original_len: payload.len(),
            shards,
        })
    }

    /// Compute the Merkle root over the shard hashes.
    pub fn merkle_root(&self) -> [u8; 32] {
        compute_merkle_root(&self.shards)
    }

    /// Reconstruct the original payload. This works even if some shards are
    /// missing (represented by `None`), as long as at least `data_shards`
    /// shards remain.
    pub fn reconstruct(&self, mut shards: Vec<Option<Vec<u8>>>) -> Result<Vec<u8>, String> {
        if shards.len() != self.shards.len() {
            return Err("shard vector length mismatch".to_string());
        }

        ReedSolomon::new(self.data_shards, self.parity_shards)
            .map_err(|e| e.to_string())?
            .reconstruct(&mut shards)
            .map_err(|e| e.to_string())?;

        let mut data = Vec::with_capacity(self.original_len);
        for shard in shards.into_iter().take(self.data_shards) {
            let shard = shard.ok_or_else(|| "missing shard after reconstruction".to_string())?;
            if data.len() >= self.original_len {
                break;
            }
            let remaining = self.original_len - data.len();
            let take = std::cmp::min(remaining, shard.len());
            data.extend_from_slice(&shard[..take]);
        }
        data.truncate(self.original_len);
        Ok(data)
    }

    /// Convenience wrapper when all shards are locally available.
    pub fn reconstruct_full(&self) -> Result<Vec<u8>, String> {
        let shards = self.shards.iter().cloned().map(Some).collect::<Vec<_>>();
        self.reconstruct(shards)
    }
}

/// Compute the Merkle root using SHA-256 for a list of shard payloads.
pub fn compute_merkle_root(shards: &[Vec<u8>]) -> [u8; 32] {
    if shards.is_empty() {
        return [0u8; 32];
    }

    let leaf_hashes: Vec<[u8; 32]> = shards.iter().map(|shard| Sha256::hash(shard)).collect();

    MerkleTree::<Sha256>::from_leaves(&leaf_hashes)
        .root()
        .unwrap_or([0u8; 32])
}
