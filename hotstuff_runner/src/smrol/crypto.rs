use blsttc::{PublicKeySet, SecretKeySet, SecretKeyShare};
use ed25519_dalek::VerifyingKey;
use rand08::{rngs::StdRng, SeedableRng};
use reed_solomon_erasure::galois_8::ReedSolomon;
use rs_merkle::{algorithms::Sha256, Hasher, MerkleTree};
use serde::{Deserialize, Serialize};
use sha2::Digest;
use sha2::Sha256 as Sha256Digest;
use std::collections::HashMap;

use blst::{
    min_sig::{
        AggregatePublicKey as BlstAggregatePublicKey, AggregateSignature as BlstAggregateSignature,
        PublicKey as BlstPublicKey, SecretKey as BlstSecretKey, Signature as BlstSignature,
    },
    BLST_ERROR,
};
use std::sync::Arc;

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
    let mut rng = StdRng::from_seed(seed);
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

/// Multi-signature key material for all participants, backed by `blst`.
pub struct MultiSigContext {
    secret_keys: Vec<BlstSecretKey>,
    public_keys: Vec<BlstPublicKey>,
}

impl MultiSigContext {
    /// Domain separation tag used for BLS signatures in this context.
    pub const DST: &'static [u8] = b"BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_NUL_";

    /// Deterministically derive per-node keys from a seed.
    pub fn new(num_nodes: usize, ikm_seed: &[u8]) -> Result<Self, BLST_ERROR> {
        let mut secret_keys = Vec::with_capacity(num_nodes);
        let mut public_keys = Vec::with_capacity(num_nodes);

        for idx in 0..num_nodes {
            let mut hasher = Sha256Digest::new();
            hasher.update(ikm_seed);
            hasher.update((idx as u64).to_be_bytes());
            let ikm = hasher.finalize();

            let sk = BlstSecretKey::key_gen(ikm.as_slice(), &[])?;
            let pk = sk.sk_to_pk();
            secret_keys.push(sk);
            public_keys.push(pk);
        }

        Ok(Self {
            secret_keys,
            public_keys,
        })
    }

    pub fn participant_count(&self) -> usize {
        self.secret_keys.len()
    }

    pub fn public_key(&self, node_id: usize) -> Option<&BlstPublicKey> {
        self.public_keys.get(node_id)
    }

    pub fn sign(&self, node_id: usize, msg: &[u8]) -> Option<BlstSignature> {
        self.secret_keys
            .get(node_id)
            .map(|sk| sk.sign(msg, Self::DST, &[]))
    }

    pub fn aggregate(
        &self,
        signatures: &[(usize, BlstSignature)],
    ) -> Result<(BlstAggregateSignature, BlstAggregatePublicKey), BLST_ERROR> {
        if signatures.is_empty() {
            return Err(BLST_ERROR::BLST_AGGR_TYPE_MISMATCH);
        }

        let mut sig_refs: Vec<&BlstSignature> = Vec::with_capacity(signatures.len());
        let mut pk_refs: Vec<&BlstPublicKey> = Vec::with_capacity(signatures.len());

        for (idx, sig) in signatures {
            let pk = self
                .public_keys
                .get(*idx)
                .ok_or(BLST_ERROR::BLST_AGGR_TYPE_MISMATCH)?;
            sig_refs.push(sig);
            pk_refs.push(pk);
        }

        let agg_sig = BlstAggregateSignature::aggregate(&sig_refs, true)?;
        let agg_pk = BlstAggregatePublicKey::aggregate(&pk_refs, true)?;
        Ok((agg_sig, agg_pk))
    }

    pub fn verify(
        &self,
        msg: &[u8],
        aggregate_sig: &BlstAggregateSignature,
        signers: &[usize],
    ) -> Result<bool, BLST_ERROR> {
        if signers.is_empty() {
            return Ok(false);
        }

        let mut pk_refs: Vec<&BlstPublicKey> = Vec::with_capacity(signers.len());
        for idx in signers {
            let pk = self
                .public_keys
                .get(*idx)
                .ok_or(BLST_ERROR::BLST_AGGR_TYPE_MISMATCH)?;
            pk_refs.push(pk);
        }

        let agg_pk = BlstAggregatePublicKey::aggregate(&pk_refs, true)?;
        let sig = aggregate_sig.to_signature();
        let err = sig.verify(true, msg, Self::DST, &[], &agg_pk.to_public_key(), true);
        Ok(err == BLST_ERROR::BLST_SUCCESS)
    }
}

pub fn derive_multisig_context(
    verifying_keys: &HashMap<usize, VerifyingKey>,
) -> Result<MultiSigContext, String> {
    if verifying_keys.is_empty() {
        return Err("verifying key set is empty".to_string());
    }

    let mut ids: Vec<_> = verifying_keys.keys().cloned().collect();
    ids.sort_unstable();

    let mut hasher = Sha256Digest::new();
    hasher.update((ids.len() as u32).to_be_bytes());

    for id in &ids {
        let vk = verifying_keys
            .get(id)
            .ok_or_else(|| format!("missing verifying key for node {}", id))?;
        hasher.update((*id as u64).to_be_bytes());
        hasher.update(vk.to_bytes());
    }

    let seed: [u8; 32] = hasher.finalize().into();
    MultiSigContext::new(ids.len(), &seed)
        .map_err(|e| format!("failed to derive multisig context: {:?}", e))
}
