use crate::smrol::{
    consensus::{SequenceEntry, TransactionEntry},
    message::{SmrolMessage, SmrolTransaction},
    network::SmrolTcpNetwork,
};
use ed25519_dalek::SigningKey;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::{debug, info};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinalizationValue {
    pub vc_tx: Vec<u8>,
    pub tx_data: String,
}

#[derive(Debug)]
pub struct OutputFinalization {
    pub process_id: usize,
    pub f: usize,
    pub network: Arc<SmrolTcpNetwork>,
    pub signing_key: SigningKey,
    pub vc_ledger: HashSet<Vec<u8>>,
    pub final_ledger: HashMap<u64, Vec<String>>, // epoch -> transactions
    pub mi: HashMap<u64, Vec<TransactionEntry>>,
}

impl OutputFinalization {
    pub fn new(
        process_id: usize,
        f: usize,
        network: Arc<SmrolTcpNetwork>,
        signing_key: SigningKey,
    ) -> Self {
        Self {
            process_id,
            f,
            network,
            signing_key,
            vc_ledger: HashSet::new(),
            final_ledger: HashMap::new(),
            mi: HashMap::new(),
        }
    }

    /// Algorithm 4 - Output Finalization for epoch e
    pub async fn finalize_epoch(
        &mut self,
        epoch: u64,
        m_e: Vec<TransactionEntry>,
        s_e: Vec<SequenceEntry>,
        t_e: HashSet<usize>,
    ) -> Result<Vec<String>, String> {
        debug!("🔄 [Finalization] Starting epoch {} finalization", epoch);

        // Line 59: Parse M_e := {(vc_tx_k', s_tx_k', Σ)}
        let mut s_prime: Vec<u64> = Vec::new();

        // Line 60: for ∀ (vc_tx_k', s_tx_k', Σ) ∈ M_e: S' ← S' ∪ s_tx_k'
        for entry in &m_e {
            s_prime.push(entry.s_tx);
        }

        // Line 61: Sort and get median
        s_prime.sort();
        let h_e_prime = if !s_prime.is_empty() {
            s_prime[s_prime.len() - 1] // max value as h_e'
        } else {
            0
        };

        // Line 62: let M_e' = {vc_tx_1', vc_tx_2', ..., vc_tx_l'}
        let m_e_prime: Vec<Vec<u8>> = m_e.iter().map(|entry| entry.vc_tx.clone()).collect();

        // Lines 63-74: Process pending VCs in order
        for vc_tx in &m_e_prime {
            for entry in &self.get_pending_for_epoch(epoch) {
                if &entry.vc_tx == vc_tx && !m_e_prime.contains(&entry.vc_tx) {
                    if self.appears_at_least_f_plus_1_times(&entry.vc_tx, &s_e, epoch, &t_e) {
                        let s_tx_prime = self.calculate_median_sequence(&entry.vc_tx, &s_e);
                        self.process_value(epoch, entry, s_tx_prime).await?;
                    }
                }
            }
        }

        // Lines 75-82: Extract and deliver transactions
        self.extract_and_deliver(epoch, &m_e).await?;

        let finalized = self.final_ledger.get(&epoch).cloned().unwrap_or_default();

        info!(
            "✅ [Finalization] Epoch {} finalized with {} transactions",
            epoch,
            finalized.len()
        );
        Ok(finalized)
    }

    // Line 65: Check if vc_tx appears at least f+1 times
    fn appears_at_least_f_plus_1_times(
        &self,
        vc_tx: &[u8],
        s_e: &[SequenceEntry],
        epoch: u64,
        t_e: &HashSet<usize>,
    ) -> bool {
        let count = s_e
            .iter()
            .filter(|entry| entry.vc_j_h_e == *vc_tx && t_e.contains(&entry.j))
            .count();
        count >= self.f + 1
    }

    // Line 70: Calculate median sequence number
    fn calculate_median_sequence(&self, vc_tx: &[u8], s_e: &[SequenceEntry]) -> u64 {
        let mut sequences: Vec<u64> = s_e
            .iter()
            .filter(|entry| entry.vc_j_h_e == *vc_tx)
            .map(|entry| entry.s_tx)
            .collect();

        sequences.sort();
        if sequences.is_empty() {
            0
        } else {
            sequences[sequences.len() / 2]
        }
    }

    // Lines 71-74: Process individual value
    async fn process_value(
        &mut self,
        epoch: u64,
        entry: &TransactionEntry,
        s_tx_prime: u64,
    ) -> Result<(), String> {
        // Line 72-73: Sort and insert
        let final_entries = self.final_ledger.entry(epoch).or_default();

        // Line 74: Insert Value[vc_tx_k'] in correct position
        let formatted = Self::format_transaction_entry(entry, s_tx_prime);
        final_entries.push(formatted);
        final_entries.sort();

        Ok(())
    }

    // Lines 75-82: Extract transactions and deliver
    async fn extract_and_deliver(
        &mut self,
        epoch: u64,
        entries: &[TransactionEntry],
    ) -> Result<(), String> {
        for entry in entries {
            if !self.transaction_received(&entry.vc_tx) {
                self.call_help_value(&entry.vc_tx).await?;
            }

            let final_entries = self.final_ledger.entry(epoch).or_default();
            final_entries.push(Self::format_transaction_entry(entry, entry.s_tx));
        }

        for entry in entries {
            self.mi
                .entry(epoch)
                .or_default()
                .retain(|pending| pending.vc_tx != entry.vc_tx);
            self.vc_ledger.insert(entry.vc_tx.clone());
        }

        self.smrol_delivery(epoch).await
    }

    fn format_transaction_entry(entry: &TransactionEntry, sequence: u64) -> String {
        if let Ok(tx) = bincode::deserialize::<SmrolTransaction>(&entry.payload) {
            tx.to_hotstuff_format(sequence)
        } else {
            format!(
                "{}:{}",
                sequence,
                hex::encode(&entry.vc_tx[..std::cmp::min(8, entry.vc_tx.len())])
            )
        }
    }

    fn transaction_received(&self, _vc_tx: &[u8]) -> bool {
        true
    }

    async fn call_help_value(&self, vc_tx: &[u8]) -> Result<(), String> {
        debug!(
            "🆘 [Finalization] Requesting help for VC: {}",
            hex::encode(vc_tx)
        );
        Ok(())
    }

    // Helper: Get pending entries for epoch
    fn get_pending_for_epoch(&self, epoch: u64) -> Vec<TransactionEntry> {
        self.mi.get(&epoch).cloned().unwrap_or_default()
    }

    // Line 82: SMROL delivery mechanism
    async fn smrol_delivery(&self, epoch: u64) -> Result<(), String> {
        if let Some(transactions) = self.final_ledger.get(&epoch) {
            info!(
                "📦 [Finalization] Delivering {} transactions for epoch {}",
                transactions.len(),
                epoch
            );

            // Deliver to application layer
            for (i, tx) in transactions.iter().enumerate() {
                debug!("  📄 Transaction {}: {}", i + 1, tx);
            }
        }
        Ok(())
    }

    // Public interface
    pub fn add_to_mi(&mut self, epoch: u64, entry: TransactionEntry) {
        self.mi.entry(epoch).or_default().push(entry);
    }

    pub fn get_final_ledger_size(&self, epoch: u64) -> usize {
        self.final_ledger
            .get(&epoch)
            .map(|txs| txs.len())
            .unwrap_or(0)
    }

    pub fn get_final_ledger(&self, epoch: u64) -> Vec<String> {
        self.final_ledger.get(&epoch).cloned().unwrap_or_default()
    }

    pub fn is_in_vc_ledger(&self, vc_tx: &[u8]) -> bool {
        self.vc_ledger.contains(vc_tx)
    }

    pub fn is_in_mi(&self, vc_tx: &[u8]) -> bool {
        self.mi
            .values()
            .any(|entries| entries.iter().any(|entry| entry.vc_tx == vc_tx))
    }
}
